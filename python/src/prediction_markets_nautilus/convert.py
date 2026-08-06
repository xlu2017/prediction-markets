"""Schema-v2 to Nautilus converter.

Converts a validated schema-v2 run into normalized intermediate records, then into Nautilus
domain objects written to a Parquet data catalog. Emits a machine-readable conversion report.
Lineage is written to a sidecar ``lineage.parquet`` so every Nautilus event maps back to its
source envelope.
"""

from __future__ import annotations

import dataclasses
import json
import shutil
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .models import (
    Level,
    LifecycleEvent,
    Lineage,
    NormalizedAuditEvent,
    NormalizedBookDelta,
    NormalizedBookSnapshot,
    NormalizedInstrument,
    NormalizedLifecycleEvent,
    NormalizedReferencePrice,
    NormalizedRun,
    NormalizedTrade,
    ReferencePriceUpdate,
)
from .reader import RunDirectory

POLYMARKET_VENUE = "POLYMARKET"

# Nautilus timestamp unit: nanoseconds. Schema-v2 timestamps are Unix milliseconds.
_MS_TO_NS = 1_000_000


class ReplayClock:
    """Stateful monotonic replay clock preserving wall-clock time where possible.

    Uses ``receivedAt`` (in ns) as the candidate timestamp, but enforces strict
    monotonicity by taking ``max(candidate, previous + 1)``. Child records within
    one envelope get nanosecond offsets from the envelope's base time.

    This preserves actual elapsed time between observations, feed-gap durations,
    staleness intervals, and time remaining until market close — unlike a synthetic
    compressed clock that destroys all time semantics.
    """

    def __init__(self) -> None:
        self._last: int = 0

    def next_ts(self, received_at_ms: int, child_index: int = 0) -> int:
        candidate = received_at_ms * _MS_TO_NS + child_index
        if candidate <= self._last:
            candidate = self._last + 1
        self._last = candidate
        return candidate


def _catalog_record_id(
    run_id: str, source_sequence: int, child_index: int, token_id: str | None
) -> str:
    """Stable unique ID for each catalog row.

    Every expanded row (CLEAR, ADD bid, ADD ask, etc.) gets a distinct ID by
    including the child index. Instruments include their token ID to distinguish
    UP vs DOWN from the same market-discovery event.
    """
    return f"{run_id}:{source_sequence}:{child_index}:{token_id or 'none'}"


def _instrument_id(condition_id: str, token_id: str) -> str:
    return f"{condition_id}-{token_id}.{POLYMARKET_VENUE}"


def _ts_event_ms(env_source_ts: int | None, env_received_at: int) -> int:
    return env_source_ts if env_source_ts is not None else env_received_at


def _lineage(
    env, market_id: str, child_index: int = 0, resolved_token_id: str | None = None
) -> Lineage:
    # resolvedTokenId is the actual token for this row (may differ from envelope tokenId for
    # multi-token price_change events where envelope tokenId is null).
    rtid = resolved_token_id if resolved_token_id is not None else env.tokenId
    # catalogRecordId: stable unique ID per catalog row. Includes child_index
    # so every expanded row (CLEAR, ADD bid, ADD ask) gets a distinct ID.
    catalog_record_id = _catalog_record_id(env.runId, env.sequence, child_index, rtid)
    return Lineage(
        runId=env.runId,
        sequence=env.sequence,
        frameId=env.frameId,
        messageIndex=env.messageIndex,
        messageCount=env.messageCount,
        connectionGeneration=env.connectionGeneration,
        marketId=market_id,
        conditionId=env.conditionId,
        tokenId=env.tokenId,
        rawEventType=env.eventType,
        sourceTimestamp=env.sourceTimestamp,
        receivedAt=env.receivedAt,
        processedAt=env.processedAt,
        childIndex=child_index,
        resolvedTokenId=rtid,
        catalogRecordId=catalog_record_id,
    )


def normalize_instruments(run: RunDirectory) -> list[NormalizedInstrument]:
    m = run.market
    out: list[NormalizedInstrument] = []
    # Use the MARKET_DISCOVERED event lineage if present, else synthesize from manifest.
    discovered = next((e for e in run.events if e.source == "POLYMARKET_MARKET"), None)
    if discovered is not None:
        lin = _lineage(discovered, m.marketId)
    else:
        lin = Lineage(
            runId=run.run_id,
            sequence=0,
            frameId=None,
            messageIndex=None,
            messageCount=None,
            connectionGeneration=None,
            marketId=m.marketId,
            conditionId=m.conditionId,
            tokenId=None,
            rawEventType="MARKET_DISCOVERED",
            sourceTimestamp=None,
            receivedAt=0,
            processedAt=0,
        )
    for outcome, token_id in (("UP", m.upTokenId), ("DOWN", m.downTokenId)):
        # Each instrument gets its own lineage with a unique catalogRecordId
        # that includes the token ID, so UP and DOWN instruments are distinct.
        instr_lin = dataclasses.replace(
            lin,
            tokenId=token_id,
            resolvedTokenId=token_id,
            catalogRecordId=_catalog_record_id(lin.runId, lin.sourceSequence, 0, token_id),
        )
        out.append(
            NormalizedInstrument(
                lineage=instr_lin,
                instrumentId=_instrument_id(m.conditionId, token_id),
                conditionId=m.conditionId,
                tokenId=token_id,
                outcome=outcome,
                slug=m.slug,
                question=m.question,
                opensAt=m.opensAt,
                closesAt=m.closesAt,
                tickSize=m.tickSize,
                minOrderSize=None,  # not synthesized; Polymarket has separate limit/market minimums
                collateralCurrency="pUSD",
                rawGammaText=m.rawGammaText,
            )
        )
    return out


def _payload_levels(payload: dict, key: str) -> list[Level]:
    levels = payload.get(key) or []
    out: list[Level] = []
    for lv in levels:
        price = lv.get("price")
        size = lv.get("size")
        if not isinstance(price, str) or not isinstance(size, str):
            raise ValueError(f"book level price/size must be strings: {lv!r}")
        out.append(Level(price=price, size=size))
    return out


def normalize_event(env, instruments_by_token: dict[str, NormalizedInstrument]) -> list[Any]:
    """Convert one envelope into zero or more normalized records.

    Returns a list of normalized records (instrument/snapshot/delta/trade/reference/lifecycle/audit).
    """
    market_id = env.marketId or ""
    lin = _lineage(env, market_id)

    # Dedupe-state routing: only UNIQUE_APPLIED events enter the Nautilus engine.
    # DUPLICATE_APPLIED and RECEIVED_NOT_APPLIED are both audit-only — the recorder
    # persisted them as historical evidence without mutating accepted state.
    if env.dedupeState == "DUPLICATE_APPLIED":
        return [
            NormalizedAuditEvent(
                lineage=lin,
                kind="DEDUPLICATED_COPY",
                originalEventType=env.eventType,
                originalPayload=env.payload,
                rawText=env.rawText,
                rawMessageText=env.rawMessageText,
            )
        ]

    if env.dedupeState == "RECEIVED_NOT_APPLIED":
        return [
            NormalizedAuditEvent(
                lineage=lin,
                kind="REJECTED_NOT_APPLIED",
                originalEventType=env.eventType,
                originalPayload=env.payload,
                rawText=env.rawText,
                rawMessageText=env.rawMessageText,
            )
        ]

    if env.source == "POLYMARKET_MARKET":
        return [
            NormalizedLifecycleEvent(
                lineage=lin,
                kind="MARKET_DISCOVERED",
                generation=env.connectionGeneration,
                detail=env.eventType,
                payloadJson=json.dumps(env.payload, sort_keys=True, default=str)
                if env.payload is not None
                else "",
            )
        ]

    if env.source == "POLYMARKET_REFERENCE_PRICE":
        payload = env.payload if isinstance(env.payload, dict) else {}
        inner_raw = payload.get("payload")
        inner: dict = inner_raw if isinstance(inner_raw, dict) else {}
        value = inner.get("value")
        if not isinstance(value, str):
            # Preserve exact decimal: if the source emitted a JSON number, keep its raw text.
            extracted = _extract_value_from_raw(env.rawMessageText) if env.rawMessageText else None
            if extracted is not None:
                value = extracted
            elif isinstance(value, (int, Decimal)):
                value = str(value)
            elif value is None:
                # Abort conversion — a missing reference price is a data integrity
                # failure, not an audit-only event. Inventing a price would corrupt
                # downstream settlement logic.
                raise ValueError(
                    f"reference price missing value at sourceSequence={env.sequence} "
                    f"(runId={env.runId})"
                )
            else:
                value = str(value)
        symbol = inner.get("symbol") or payload.get("symbol") or ""
        topic = payload.get("topic") or ""
        return [
            NormalizedReferencePrice(
                lineage=lin,
                symbol=str(symbol),
                topic=str(topic),
                value=str(value),
                topLevelSourceTimestamp=env.topLevelSourceTimestamp,
                payloadSourceTimestamp=env.payloadSourceTimestamp,
                sourceTimestamp=env.sourceTimestamp,
            )
        ]

    if env.source == "POLYMARKET_CLOB":
        token_id = env.tokenId
        instr = instruments_by_token.get(token_id) if token_id else None
        et = env.eventType
        # Lifecycle events: real recorder event names from managed-websocket.ts.
        if et in (
            "CONNECTION_ATTEMPT",
            "CONNECTION_OPEN",
            "SUBSCRIPTION_SENT",
            "CONNECTION_ERROR",
            "CONNECTION_CLOSED",
            "DATA_GAP_STARTED",
            "DATA_GAP_ENDED",
            "RECONNECT_SCHEDULED",
            "STALE_STREAM",
            "BOOK_RESYNC_REQUIRED",
        ):
            return [
                NormalizedLifecycleEvent(
                    lineage=lin,
                    kind=et,
                    generation=env.connectionGeneration,
                    detail=str(env.dedupeReason or ""),
                    payloadJson=json.dumps(env.payload, sort_keys=True, default=str)
                    if env.payload is not None
                    else "",
                )
            ]
        if et in ("EMPTY_FRAME", "IGNORED_FOREIGN_MARKET", "PING", "PONG"):
            return [
                NormalizedAuditEvent(
                    lineage=lin,
                    kind=et,
                    originalEventType=et,
                    originalPayload=env.payload,
                    rawText=env.rawText,
                    rawMessageText=env.rawMessageText,
                )
            ]
        if et == "book":
            if instr is None:
                return [_audit_unsupported(env, lin, "book without known token")]
            payload = env.payload if isinstance(env.payload, dict) else {}
            return [
                NormalizedBookSnapshot(
                    lineage=lin,
                    instrumentId=instr.instrumentId,
                    bids=_payload_levels(payload, "bids"),
                    asks=_payload_levels(payload, "asks"),
                    sourceTimestamp=env.sourceTimestamp,
                )
            ]
        if et == "price_change":
            payload = env.payload if isinstance(env.payload, dict) else {}
            changes = payload.get("price_changes") or payload.get("changes") or []
            # Single-level price_change: payload may carry price/size/side directly.
            if not changes and "price" in payload:
                changes = [payload]
            if not changes:
                return [_audit_unsupported(env, lin, "price_change with no levels")]
            out: list[Any] = []
            for child_idx, ch in enumerate(changes):
                price = ch.get("price")
                size = ch.get("size")
                side = ch.get("side")
                if not isinstance(price, str) or not isinstance(size, str):
                    out.append(_audit_unsupported(env, lin, "price_change level not strings"))
                    continue
                # Resolve instrument per-change: prefer asset_id, fall back to envelope tokenId.
                change_token = ch.get("asset_id") or token_id
                change_instr = instruments_by_token.get(change_token) if change_token else None
                if change_instr is None:
                    out.append(
                        _audit_unsupported(
                            env,
                            _lineage(env, market_id, child_idx, resolved_token_id=change_token),
                            f"price_change change {child_idx} without known token",
                        )
                    )
                    continue
                out.append(
                    NormalizedBookDelta(
                        lineage=_lineage(env, market_id, child_idx, resolved_token_id=change_token),
                        instrumentId=change_instr.instrumentId,
                        side=_map_side(side),
                        price=price,
                        size=size,
                        sourceTimestamp=env.sourceTimestamp,
                    )
                )
            return out
        if et in ("last_trade_price", "trade"):
            if instr is None:
                return [_audit_unsupported(env, lin, "trade without known token")]
            payload = env.payload if isinstance(env.payload, dict) else {}
            # Trade may be at top level or in a "trades" list.
            trades_list = payload.get("trades") or []
            if not trades_list and "price" in payload:
                trades_list = [payload]
            if not trades_list:
                return [_audit_unsupported(env, lin, "trade with no trade entries")]
            trade_out: list[Any] = []
            for child_idx, tr in enumerate(trades_list):
                price = tr.get("price")
                size = tr.get("size")
                side = tr.get("side") or tr.get("taker_side")
                if not isinstance(price, str) or not isinstance(size, str):
                    trade_out.append(
                        _audit_unsupported(
                            env, _lineage(env, market_id, child_idx), "trade price/size not strings"
                        )
                    )
                    continue
                trade_out.append(
                    NormalizedTrade(
                        lineage=_lineage(env, market_id, child_idx),
                        instrumentId=instr.instrumentId,
                        price=price,
                        size=size,
                        aggressorSide=_map_side(side) if side else "UNKNOWN",
                        sourceTimestamp=env.sourceTimestamp,
                    )
                )
            return trade_out
        # Unsupported CLOB event
        return [_audit_unsupported(env, lin, f"unsupported CLOB eventType {et}")]

    # Unsupported source
    return [_audit_unsupported(env, lin, f"unsupported source {env.source}")]


def _map_side(side: Any) -> str:
    if isinstance(side, str):
        s = side.upper()
        if s in ("BUY", "BUYER", "BID"):
            return "BUY"
        if s in ("SELL", "SELLER", "ASK", "ASKS"):
            return "SELL"
    return "UNKNOWN"


def _extract_value_from_raw(raw: str) -> str | None:
    """Extract the Chainlink reference value from raw JSON text, preserving exact decimals.

    Uses ``json.loads`` with ``parse_float=Decimal`` so JSON numbers are never passed
    through binary float. If the value is already a string, it is returned as-is.
    """
    try:
        obj = json.loads(raw, parse_float=Decimal)
    except json.JSONDecodeError:
        return None
    inner = obj.get("payload", obj) if isinstance(obj, dict) else {}
    if isinstance(inner, dict) and "value" in inner:
        v = inner["value"]
        if isinstance(v, str):
            return v
        if isinstance(v, Decimal):
            return str(v)
        if isinstance(v, int):
            return str(v)
        return json.dumps(v)
    return None


def _audit_unsupported(env, lin: Lineage, detail: str) -> NormalizedAuditEvent:
    return NormalizedAuditEvent(
        lineage=lin,
        kind="UNSUPPORTED_SOURCE_EVENT",
        originalEventType=env.eventType,
        originalPayload=env.payload,
        rawText=env.rawText,
        rawMessageText=env.rawMessageText,
    )


def normalize_run(run: RunDirectory) -> NormalizedRun:
    instruments = normalize_instruments(run)
    by_token = {instr.tokenId: instr for instr in instruments}
    normalized = NormalizedRun(runId=run.run_id, instruments=instruments)
    for env in run.events:
        for rec in normalize_event(env, by_token):
            if isinstance(rec, NormalizedBookSnapshot):
                normalized.snapshots.append(rec)
            elif isinstance(rec, NormalizedBookDelta):
                normalized.deltas.append(rec)
            elif isinstance(rec, NormalizedTrade):
                normalized.trades.append(rec)
            elif isinstance(rec, NormalizedReferencePrice):
                normalized.references.append(rec)
            elif isinstance(rec, NormalizedLifecycleEvent):
                normalized.lifecycle.append(rec)
            elif isinstance(rec, NormalizedAuditEvent):
                normalized.audit.append(rec)
    return normalized


# --- Nautilus construction ------------------------------------------------------


def _build_binary_option(instr: NormalizedInstrument):
    from nautilus_trader.model.currencies import pUSD
    from nautilus_trader.model.enums import AssetClass
    from nautilus_trader.model.identifiers import InstrumentId, Symbol
    from nautilus_trader.model.instruments import BinaryOption
    from nautilus_trader.model.objects import Price, Quantity

    iid = InstrumentId.from_str(instr.instrumentId)
    price_increment = Price.from_str(instr.tickSize) if instr.tickSize else Price.from_str("0.01")
    size_increment = Quantity.from_str("0.000001")
    expiration_ns = _iso_to_ns(instr.closesAt)
    ts_init = _iso_to_ns(instr.opensAt) or 0
    return BinaryOption(
        instrument_id=iid,
        raw_symbol=Symbol(instr.tokenId),
        outcome=instr.outcome,
        description=instr.question or instr.slug,
        asset_class=AssetClass.ALTERNATIVE,
        currency=pUSD,
        price_increment=price_increment,
        price_precision=price_increment.precision,
        size_increment=size_increment,
        size_precision=size_increment.precision,
        activation_ns=0,
        expiration_ns=expiration_ns,
        max_quantity=None,
        min_quantity=None,
        maker_fee=Decimal("0"),
        taker_fee=Decimal("0"),
        ts_init=ts_init,
        ts_event=ts_init,
    )


def _iso_to_ns(iso: str) -> int:
    import pandas as pd

    try:
        return int(pd.Timestamp(iso).value)
    except (ValueError, TypeError):
        return 0


def _ms_to_ns(ms: int | None) -> int:
    return int(ms) * _MS_TO_NS if ms is not None else 0


def _snapshot_to_deltas(
    snap: NormalizedBookSnapshot, instruments_by_id: dict[str, Any], clock: ReplayClock
) -> tuple[Any, list[dict]]:
    """Convert a snapshot to OrderBookDeltas and return per-row lineage entries.

    Returns (OrderBookDeltas, lineage_rows) where lineage_rows has one entry per
    expanded delta (CLEAR + N ADDs), each with a distinct childIndex.
    """
    from nautilus_trader.model.data import BookOrder, OrderBookDelta, OrderBookDeltas
    from nautilus_trader.model.enums import BookAction, OrderSide, RecordFlag

    instr = instruments_by_id[snap.instrumentId]
    iid = instr.id
    # All deltas from one snapshot share the same ts_event (the envelope's
    # receivedAt-based replay time). The `sequence` field encodes
    # (sourceSequence, childIndex) for ordering within the batch.
    ts_event = clock.next_ts(snap.lineage.receivedAt, 0)
    ts_init = ts_event
    deltas: list[OrderBookDelta] = []
    lineage_rows: list[dict] = []
    child = 0

    # CLEAR
    seq_key = snap.lineage.sourceSequence * 1_000_000 + child
    deltas.append(
        OrderBookDelta(
            instrument_id=iid,
            action=BookAction.CLEAR,
            order=None,
            flags=RecordFlag.F_SNAPSHOT,
            sequence=seq_key,
            ts_event=ts_event,
            ts_init=ts_init,
        )
    )
    lineage_rows.append(
        {
            **snap.lineage.to_dict(),
            "childIndex": child,
            "catalogRecordId": _catalog_record_id(
                snap.lineage.runId,
                snap.lineage.sourceSequence,
                child,
                snap.lineage.resolvedTokenId or snap.lineage.tokenId,
            ),
            "recordType": "OrderBookDelta_CLEAR",
        }
    )
    child += 1

    n_bids = len(snap.bids)
    n_asks = len(snap.asks)
    for idx, lv in enumerate(snap.bids):
        flags = RecordFlag.F_SNAPSHOT
        if idx == n_bids - 1 and n_asks == 0:
            flags = flags | RecordFlag.F_LAST
        order = BookOrder(
            side=OrderSide.BUY,
            price=instr.make_price(Decimal(lv.price)),
            size=instr.make_qty(Decimal(lv.size)),
            order_id=child,
        )
        seq_key = snap.lineage.sourceSequence * 1_000_000 + child
        deltas.append(
            OrderBookDelta(
                instrument_id=iid,
                action=BookAction.ADD,
                order=order,
                flags=flags,
                sequence=seq_key,
                ts_event=ts_event,
                ts_init=ts_init,
            )
        )
        lineage_rows.append(
            {
                **snap.lineage.to_dict(),
                "childIndex": child,
                "catalogRecordId": _catalog_record_id(
                    snap.lineage.runId,
                    snap.lineage.sourceSequence,
                    child,
                    snap.lineage.resolvedTokenId or snap.lineage.tokenId,
                ),
                "recordType": "OrderBookDelta_ADD_BID",
            }
        )
        child += 1
    for idx, lv in enumerate(snap.asks):
        flags = RecordFlag.F_SNAPSHOT
        if idx == n_asks - 1:
            flags = flags | RecordFlag.F_LAST
        order = BookOrder(
            side=OrderSide.SELL,
            price=instr.make_price(Decimal(lv.price)),
            size=instr.make_qty(Decimal(lv.size)),
            order_id=child,
        )
        seq_key = snap.lineage.sourceSequence * 1_000_000 + child
        deltas.append(
            OrderBookDelta(
                instrument_id=iid,
                action=BookAction.ADD,
                order=order,
                flags=flags,
                sequence=seq_key,
                ts_event=ts_event,
                ts_init=ts_init,
            )
        )
        lineage_rows.append(
            {
                **snap.lineage.to_dict(),
                "childIndex": child,
                "catalogRecordId": _catalog_record_id(
                    snap.lineage.runId,
                    snap.lineage.sourceSequence,
                    child,
                    snap.lineage.resolvedTokenId or snap.lineage.tokenId,
                ),
                "recordType": "OrderBookDelta_ADD_ASK",
            }
        )
        child += 1
    return OrderBookDeltas(iid, deltas), lineage_rows


def _delta_to_nautilus(
    d: NormalizedBookDelta, instruments_by_id: dict[str, Any], clock: ReplayClock
) -> tuple[Any, list[dict]]:
    from nautilus_trader.model.data import BookOrder, OrderBookDelta, OrderBookDeltas
    from nautilus_trader.model.enums import BookAction, OrderSide

    instr = instruments_by_id[d.instrumentId]
    iid = instr.id
    ts_event = clock.next_ts(d.lineage.receivedAt, d.lineage.childIndex)
    ts_init = ts_event
    if d.side == "BUY":
        side = OrderSide.BUY
    elif d.side == "SELL":
        side = OrderSide.SELL
    else:
        raise ValueError(f"invalid book side: {d.side!r} for seq {d.lineage.sourceSequence}")
    size_dec = Decimal(d.size)
    action = BookAction.UPDATE if size_dec > 0 else BookAction.DELETE
    seq_key = d.lineage.sourceSequence * 1_000_000 + d.lineage.childIndex
    order = BookOrder(
        side=side,
        price=instr.make_price(Decimal(d.price)),
        size=instr.make_qty(size_dec),
        order_id=d.lineage.childIndex,
    )
    delta = OrderBookDelta(
        instrument_id=iid,
        action=action,
        order=order,
        flags=0,
        sequence=seq_key,
        ts_event=ts_event,
        ts_init=ts_init,
    )
    lineage_rows = [{**d.lineage.to_dict(), "recordType": "OrderBookDelta"}]
    return OrderBookDeltas(iid, [delta]), lineage_rows


def _trade_to_nautilus(
    t: NormalizedTrade, instruments_by_id: dict[str, Any], clock: ReplayClock
) -> tuple[Any, list[dict]]:
    from nautilus_trader.model.data import TradeTick
    from nautilus_trader.model.enums import AggressorSide
    from nautilus_trader.model.identifiers import TradeId

    instr = instruments_by_id[t.instrumentId]
    iid = instr.id
    ts_event = clock.next_ts(t.lineage.receivedAt, t.lineage.childIndex)
    ts_init = ts_event
    agg = AggressorSide.NO_AGGRESSOR
    if t.aggressorSide == "BUY":
        agg = AggressorSide.BUYER
    elif t.aggressorSide == "SELL":
        agg = AggressorSide.SELLER
    trade_id = TradeId(f"{t.lineage.runId}-{t.lineage.sourceSequence}-{t.lineage.childIndex}")
    trade = TradeTick(
        instrument_id=iid,
        price=instr.make_price(Decimal(t.price)),
        size=instr.make_qty(Decimal(t.size)),
        aggressor_side=agg,
        trade_id=trade_id,
        ts_event=ts_event,
        ts_init=ts_init,
    )
    lineage_rows = [{**t.lineage.to_dict(), "recordType": "TradeTick"}]
    return trade, lineage_rows


def _reference_to_nautilus(
    r: NormalizedReferencePrice, clock: ReplayClock
) -> tuple[Any, list[dict]]:
    from nautilus_trader.model.data import CustomData, DataType

    ts_event = clock.next_ts(r.lineage.receivedAt, r.lineage.childIndex)
    ts_init = ts_event
    update = ReferencePriceUpdate(
        ts_event,
        ts_init,
        symbol=r.symbol,
        topic=r.topic,
        value=r.value,
        topLevelSourceTimestamp=r.topLevelSourceTimestamp or 0,
        payloadSourceTimestamp=r.payloadSourceTimestamp or 0,
        sourceSequence=r.lineage.sourceSequence,
        childIndex=r.lineage.childIndex,
    )
    cd = CustomData(
        DataType(ReferencePriceUpdate, {"topic": r.topic or "crypto_prices_chainlink"}), update
    )
    lineage_rows = [{**r.lineage.to_dict(), "recordType": "ReferencePriceUpdate"}]
    return cd, lineage_rows


def _lifecycle_to_nautilus(
    ev: NormalizedLifecycleEvent, clock: ReplayClock
) -> tuple[Any, list[dict]]:
    from nautilus_trader.model.data import CustomData, DataType

    ts_event = clock.next_ts(ev.lineage.receivedAt, ev.lineage.childIndex)
    ts_init = ts_event
    le = LifecycleEvent(
        ts_event,
        ts_init,
        kind=ev.kind,
        generation=ev.generation or 0,
        detail=ev.detail or "",
        payloadJson=ev.payloadJson,
        sourceSequence=ev.lineage.sourceSequence,
        childIndex=ev.lineage.childIndex,
    )
    cd = CustomData(DataType(LifecycleEvent, {"kind": ev.kind}), le)
    lineage_rows = [{**ev.lineage.to_dict(), "recordType": "LifecycleEvent"}]
    return cd, lineage_rows


def _check_precision(
    source_price: str, instr, context: str, lineage: Lineage, issues: list[dict]
) -> None:
    """Check if a source decimal price round-trips through instrument tick precision."""
    try:
        src = Decimal(source_price)
        made = instr.make_price(src)
        rt = Decimal(str(made))
        if rt != src:
            issues.append(
                {
                    "context": context,
                    "sourceSequence": lineage.sourceSequence,
                    "childIndex": lineage.childIndex,
                    "sourcePrice": source_price,
                    "instrumentPrice": str(made),
                    "tickSize": str(instr.price_increment),
                }
            )
    except Exception:
        issues.append(
            {
                "context": context,
                "sourceSequence": lineage.sourceSequence,
                "childIndex": lineage.childIndex,
                "sourcePrice": source_price,
                "error": "precision check failed",
            }
        )


def write_catalog(normalized: NormalizedRun, catalog_directory: str | Path) -> dict:
    """Write normalized records into a Nautilus Parquet catalog + lineage sidecar.

    Returns the machine-readable conversion report.
    """
    from nautilus_trader.persistence.catalog import ParquetDataCatalog

    cat_dir = Path(catalog_directory)
    if cat_dir.exists():
        shutil.rmtree(cat_dir)
    cat_dir.mkdir(parents=True, exist_ok=True)

    instruments_by_id: dict[str, Any] = {}
    nautilus_instruments = []
    for instr in normalized.instruments:
        bo = _build_binary_option(instr)
        instruments_by_id[instr.instrumentId] = bo
        nautilus_instruments.append(bo)

    cat = ParquetDataCatalog(str(cat_dir))
    if nautilus_instruments:
        cat.write_data(nautilus_instruments)

    snapshot_count = 0
    delta_count = 0
    trade_count = 0
    reference_count = 0
    lifecycle_count = 0
    precision_issues: list[dict] = []
    all_deltas: list = []
    all_trades: list = []
    all_refs: list = []
    all_lifecycle: list = []
    catalog_lineage_rows: list[dict] = []
    audit_rows: list[dict] = []
    catalog_ordinal = 0

    def _add_lineage_row(base: dict, record_type: str | None = None) -> dict:
        nonlocal catalog_ordinal
        catalog_ordinal += 1
        rt = base.get("recordType") or record_type
        row = {**base, "recordType": rt}
        run_id = row.get("runId", "")
        seq = row.get("sourceSequence", 0)
        child = row.get("childIndex", 0)
        token = row.get("resolvedTokenId") or row.get("tokenId") or "none"
        row["catalogRecordId"] = f"{run_id}:{seq}:{child}:{token}:{rt}:{catalog_ordinal}"
        return row

    # Instrument lineage (one row per instrument).
    for instr in normalized.instruments:
        catalog_lineage_rows.append(_add_lineage_row(instr.lineage.to_dict(), "Instrument"))

    # Process all engine-bound records in recorder order (sourceSequence, childIndex)
    # so the ReplayClock produces monotonically increasing timestamps that preserve
    # actual elapsed time where possible.
    clock = ReplayClock()
    # Build a unified list of (sort_key, type, record) for all engine-bound records.
    engine_records: list[tuple[tuple[int, int], str, Any]] = []
    for snap in normalized.snapshots:
        engine_records.append(((snap.lineage.sourceSequence, 0), "snap", snap))
    for d in normalized.deltas:
        engine_records.append(((d.lineage.sourceSequence, d.lineage.childIndex), "delta", d))
    for t in normalized.trades:
        engine_records.append(((t.lineage.sourceSequence, t.lineage.childIndex), "trade", t))
    for r in normalized.references:
        engine_records.append(((r.lineage.sourceSequence, r.lineage.childIndex), "ref", r))
    for ev in normalized.lifecycle:
        engine_records.append(((ev.lineage.sourceSequence, ev.lineage.childIndex), "lifecycle", ev))
    engine_records.sort(key=lambda x: x[0])

    for _, rtype, rec in engine_records:
        if rtype == "snap":
            snap = rec
            deltas_obj, rows = _snapshot_to_deltas(snap, instruments_by_id, clock)
            all_deltas.append(deltas_obj)
            catalog_lineage_rows.extend([_add_lineage_row(r) for r in rows])
            snapshot_count += 1
            for lv in [*snap.bids, *snap.asks]:
                _check_precision(
                    lv.price,
                    instruments_by_id[snap.instrumentId],
                    "snapshot",
                    snap.lineage,
                    precision_issues,
                )
        elif rtype == "delta":
            d = rec
            deltas_obj, rows = _delta_to_nautilus(d, instruments_by_id, clock)
            all_deltas.append(deltas_obj)
            catalog_lineage_rows.extend([_add_lineage_row(r) for r in rows])
            delta_count += 1
            _check_precision(
                d.price, instruments_by_id[d.instrumentId], "delta", d.lineage, precision_issues
            )
        elif rtype == "trade":
            t = rec
            trade_obj, rows = _trade_to_nautilus(t, instruments_by_id, clock)
            all_trades.append(trade_obj)
            catalog_lineage_rows.extend([_add_lineage_row(r) for r in rows])
            trade_count += 1
            _check_precision(
                t.price, instruments_by_id[t.instrumentId], "trade", t.lineage, precision_issues
            )
        elif rtype == "ref":
            r = rec
            ref_obj, rows = _reference_to_nautilus(r, clock)
            all_refs.append(ref_obj)
            catalog_lineage_rows.extend([_add_lineage_row(r) for r in rows])
            reference_count += 1
        elif rtype == "lifecycle":
            ev = rec
            lc_obj, rows = _lifecycle_to_nautilus(ev, clock)
            all_lifecycle.append(lc_obj)
            catalog_lineage_rows.extend([_add_lineage_row(r) for r in rows])
            lifecycle_count += 1

    # Abort if any precision loss was detected — sub-tick prices must not be silently quantized.
    if precision_issues:
        issue_lines = [
            f"  seq {pi.get('sourceSequence')} child {pi.get('childIndex')}: "
            f"{pi.get('sourcePrice')} -> {pi.get('instrumentPrice', pi.get('error'))}"
            for pi in precision_issues[:10]
        ]
        suffix = (
            f"\n  ... and {len(precision_issues) - 10} more" if len(precision_issues) > 10 else ""
        )
        raise ValueError(
            f"conversion aborted: {len(precision_issues)} precision loss issue(s) detected. "
            f"Source prices contain sub-tick values that would be quantized by instrument precision."
            f"\n{'\n'.join(issue_lines)}{suffix}"
        )

    # Audit events: one lineage row per audit event; reuse the finalized ID in the
    # audit sidecar so both sidecars share the same catalogRecordId with global ordinal.
    for a in normalized.audit:
        row = _add_lineage_row(a.lineage.to_dict(), "AuditEvent")
        catalog_lineage_rows.append(row)
        audit_rows.append(
            {
                **row,
                "kind": a.kind,
                "originalEventType": a.originalEventType,
                "originalPayload": json.dumps(a.originalPayload, sort_keys=True, default=str),
                "rawText": a.rawText,
                "rawMessageText": a.rawMessageText,
            }
        )

    if all_deltas:
        # Sort by ts_init as required by the catalog.
        all_deltas.sort(key=lambda d: d.ts_init if hasattr(d, "ts_init") else 0)
        cat.write_data(all_deltas)
    if all_trades:
        all_trades.sort(key=lambda t: t.ts_init if hasattr(t, "ts_init") else 0)
        cat.write_data(all_trades)
    if all_refs:
        cat.write_data(all_refs)
    if all_lifecycle:
        cat.write_data(all_lifecycle)

    # Lineage sidecar: one row per actual catalog row (not per normalized record).
    ids = [row["catalogRecordId"] for row in catalog_lineage_rows]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate catalogRecordId detected")
    _write_lineage_sidecar(cat_dir, catalog_lineage_rows)

    # Audit sidecar: full audit events using the same finalized lineage rows.
    _write_audit_sidecar(cat_dir, audit_rows)

    # Count source envelopes (unique source sequences), not expanded normalized records.
    all_records: list[Any] = [
        *normalized.instruments,
        *normalized.snapshots,
        *normalized.deltas,
        *normalized.trades,
        *normalized.references,
        *normalized.lifecycle,
        *normalized.audit,
    ]
    source_sequences = {e.lineage.sourceSequence for e in all_records}
    report = {
        "sourceRunId": normalized.runId,
        "sourceEventCount": len(source_sequences),
        "convertedSnapshotCount": snapshot_count,
        "convertedDeltaCount": delta_count,
        "convertedTradeCount": trade_count,
        "convertedReferenceCount": reference_count,
        "convertedLifecycleCount": lifecycle_count,
        "auditOnlyEventCount": len(normalized.audit),
        "ignoredEventCount": sum(
            1 for a in normalized.audit if a.kind == "UNSUPPORTED_SOURCE_EVENT"
        ),
        "conversionErrors": [],
        "precisionIssues": precision_issues,
        "firstSourceSequence": min(source_sequences) if source_sequences else None,
        "lastSourceSequence": max(source_sequences) if source_sequences else None,
        "catalogLocation": str(cat_dir),
        "catalogRowCount": len(catalog_lineage_rows),
        "unsupportedEventTypes": sorted(
            {a.originalEventType for a in normalized.audit if a.kind == "UNSUPPORTED_SOURCE_EVENT"}
        ),
    }
    report_path = cat_dir / "conversion-report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def _write_lineage_sidecar(cat_dir: Path, rows: list[dict]) -> None:
    if not rows:
        return
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, cat_dir / "lineage.parquet")


def _write_audit_sidecar(cat_dir: Path, audit_rows: list[dict]) -> None:
    if not audit_rows:
        return
    table = pa.Table.from_pylist(audit_rows)
    pq.write_table(table, cat_dir / "audit.parquet")


def convert_run(run_directory: str | Path, catalog_directory: str | Path) -> dict:
    """End-to-end: load, validate, normalize, and write catalog. Returns the report."""
    from .reader import load_run

    run = load_run(run_directory)
    normalized = normalize_run(run)
    report = write_catalog(normalized, catalog_directory)
    # Expected final-book health sidecar for self-contained Phase 9 comparison.
    expected = {
        tid: {
            "tokenId": book.get("tokenId"),
            "state": book.get("state"),
            "integrity": book.get("integrity"),
            "bestBid": book.get("bestBid"),
            "bestAsk": book.get("bestAsk"),
            "bidDepthCount": book.get("bidDepthCount"),
            "askDepthCount": book.get("askDepthCount"),
            "lastSuccessfulUpdateTime": book.get("lastSuccessfulUpdateTime"),
        }
        for tid, book in run.health.get("finalBookHealth", {}).items()
    }
    (Path(catalog_directory) / "expected-final-books.json").write_text(
        json.dumps(expected, indent=2), encoding="utf-8"
    )
    return report


def _main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="prediction_markets_nautilus.convert")
    parser.add_argument("--run-directory", required=True)
    parser.add_argument("--catalog-directory", required=True)
    args = parser.parse_args()
    print(json.dumps(convert_run(args.run_directory, args.catalog_directory), indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main())
