"""Schema-v2 to Nautilus converter.

Converts a validated schema-v2 run into normalized intermediate records, then into Nautilus
domain objects written to a Parquet data catalog. Emits a machine-readable conversion report.
Lineage is written to a sidecar ``lineage.parquet`` so every Nautilus event maps back to its
source envelope.
"""

from __future__ import annotations

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


def _instrument_id(condition_id: str, token_id: str) -> str:
    return f"{condition_id}-{token_id}.{POLYMARKET_VENUE}"


def _ts_event_ms(env_source_ts: int | None, env_received_at: int) -> int:
    return env_source_ts if env_source_ts is not None else env_received_at


def _lineage(env, market_id: str) -> Lineage:
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
        out.append(
            NormalizedInstrument(
                lineage=lin,
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

    # Duplicate handling: applied duplicates and not-applied copies go to audit only.
    if env.dedupeState in ("DUPLICATE_APPLIED", "RECEIVED_NOT_APPLIED"):
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

    if env.source == "POLYMARKET_MARKET":
        return [
            NormalizedLifecycleEvent(
                lineage=lin,
                kind="MARKET_DISCOVERED",
                generation=env.connectionGeneration,
                detail=env.eventType,
            )
        ]

    if env.source == "POLYMARKET_REFERENCE_PRICE":
        payload = env.payload if isinstance(env.payload, dict) else {}
        inner_raw = payload.get("payload")
        inner: dict = inner_raw if isinstance(inner_raw, dict) else {}
        value = inner.get("value")
        if not isinstance(value, str):
            # Preserve exact decimal: if the source emitted a JSON number, keep its raw text.
            value = env.rawMessageText and _extract_value_from_raw(env.rawMessageText) or str(value)
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
            if instr is None:
                return [_audit_unsupported(env, lin, "price_change without known token")]
            payload = env.payload if isinstance(env.payload, dict) else {}
            changes = payload.get("price_changes") or payload.get("changes") or []
            # Single-level price_change: payload may carry price/size/side directly.
            if not changes and "price" in payload:
                changes = [payload]
            out: list[Any] = []
            for ch in changes:
                price = ch.get("price")
                size = ch.get("size")
                side = ch.get("side")
                if not isinstance(price, str) or not isinstance(size, str):
                    out.append(_audit_unsupported(env, lin, "price_change level not strings"))
                    continue
                out.append(
                    NormalizedBookDelta(
                        lineage=lin,
                        instrumentId=instr.instrumentId,
                        side=_map_side(side),
                        price=price,
                        size=size,
                        sourceTimestamp=env.sourceTimestamp,
                    )
                )
            return out or [_audit_unsupported(env, lin, "price_change with no levels")]
        if et in ("last_trade_price", "trade"):
            if instr is None:
                return [_audit_unsupported(env, lin, "trade without known token")]
            payload = env.payload if isinstance(env.payload, dict) else {}
            price = payload.get("price")
            size = payload.get("size")
            side = payload.get("side") or payload.get("taker_side")
            if not isinstance(price, str) or not isinstance(size, str):
                return [_audit_unsupported(env, lin, "trade price/size not strings")]
            return [
                NormalizedTrade(
                    lineage=lin,
                    instrumentId=instr.instrumentId,
                    price=price,
                    size=size,
                    aggressorSide=_map_side(side) if side else "UNKNOWN",
                    sourceTimestamp=env.sourceTimestamp,
                )
            ]
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
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return None
    inner = obj.get("payload", obj) if isinstance(obj, dict) else {}
    if isinstance(inner, dict) and "value" in inner:
        v = inner["value"]
        return v if isinstance(v, str) else json.dumps(v)
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


def _snapshot_to_deltas(snap: NormalizedBookSnapshot, instruments_by_id: dict[str, Any]):
    from nautilus_trader.model.data import BookOrder, OrderBookDelta, OrderBookDeltas
    from nautilus_trader.model.enums import BookAction, OrderSide, RecordFlag

    instr = instruments_by_id[snap.instrumentId]
    iid = instr.id
    ts_event = _ms_to_ns(snap.sourceTimestamp) or _ms_to_ns(snap.lineage.receivedAt)
    ts_init = _ms_to_ns(snap.lineage.processedAt)
    deltas: list[OrderBookDelta] = [
        OrderBookDelta(
            instrument_id=iid,
            action=BookAction.CLEAR,
            order=None,
            flags=RecordFlag.F_SNAPSHOT,
            sequence=0,
            ts_event=ts_event,
            ts_init=ts_init,
        )
    ]
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
            order_id=0,
        )
        deltas.append(
            OrderBookDelta(
                instrument_id=iid,
                action=BookAction.ADD,
                order=order,
                flags=flags,
                sequence=0,
                ts_event=ts_event,
                ts_init=ts_init,
            )
        )
    for idx, lv in enumerate(snap.asks):
        flags = RecordFlag.F_SNAPSHOT
        if idx == n_asks - 1:
            flags = flags | RecordFlag.F_LAST
        order = BookOrder(
            side=OrderSide.SELL,
            price=instr.make_price(Decimal(lv.price)),
            size=instr.make_qty(Decimal(lv.size)),
            order_id=0,
        )
        deltas.append(
            OrderBookDelta(
                instrument_id=iid,
                action=BookAction.ADD,
                order=order,
                flags=flags,
                sequence=0,
                ts_event=ts_event,
                ts_init=ts_init,
            )
        )
    return OrderBookDeltas(iid, deltas)


def _delta_to_nautilus(d: NormalizedBookDelta, instruments_by_id: dict[str, Any]):
    from nautilus_trader.model.data import BookOrder, OrderBookDelta, OrderBookDeltas
    from nautilus_trader.model.enums import BookAction, OrderSide

    instr = instruments_by_id[d.instrumentId]
    iid = instr.id
    ts_event = _ms_to_ns(d.sourceTimestamp) or _ms_to_ns(d.lineage.receivedAt)
    ts_init = _ms_to_ns(d.lineage.processedAt)
    side = OrderSide.BUY if d.side == "BUY" else OrderSide.SELL
    size_dec = Decimal(d.size)
    action = BookAction.UPDATE if size_dec > 0 else BookAction.DELETE
    order = BookOrder(
        side=side,
        price=instr.make_price(Decimal(d.price)),
        size=instr.make_qty(size_dec),
        order_id=0,
    )
    delta = OrderBookDelta(
        instrument_id=iid,
        action=action,
        order=order,
        flags=0,
        sequence=0,
        ts_event=ts_event,
        ts_init=ts_init,
    )
    return OrderBookDeltas(iid, [delta])


def _trade_to_nautilus(t: NormalizedTrade, instruments_by_id: dict[str, Any]):
    from nautilus_trader.model.data import TradeTick
    from nautilus_trader.model.enums import AggressorSide
    from nautilus_trader.model.identifiers import TradeId

    instr = instruments_by_id[t.instrumentId]
    iid = instr.id
    ts_event = _ms_to_ns(t.sourceTimestamp) or _ms_to_ns(t.lineage.receivedAt)
    ts_init = _ms_to_ns(t.lineage.processedAt)
    agg = AggressorSide.NO_AGGRESSOR
    if t.aggressorSide == "BUY":
        agg = AggressorSide.BUYER
    elif t.aggressorSide == "SELL":
        agg = AggressorSide.SELLER
    trade_id = TradeId(f"{t.lineage.runId}-{t.lineage.sequence}")
    return TradeTick(
        instrument_id=iid,
        price=instr.make_price(Decimal(t.price)),
        size=instr.make_qty(Decimal(t.size)),
        aggressor_side=agg,
        trade_id=trade_id,
        ts_event=ts_event,
        ts_init=ts_init,
    )


def _reference_to_nautilus(r: NormalizedReferencePrice):
    from nautilus_trader.model.data import CustomData, DataType

    ts_event = _ms_to_ns(r.sourceTimestamp) or _ms_to_ns(r.lineage.receivedAt)
    ts_init = _ms_to_ns(r.lineage.processedAt)
    update = ReferencePriceUpdate(
        ts_event,
        ts_init,
        symbol=r.symbol,
        topic=r.topic,
        value=r.value,
        topLevelSourceTimestamp=r.topLevelSourceTimestamp or 0,
        payloadSourceTimestamp=r.payloadSourceTimestamp or 0,
    )
    return CustomData(
        DataType(ReferencePriceUpdate, {"topic": r.topic or "crypto_prices_chainlink"}), update
    )


def _lifecycle_to_nautilus(ev: NormalizedLifecycleEvent):
    from nautilus_trader.model.data import CustomData, DataType

    ts_event = _ms_to_ns(ev.lineage.sourceTimestamp) or _ms_to_ns(ev.lineage.receivedAt)
    ts_init = _ms_to_ns(ev.lineage.processedAt)
    le = LifecycleEvent(
        ts_event,
        ts_init,
        kind=ev.kind,
        generation=ev.generation or 0,
        detail=ev.detail or "",
    )
    return CustomData(DataType(LifecycleEvent, {"kind": ev.kind}), le)


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
    all_deltas: list = []
    all_trades: list = []
    all_refs: list = []
    all_lifecycle: list = []
    for snap in normalized.snapshots:
        all_deltas.append(_snapshot_to_deltas(snap, instruments_by_id))
        snapshot_count += 1
    for d in normalized.deltas:
        all_deltas.append(_delta_to_nautilus(d, instruments_by_id))
        delta_count += 1
    for t in normalized.trades:
        all_trades.append(_trade_to_nautilus(t, instruments_by_id))
        trade_count += 1
    for r in normalized.references:
        all_refs.append(_reference_to_nautilus(r))
        reference_count += 1
    for ev in normalized.lifecycle:
        all_lifecycle.append(_lifecycle_to_nautilus(ev))
        lifecycle_count += 1

    if all_deltas:
        cat.write_data(all_deltas)
    if all_trades:
        cat.write_data(all_trades)
    if all_refs:
        cat.write_data(all_refs)
    if all_lifecycle:
        cat.write_data(all_lifecycle)

    # Lineage sidecar: one row per source sequence.
    lineage_rows: list[dict] = []
    lineage_records: list[Any] = [
        *normalized.instruments,
        *normalized.snapshots,
        *normalized.deltas,
        *normalized.trades,
        *normalized.references,
        *normalized.lifecycle,
        *normalized.audit,
    ]
    for rec in lineage_records:
        lineage_rows.append({**rec.lineage.to_dict(), "recordType": type(rec).__name__})
    _write_lineage_sidecar(cat_dir, lineage_rows)

    # Audit sidecar: full audit events.
    _write_audit_sidecar(cat_dir, normalized.audit)

    seq_records: list[Any] = [
        *normalized.snapshots,
        *normalized.deltas,
        *normalized.trades,
        *normalized.references,
        *normalized.lifecycle,
        *normalized.audit,
        *normalized.instruments,
    ]
    sequences = [e.lineage.sequence for e in seq_records]
    report = {
        "sourceRunId": normalized.runId,
        "sourceEventCount": len(sequences),
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
        "firstSourceSequence": min(sequences) if sequences else None,
        "lastSourceSequence": max(sequences) if sequences else None,
        "catalogLocation": str(cat_dir),
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


def _write_audit_sidecar(cat_dir: Path, audit: list[NormalizedAuditEvent]) -> None:
    if not audit:
        return
    rows = []
    for a in audit:
        rows.append(
            {
                **a.lineage.to_dict(),
                "kind": a.kind,
                "originalEventType": a.originalEventType,
                "originalPayload": json.dumps(a.originalPayload, sort_keys=True, default=str),
                "rawText": a.rawText,
                "rawMessageText": a.rawMessageText,
            }
        )
    table = pa.Table.from_pylist(rows)
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
