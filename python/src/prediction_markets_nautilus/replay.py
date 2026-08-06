"""Deterministic replay and verification from a Nautilus catalog.

Reads a converted catalog, replays order-book deltas into Nautilus ``OrderBook`` objects in a
deterministic order, and computes deterministic hashes for the normalized input stream, the Up
and Down books after each event, the final books, the reference-price stream, the
strategy-visible event stream, and a simulated order-intent stream.

The simulated strategy is deliberately trivial and **never submits a real order**: it records an
internal intent when a fixed predicate is true (best bid < 0.50). No execution client is
connected.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .hashes import hash_value
from .models import LifecycleEvent, ReferencePriceUpdate

_MS_TO_NS = 1_000_000


@dataclass
class BookState:
    instrument_id: str
    best_bid: str | None
    best_ask: str | None
    bid_depth_count: int
    ask_depth_count: int
    levels: list[tuple[str, str, str]]  # (side, price, size)

    @classmethod
    def from_book(cls, book) -> BookState:
        bids = book.bids()
        asks = book.asks()
        levels: list[tuple[str, str, str]] = []
        for lv in bids:
            levels.append(("BUY", str(lv.price), str(lv.size())))
        for lv in asks:
            levels.append(("SELL", str(lv.price), str(lv.size())))
        return cls(
            instrument_id=str(book.instrument_id),
            best_bid=str(book.best_bid_price()) if bids else None,
            best_ask=str(book.best_ask_price()) if asks else None,
            bid_depth_count=len(bids),
            ask_depth_count=len(asks),
            levels=levels,
        )

    def integrity(self) -> str:
        if self.best_bid is None or self.best_ask is None:
            return "NORMAL"
        from decimal import Decimal

        b = Decimal(self.best_bid)
        a = Decimal(self.best_ask)
        if b > a:
            return "CROSSED"
        if b == a:
            return "LOCKED"
        return "NORMAL"


@dataclass
class ReplayResult:
    run_id: str
    input_stream_hash: str
    up_book_after_each_event: list[str]
    down_book_after_each_event: list[str]
    final_up_book_hash: str
    final_down_book_hash: str
    reference_stream_hash: str
    strategy_visible_stream_hash: str
    order_intent_stream_hash: str
    final_up_book: BookState
    final_down_book: BookState
    reference_count: int
    event_count: int

    def to_dict(self) -> dict:
        return {
            "runId": self.run_id,
            "eventCount": self.event_count,
            "inputStreamHash": self.input_stream_hash,
            "upBookAfterEachEvent": self.up_book_after_each_event,
            "downBookAfterEachEvent": self.down_book_after_each_event,
            "finalUpBookHash": self.final_up_book_hash,
            "finalDownBookHash": self.final_down_book_hash,
            "referenceStreamHash": self.reference_stream_hash,
            "strategyVisibleStreamHash": self.strategy_visible_stream_hash,
            "orderIntentStreamHash": self.order_intent_stream_hash,
            "finalUpBook": _book_to_dict(self.final_up_book),
            "finalDownBook": _book_to_dict(self.final_down_book),
            "referenceCount": self.reference_count,
        }


def _book_to_dict(b: BookState) -> dict:
    return {
        "instrumentId": b.instrument_id,
        "bestBid": b.best_bid,
        "bestAsk": b.best_ask,
        "bidDepthCount": b.bid_depth_count,
        "askDepthCount": b.ask_depth_count,
        "integrity": b.integrity(),
        "levels": b.levels,
    }


def _load_lineage(catalog_dir: Path) -> list[dict]:
    lineage_path = catalog_dir / "lineage.parquet"
    if not lineage_path.exists():
        return []
    import pyarrow.parquet as pq

    return pq.read_table(lineage_path).to_pylist()


def replay_catalog(catalog_directory: str | Path) -> ReplayResult:
    """Replay a converted catalog deterministically and return hashes + final book state.

    Events are ordered by ``(sourceSequence, childIndex)`` to preserve the recorder's
    canonical receipt order, not source timestamps which may be delayed, equal, or
    out of order.
    """
    from nautilus_trader.model.book import OrderBook
    from nautilus_trader.model.enums import BookType
    from nautilus_trader.persistence.catalog import ParquetDataCatalog

    cat_dir = Path(catalog_directory)
    cat = ParquetDataCatalog(str(cat_dir))

    instruments = cat.instruments()
    if not instruments:
        raise ValueError("no instruments in catalog")
    up_instr = next((i for i in instruments if i.outcome == "UP"), instruments[0])
    down_instr = next((i for i in instruments if i.outcome == "DOWN"), None)

    up_book = OrderBook(up_instr.id, book_type=BookType.L2_MBP)
    down_book = OrderBook(down_instr.id, book_type=BookType.L2_MBP) if down_instr else None

    # Read all order-book deltas, then order by (sourceSequence, childIndex) embedded
    # in the delta's `sequence` field (sourceSequence * 1000 + childIndex).
    up_deltas = _read_deltas(cat, up_instr.id)
    down_deltas = _read_deltas(cat, down_instr.id) if down_instr else []

    # Merge into a single event stream ordered by the embedded sequence key.
    events: list[dict] = []
    for d in up_deltas:
        seq_key = d.sequence
        seq = seq_key // 1_000_000
        child = seq_key % 1_000_000
        events.append({"seq": seq, "child": child, "side": "UP", "delta": d})
    for d in down_deltas:
        seq_key = d.sequence
        seq = seq_key // 1_000_000
        child = seq_key % 1_000_000
        events.append({"seq": seq, "child": child, "side": "DOWN", "delta": d})
    # Deterministic sort by recorder sequence, then child index, then side as tiebreaker.
    events.sort(key=lambda e: (e["seq"], e["child"], e["side"]))

    up_after: list[str] = []
    down_after: list[str] = []
    strategy_visible: list[dict] = []
    order_intents: list[dict] = []
    input_stream: list[dict] = []

    for ev in events:
        delta = ev["delta"]
        side_key = ev["side"]
        book = up_book if side_key == "UP" else down_book
        if book is None:
            continue
        book.apply_deltas(_as_deltas(delta))
        state = BookState.from_book(book)
        h = hash_value(_book_to_dict(state))
        if side_key == "UP":
            up_after.append(h)
        else:
            down_after.append(h)
        strategy_visible.append(
            {
                "sourceSequence": ev["seq"],
                "childIndex": ev["child"],
                "side": side_key,
                "bestBid": state.best_bid,
                "bestAsk": state.best_ask,
            }
        )
        # Trivial deterministic strategy: record an intent (never submit) when best bid < 0.50.
        # Use Decimal, not float, for the comparison.
        if state.best_bid is not None:
            from decimal import Decimal

            if Decimal(state.best_bid) < Decimal("0.50"):
                order_intents.append(
                    {
                        "sourceSequence": ev["seq"],
                        "childIndex": ev["child"],
                        "side": side_key,
                        "predicate": "bestBid<0.50",
                        "bestBid": state.best_bid,
                        "action": "INTENT_ONLY_NO_ORDER",
                    }
                )
        input_stream.append(
            {
                "sourceSequence": ev["seq"],
                "childIndex": ev["child"],
                "side": side_key,
                "action": str(delta.action),
            }
        )

    final_up = BookState.from_book(up_book)
    final_down = (
        BookState.from_book(down_book)
        if down_book
        else BookState(
            instrument_id="",
            best_bid=None,
            best_ask=None,
            bid_depth_count=0,
            ask_depth_count=0,
            levels=[],
        )
    )

    # Reference-price stream from custom data — ordered by (sourceSequence, childIndex).
    refs = cat.custom_data(cls=ReferencePriceUpdate)
    ref_records = []
    for cd in refs:
        r = cd.data
        ref_entry = {
            "sourceSequence": r.sourceSequence,
            "childIndex": r.childIndex,
            "symbol": r.symbol,
            "topic": r.topic,
            "value": r.value,
            "topLevelSourceTimestamp": r.topLevelSourceTimestamp,
            "payloadSourceTimestamp": r.payloadSourceTimestamp,
            "ts_event": r.ts_event,
            "ts_init": r.ts_init,
        }
        ref_records.append(ref_entry)
        # Include references in the canonical input stream.
        input_stream.append(
            {
                "sourceSequence": r.sourceSequence,
                "childIndex": r.childIndex,
                "side": "REFERENCE",
                "symbol": r.symbol,
                "value": r.value,
            }
        )
    ref_records.sort(key=lambda x: (x["sourceSequence"], x["childIndex"]))

    # Trade ticks — ordered by (sourceSequence, childIndex) and included in input stream.
    trade_ticks = cat.trade_ticks()
    for tt in trade_ticks:
        # TradeId format: runId-sourceSequence-childIndex
        tid_str = str(tt.trade_id)
        parts = tid_str.rsplit("-", 2)
        tt_seq = int(parts[-2]) if len(parts) == 3 else 0
        tt_child = int(parts[-1]) if len(parts) == 3 else 0
        input_stream.append(
            {
                "sourceSequence": tt_seq,
                "childIndex": tt_child,
                "side": "TRADE",
                "price": str(tt.price),
                "size": str(tt.size),
            }
        )

    # Lifecycle events (audit/catalog sidecar) — include sourceSequence, childIndex,
    # and the full typed payload (feed name, close code, gap duration, etc.).
    lifecycle = cat.custom_data(cls=LifecycleEvent)
    for cd in lifecycle:
        le = cd.data
        input_stream.append(
            {
                "sourceSequence": le.sourceSequence,
                "childIndex": le.childIndex,
                "side": "LIFECYCLE",
                "kind": le.kind,
                "generation": le.generation,
                "payloadJson": le.payloadJson,
            }
        )

    # Audit events (duplicates, empty frames, unsupported events) — included in the
    # canonical input hash so the hash covers the complete source stream, not just
    # the events fed to the trading engine.
    audit_path = cat_dir / "audit.parquet"
    if audit_path.exists():
        import pyarrow.parquet as pq

        audit_table = pq.read_table(audit_path)
        for row in audit_table.to_pylist():
            input_stream.append(
                {
                    "sourceSequence": row.get("sourceSequence", 0),
                    "childIndex": row.get("childIndex", 0),
                    "side": "AUDIT",
                    "kind": row.get("kind", ""),
                    "originalEventType": row.get("originalEventType", ""),
                }
            )

    # Canonical sort by (sourceSequence, childIndex, side) — includes ALL event types.
    input_stream.sort(
        key=lambda x: (x.get("sourceSequence", 0), x.get("childIndex", 0), x.get("side", ""))
    )

    run_id = _read_run_id(cat_dir)
    return ReplayResult(
        run_id=run_id,
        input_stream_hash=hash_value(input_stream),
        up_book_after_each_event=up_after,
        down_book_after_each_event=down_after,
        final_up_book_hash=hash_value(_book_to_dict(final_up)),
        final_down_book_hash=hash_value(_book_to_dict(final_down)),
        reference_stream_hash=hash_value(ref_records),
        strategy_visible_stream_hash=hash_value(strategy_visible),
        order_intent_stream_hash=hash_value(order_intents),
        final_up_book=final_up,
        final_down_book=final_down,
        reference_count=len(ref_records),
        event_count=len(events),
    )


_ACTION_RANK = {"CLEAR": 0, "ADD": 1, "UPDATE": 2, "DELETE": 3}


def _action_name(delta) -> str:
    return delta.action.name if hasattr(delta.action, "name") else str(delta.action)


def _read_deltas(cat, instrument_id) -> list:
    # The catalog's instrument_id filter is unreliable in this version, so filter manually.
    raw = cat.order_book_deltas()
    flat = [d for d in raw if d.instrument_id == instrument_id]
    # Sort by ts_event, then action rank (CLEAR before ADDs within a snapshot).
    # Final ordering by (sourceSequence, childIndex) is done by the caller.
    flat.sort(
        key=lambda d: (
            d.ts_event,
            d.ts_init,
            _ACTION_RANK.get(_action_name(d), 9),
            str(d.order.side) if d.order else "",
            str(d.order.price) if d.order else "",
        )
    )
    return flat


def _match_deltas_to_lineage(
    up_deltas: list, down_deltas: list, lineage: list[dict], up_instr, down_instr
) -> list[tuple]:
    """Match catalog delta rows to lineage rows by (tokenId, ts_event, actionRank, order).

    Both the delta lists and the lineage rows are sorted deterministically, so we can
    match them in order within each (tokenId, ts_event) group.
    Returns a list of (delta, sourceSequence, childIndex, sideKey) tuples.
    """
    # Group lineage rows by tokenId, sorted by (sourceSequence, childIndex).
    from collections import defaultdict

    lin_by_token: dict[str, list[dict]] = defaultdict(list)
    for row in lineage:
        tid = row.get("tokenId")
        if tid:
            lin_by_token[tid].append(row)
    for tid in lin_by_token:
        lin_by_token[tid].sort(key=lambda r: (r.get("sourceSequence", 0), r.get("childIndex", 0)))

    # Filter lineage to only delta rows (recordType starts with "OrderBookDelta").
    lin_delta_by_token: dict[str, list[dict]] = defaultdict(list)
    for tid, rows in lin_by_token.items():
        lin_delta_by_token[tid] = [
            r for r in rows if r.get("recordType", "").startswith("OrderBookDelta")
        ]

    up_token = up_instr.raw_symbol.value
    down_token = down_instr.raw_symbol.value if down_instr else None

    result: list[tuple] = []
    for deltas, token, side_key in [
        (up_deltas, up_token, "UP"),
        (down_deltas, down_token, "DOWN"),
    ]:
        if not deltas or not token:
            continue
        lin_rows = lin_delta_by_token.get(token, [])
        # Match by position within the sorted group â€” both are sorted by (ts_event, action_rank).
        # This works because the converter writes deltas and lineage in the same order.
        for i, d in enumerate(deltas):
            if i < len(lin_rows):
                row = lin_rows[i]
                seq = row.get("sourceSequence", 0)
                child = row.get("childIndex", 0)
            else:
                seq = 0
                child = i
            result.append((d, seq, child, side_key))
    return result


def _as_deltas(delta):
    from nautilus_trader.model.data import OrderBookDeltas

    return OrderBookDeltas(delta.instrument_id, [delta])


def _read_run_id(cat_dir: Path) -> str:
    report_path = cat_dir / "conversion-report.json"
    if report_path.exists():
        return json.loads(report_path.read_text(encoding="utf-8")).get("sourceRunId", "")
    return ""


def compare_final_books(result: ReplayResult, catalog_directory: str | Path) -> dict:
    """Phase 9: compare Nautilus-reconstructed final books with recorder finalBookHealth."""
    cat_dir = Path(catalog_directory)
    expected_path = cat_dir / "expected-final-books.json"
    expected = (
        json.loads(expected_path.read_text(encoding="utf-8")) if expected_path.exists() else {}
    )

    # Map expected by tokenId; map reconstructed by token id embedded in instrument id.
    instrs = _instrument_token_map(cat_dir)
    comparisons = []
    for outcome, book in (("UP", result.final_up_book), ("DOWN", result.final_down_book)):
        token_id = instrs.get(outcome)
        exp = expected.get(token_id, {}) if token_id else {}
        recon_bid = book.best_bid
        recon_ask = book.best_ask
        # Normalize decimal lexemes for comparison (strip leading ".")
        comparisons.append(
            {
                "outcome": outcome,
                "tokenId": token_id,
                "expectedBestBid": exp.get("bestBid"),
                "reconstructedBestBid": recon_bid,
                "bestBidMatch": _decimal_eq(exp.get("bestBid"), recon_bid),
                "expectedBestAsk": exp.get("bestAsk"),
                "reconstructedBestAsk": recon_ask,
                "bestAskMatch": _decimal_eq(exp.get("bestAsk"), recon_ask),
                "expectedBidDepthCount": exp.get("bidDepthCount"),
                "reconstructedBidDepthCount": book.bid_depth_count,
                "bidDepthMatch": exp.get("bidDepthCount") == book.bid_depth_count,
                "expectedAskDepthCount": exp.get("askDepthCount"),
                "reconstructedAskDepthCount": book.ask_depth_count,
                "askDepthMatch": exp.get("askDepthCount") == book.ask_depth_count,
                "expectedIntegrity": exp.get("integrity"),
                "reconstructedIntegrity": book.integrity(),
                "integrityMatch": exp.get("integrity") == book.integrity(),
            }
        )
    all_match = all(
        c["bestBidMatch"]
        and c["bestAskMatch"]
        and c["bidDepthMatch"]
        and c["askDepthMatch"]
        and c["integrityMatch"]
        for c in comparisons
    )
    return {"allMatch": all_match, "comparisons": comparisons}


def _instrument_token_map(cat_dir: Path) -> dict[str, str]:
    from nautilus_trader.persistence.catalog import ParquetDataCatalog

    cat = ParquetDataCatalog(str(cat_dir))
    out: dict[str, str] = {}
    for instr in cat.instruments():
        out[instr.outcome] = instr.raw_symbol.value
    return out


def _decimal_eq(a: str | None, b: str | None) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    from decimal import Decimal

    return Decimal(a) == Decimal(b)


def run_replay(catalog_directory: str | Path, output: str | Path | None = None) -> dict:
    """Replay, compare final books, and write a replay report. Returns the combined report."""
    result = replay_catalog(catalog_directory)
    comparison = compare_final_books(result, catalog_directory)
    report = {"replay": result.to_dict(), "finalBookComparison": comparison}
    if output is not None:
        Path(output).write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def _main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="prediction_markets_nautilus.replay")
    parser.add_argument("--catalog-directory", required=True)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    print(json.dumps(run_replay(args.catalog_directory, args.output), indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main())
