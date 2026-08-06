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
        b = float(self.best_bid)
        a = float(self.best_ask)
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
    """Replay a converted catalog deterministically and return hashes + final book state."""
    from nautilus_trader.model.book import OrderBook
    from nautilus_trader.model.enums import BookType
    from nautilus_trader.persistence.catalog import ParquetDataCatalog

    cat_dir = Path(catalog_directory)
    cat = ParquetDataCatalog(str(cat_dir))

    instruments = cat.instruments()
    if not instruments:
        raise ValueError("no instruments in catalog")
    # Identify Up and Down by outcome attribute.
    up_instr = next((i for i in instruments if i.outcome == "UP"), instruments[0])
    down_instr = next((i for i in instruments if i.outcome == "DOWN"), None)

    up_book = OrderBook(up_instr.id, book_type=BookType.L2_MBP)
    down_book = OrderBook(down_instr.id, book_type=BookType.L2_MBP) if down_instr else None

    # Read all order-book deltas grouped by instrument, sorted deterministically.
    up_deltas = _read_deltas(cat, up_instr.id)
    down_deltas = _read_deltas(cat, down_instr.id) if down_instr else []

    # Merge into a single deterministic event stream ordered by ts_event, then ts_init,
    # then instrument outcome, then side, then price.
    events: list[dict] = []
    for d in up_deltas:
        events.append({"ts": d.ts_event, "tsi": d.ts_init, "side": "UP", "delta": d})
    for d in down_deltas:
        events.append({"ts": d.ts_event, "tsi": d.ts_init, "side": "DOWN", "delta": d})
    events.sort(key=lambda e: (e["ts"], e["tsi"], e["side"]))

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
        # Strategy-visible event stream: deterministic snapshot of best bid/ask per event.
        strategy_visible.append(
            {
                "side": side_key,
                "ts": ev["ts"],
                "bestBid": state.best_bid,
                "bestAsk": state.best_ask,
            }
        )
        # Trivial deterministic strategy: record an intent (never submit) when best bid < 0.50.
        if state.best_bid is not None and float(state.best_bid) < 0.50:
            order_intents.append(
                {
                    "side": side_key,
                    "ts": ev["ts"],
                    "predicate": "bestBid<0.50",
                    "bestBid": state.best_bid,
                    "action": "INTENT_ONLY_NO_ORDER",
                }
            )
        input_stream.append({"side": side_key, "ts": ev["ts"], "action": str(delta.action)})

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

    # Reference-price stream from custom data.
    refs = cat.custom_data(cls=ReferencePriceUpdate)
    ref_records = []
    for cd in refs:
        r = cd.data
        ref_records.append(
            {
                "symbol": r.symbol,
                "topic": r.topic,
                "value": r.value,
                "topLevelSourceTimestamp": r.topLevelSourceTimestamp,
                "payloadSourceTimestamp": r.payloadSourceTimestamp,
                "ts_event": r.ts_event,
                "ts_init": r.ts_init,
            }
        )
    ref_records.sort(key=lambda x: (x["ts_event"], x["ts_init"]))

    # Lifecycle events (audit/catalog sidecar).
    lifecycle = cat.custom_data(cls=LifecycleEvent)
    for cd in lifecycle:
        le = cd.data
        input_stream.append({"side": "LIFECYCLE", "ts": le.ts_event, "kind": le.kind})

    input_stream.sort(key=lambda x: (x["ts"], x.get("side", "")))

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


def _read_deltas(cat, instrument_id) -> list:
    # The catalog's instrument_id filter is unreliable in this version, so filter manually.
    raw = cat.order_book_deltas()
    flat = [d for d in raw if d.instrument_id == instrument_id]
    # Deterministic sort: CLEAR must precede ADDs within a snapshot (ts_event shared).
    flat.sort(
        key=lambda d: (
            d.ts_event,
            d.ts_init,
            _ACTION_RANK.get(d.action.name, 9),
            str(d.order.side) if d.order else "",
            str(d.order.price) if d.order else "",
        )
    )
    return flat


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
