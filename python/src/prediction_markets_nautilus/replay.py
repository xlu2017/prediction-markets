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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .backtest import REPLAY_CONFIG
from .hashes import hash_value
from .models import LifecycleEvent, ReferencePriceUpdate

_MS_TO_NS = 1_000_000


@dataclass
class BookState:
    """Reconstructed book state for one instrument at a point in time."""

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
    """Result of replaying a converted catalog."""

    run_id: str
    source_corpus_hash: str
    normalized_engine_input_hash: str
    strategy_visible_input_hash: str
    up_book_after_each_event: list[str]
    down_book_after_each_event: list[str]
    final_up_book_hash: str
    final_down_book_hash: str
    reference_stream_hash: str
    order_intent_stream_hash: str
    final_up_book: BookState
    final_down_book: BookState
    reference_count: int
    event_count: int
    integrity_manifest: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "runId": self.run_id,
            "eventCount": self.event_count,
            "sourceCorpusHash": self.source_corpus_hash,
            "normalizedEngineInputHash": self.normalized_engine_input_hash,
            "strategyVisibleInputHash": self.strategy_visible_input_hash,
            "upBookAfterEachEvent": self.up_book_after_each_event,
            "downBookAfterEachEvent": self.down_book_after_each_event,
            "finalUpBookHash": self.final_up_book_hash,
            "finalDownBookHash": self.final_down_book_hash,
            "referenceStreamHash": self.reference_stream_hash,
            "orderIntentStreamHash": self.order_intent_stream_hash,
            "finalUpBook": _book_to_dict(self.final_up_book),
            "finalDownBook": _book_to_dict(self.final_down_book),
            "referenceCount": self.reference_count,
            "integrityManifest": self.integrity_manifest,
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


def _catalog_read(cat) -> tuple[list, Any, Any | None, list, list]:
    """Read instruments and order-book deltas from a catalog."""
    instruments = cat.instruments()
    if not instruments:
        raise ValueError("no instruments in catalog")
    up_instr = next((i for i in instruments if i.outcome == "UP"), instruments[0])
    down_instr = next((i for i in instruments if i.outcome == "DOWN"), None)
    up_deltas = _read_deltas(cat, up_instr.id)
    down_deltas = _read_deltas(cat, down_instr.id) if down_instr else []
    return instruments, up_instr, down_instr, up_deltas, down_deltas


def _build_input_stream(
    up_deltas: list, down_deltas: list, up_instr, down_instr, cat, cat_dir: Path
) -> list[dict]:
    """Build the canonical input stream used for both engine and strategy hashes."""
    events: list[dict] = []
    for d in up_deltas:
        seq = d.sequence // 1_000_000
        child = d.sequence % 1_000_000
        events.append({"seq": seq, "child": child, "side": "UP", "delta": d})
    for d in down_deltas:
        seq = d.sequence // 1_000_000
        child = d.sequence % 1_000_000
        events.append({"seq": seq, "child": child, "side": "DOWN", "delta": d})
    events.sort(key=lambda e: (e["seq"], e["child"], e["side"]))

    input_stream: list[dict] = []
    for ev in events:
        d = ev["delta"]
        input_stream.append(
            {
                **_to_dict(d),
                "sourceSequence": ev["seq"],
                "childIndex": ev["child"],
                "side": ev["side"],
            }
        )

    refs = cat.custom_data(cls=ReferencePriceUpdate)
    for cd in refs:
        r = cd.data
        input_stream.append({**r.to_dict(), "side": "REFERENCE"})

    trade_ticks = cat.trade_ticks()
    for tt in trade_ticks:
        tid_str = str(tt.trade_id)
        parts = tid_str.rsplit("-", 2)
        tt_seq = int(parts[-2]) if len(parts) == 3 else 0
        tt_child = int(parts[-1]) if len(parts) == 3 else 0
        tr = _to_dict(tt)
        tr["aggressorSide"] = tr.pop("side", "")
        tr["sourceSequence"] = tt_seq
        tr["childIndex"] = tt_child
        tr["side"] = "TRADE"
        input_stream.append(tr)

    lifecycle = cat.custom_data(cls=LifecycleEvent)
    for cd in lifecycle:
        le = cd.data
        input_stream.append({**le.to_dict(), "side": "LIFECYCLE"})

    audit_path = cat_dir / "audit.parquet"
    if audit_path.exists():
        import pyarrow.parquet as pq

        audit_table = pq.read_table(audit_path)
        for row in audit_table.to_pylist():
            input_stream.append({**row, "side": "AUDIT"})

    input_stream.sort(
        key=lambda x: (x.get("sourceSequence", 0), x.get("childIndex", 0), x.get("side", ""))
    )
    return input_stream


def _catalog_integrity_fields(
    input_stream: list[dict], instruments: list, source_line_count: int
) -> dict:
    """Compute the catalog-level integrity hashes and record counts."""
    instrument_records = [type(instr).to_dict(instr) for instr in instruments]
    instrument_records.sort(key=lambda d: d.get("id", ""))
    instrument_catalog_hash = hash_value(instrument_records)

    engine_input_stream = [e for e in input_stream if e.get("side") != "AUDIT"]
    strategy_input_stream = [
        e for e in input_stream if e.get("side") in {"UP", "DOWN", "REFERENCE"}
    ]
    normalized_engine_input_hash = hash_value(
        [engine_input_stream, instrument_records, REPLAY_CONFIG]
    )
    strategy_visible_input_hash = hash_value(strategy_input_stream)

    return {
        "instrumentCatalogHash": instrument_catalog_hash,
        "normalizedEngineInputHash": normalized_engine_input_hash,
        "strategyVisibleInputHash": strategy_visible_input_hash,
        "recordCounts": {
            "sourceCorpusLineCount": source_line_count,
            "instrumentCount": len(instruments),
            "engineInputCount": len(engine_input_stream),
            "strategyVisibleInputCount": len(strategy_input_stream),
        },
    }


def _read_integrity_manifest(cat_dir: Path) -> dict:
    path = cat_dir / "integrity-manifest.json"
    if not path.exists():
        raise ValueError(f"integrity-manifest.json missing in {cat_dir}")
    return json.loads(path.read_text(encoding="utf-8"))


def _read_source_corpus_hash(cat_dir: Path) -> tuple[str, str, int, dict]:
    """Read and validate the persisted source-corpus integrity artifact."""
    path = cat_dir / "source-corpus-hash.json"
    if not path.exists():
        raise ValueError(f"source-corpus-hash.json missing in {cat_dir}")
    data = json.loads(path.read_text(encoding="utf-8"))

    def _require_hex(name: str, value: Any) -> str:
        if not isinstance(value, str) or len(value) != 64:
            raise ValueError(f"invalid {name} in {path}")
        if any(c not in "0123456789abcdef" for c in value):
            raise ValueError(f"invalid {name} in {path}")
        return value

    source_corpus_hash = _require_hex("sourceCorpusHash", data.get("sourceCorpusHash"))
    source_events_hash = _require_hex("sourceEventsHash", data.get("sourceEventsHash"))
    line_count = data.get("sourceCorpusLineCount", 0)
    if not isinstance(line_count, int) or line_count < 0:
        raise ValueError(f"invalid sourceCorpusLineCount in {path}")
    return source_corpus_hash, source_events_hash, line_count, data.get("verifierParameters", {})


def _read_conversion_report(cat_dir: Path) -> dict:
    path = cat_dir / "conversion-report.json"
    if not path.exists():
        raise ValueError(f"conversion-report.json missing in {cat_dir}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if "sourceRunId" not in data:
        raise ValueError("sourceRunId missing in conversion-report.json")
    return data


def replay_catalog(
    catalog_directory: str | Path, *, allow_unverified: bool = False
) -> ReplayResult:
    """Replay a converted catalog deterministically and verify its integrity manifest.

    Events are ordered by ``(sourceSequence, childIndex)`` to preserve the recorder's
    canonical receipt order, not source timestamps which may be delayed, equal, or
    out of order.
    """
    from nautilus_trader.model.book import OrderBook
    from nautilus_trader.model.enums import BookType
    from nautilus_trader.persistence.catalog import ParquetDataCatalog

    cat_dir = Path(catalog_directory)
    cat = ParquetDataCatalog(str(cat_dir))

    instruments, up_instr, down_instr, up_deltas, down_deltas = _catalog_read(cat)

    up_book = OrderBook(up_instr.id, book_type=BookType.L2_MBP)
    down_book = OrderBook(down_instr.id, book_type=BookType.L2_MBP) if down_instr else None

    # Deterministic sort by recorder sequence, then child index, then side as tiebreaker.
    events: list[dict] = []
    for d in up_deltas:
        seq = d.sequence // 1_000_000
        child = d.sequence % 1_000_000
        events.append({"seq": seq, "child": child, "side": "UP", "delta": d})
    for d in down_deltas:
        seq = d.sequence // 1_000_000
        child = d.sequence % 1_000_000
        events.append({"seq": seq, "child": child, "side": "DOWN", "delta": d})
    events.sort(key=lambda e: (e["seq"], e["child"], e["side"]))

    up_after: list[str] = []
    down_after: list[str] = []
    order_intents: list[dict] = []

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

    refs = cat.custom_data(cls=ReferencePriceUpdate)
    ref_records = []
    for cd in refs:
        r = cd.data
        ref_records.append(r.to_dict())
    ref_records.sort(key=lambda x: (x["sourceSequence"], x["childIndex"]))

    input_stream = _build_input_stream(up_deltas, down_deltas, up_instr, down_instr, cat, cat_dir)

    source_corpus_hash, source_events_hash, source_line_count, verifier_parameters = (
        _read_source_corpus_hash(cat_dir)
    )
    integrity_manifest = _read_integrity_manifest(cat_dir)
    conversion_report = _read_conversion_report(cat_dir)

    if not integrity_manifest.get("verifierParameters", {}).get("verify") and not allow_unverified:
        raise ValueError("catalog was converted with verification disabled")

    if integrity_manifest.get("runId") != conversion_report.get("sourceRunId"):
        raise ValueError(
            f"runId mismatch: integrity manifest {integrity_manifest.get('runId')!r} "
            f"vs conversion report {conversion_report.get('sourceRunId')!r}"
        )

    if integrity_manifest.get("sourceCorpusHash") != source_corpus_hash:
        raise ValueError("source corpus hash mismatch")
    if integrity_manifest.get("sourceEventsHash") != source_events_hash:
        raise ValueError("source events hash mismatch")
    if integrity_manifest.get("verifierParameters") != verifier_parameters:
        raise ValueError("verifier parameters mismatch")

    catalog_fields = _catalog_integrity_fields(input_stream, instruments, source_line_count)
    for key, expected in catalog_fields.items():
        stored = integrity_manifest.get(key)
        if stored != expected:
            raise ValueError(f"integrity mismatch for {key}: expected {expected!r}, got {stored!r}")

    return ReplayResult(
        run_id=integrity_manifest.get("runId", conversion_report.get("sourceRunId", "")),
        source_corpus_hash=source_corpus_hash,
        normalized_engine_input_hash=catalog_fields["normalizedEngineInputHash"],
        strategy_visible_input_hash=catalog_fields["strategyVisibleInputHash"],
        up_book_after_each_event=up_after,
        down_book_after_each_event=down_after,
        final_up_book_hash=hash_value(_book_to_dict(final_up)),
        final_down_book_hash=hash_value(_book_to_dict(final_down)),
        reference_stream_hash=hash_value(ref_records),
        order_intent_stream_hash=hash_value(order_intents),
        final_up_book=final_up,
        final_down_book=final_down,
        reference_count=len(ref_records),
        event_count=len(events),
        integrity_manifest=integrity_manifest,
    )


_ACTION_RANK = {"CLEAR": 0, "ADD": 1, "UPDATE": 2, "DELETE": 3}


def _action_name(delta) -> str:
    return delta.action.name if hasattr(delta.action, "name") else str(delta.action)


def _read_deltas(cat, instrument_id) -> list:
    # The catalog's instrument_id filter is unreliable in this version, so filter manually.
    raw = cat.order_book_deltas()
    flat = [d for d in raw if d.instrument_id == instrument_id]
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


def _as_deltas(delta):
    from nautilus_trader.model.data import OrderBookDeltas

    return OrderBookDeltas(delta.instrument_id, [delta])


def _to_dict(obj: Any) -> dict:
    """Safely get a plain dict for a Nautilus data object."""
    if hasattr(obj, "to_dict"):
        try:
            return obj.to_dict()
        except TypeError:
            return type(obj).to_dict(obj)
    return dict(obj)


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


def compare_final_books(result: ReplayResult, catalog_directory: str | Path) -> dict:
    """Phase 9: compare Nautilus-reconstructed final books with recorder finalBookHealth."""
    cat_dir = Path(catalog_directory)
    expected_path = cat_dir / "expected-final-books.json"
    expected = (
        json.loads(expected_path.read_text(encoding="utf-8")) if expected_path.exists() else {}
    )

    instrs = _instrument_token_map(cat_dir)
    comparisons = []
    for outcome, book in (("UP", result.final_up_book), ("DOWN", result.final_down_book)):
        token_id = instrs.get(outcome)
        exp = expected.get(token_id, {}) if token_id else {}
        recon_bid = book.best_bid
        recon_ask = book.best_ask
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


def run_replay(
    catalog_directory: str | Path,
    output: str | Path | None = None,
    *,
    allow_unverified: bool = False,
) -> dict:
    """Replay, compare final books, and write a replay report. Returns the combined report."""
    result = replay_catalog(catalog_directory, allow_unverified=allow_unverified)
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
    parser.add_argument(
        "--allow-unverified",
        action="store_true",
        help="allow replay of a catalog converted with the TypeScript verifier disabled",
    )
    args = parser.parse_args()
    print(
        json.dumps(
            run_replay(
                args.catalog_directory,
                args.output,
                allow_unverified=args.allow_unverified,
            ),
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main())
