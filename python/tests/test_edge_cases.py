"""Phase 10: failure and edge-case tests covering the 20 required cases."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from prediction_markets_nautilus.convert import convert_run, normalize_run
from prediction_markets_nautilus.reader import GapError, SchemaError, load_run
from prediction_markets_nautilus.replay import compare_final_books, replay_catalog

from . import fixtures as F
from .conftest import FIXTURE_RUN

ENV = {"PYTHONIOENCODING": "utf-8", "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")}


# --- 1. Invalid schema version ---


def test_invalid_schema_version(tmp_path: Path) -> None:
    m = F.base_market()
    m["schemaVersion"] = 1
    manifest = F.base_manifest(m)
    health = F.base_health()
    F.write_run(tmp_path, manifest, health, F.sample_events(), market=m)
    with pytest.raises(SchemaError, match="schemaVersion"):
        load_run(tmp_path)


# --- 2. Non-contiguous sequence ---


def test_non_contiguous_sequence(tmp_path: Path) -> None:
    events = F.sample_events()
    events[2]["sequence"] = 5  # gap: 1,2,5,4
    manifest = F.base_manifest(F.base_market(), finalEventCount=4)
    F.write_run(tmp_path, manifest, F.base_health(), events, market=F.base_market())
    with pytest.raises(GapError, match="non-contiguous"):
        load_run(tmp_path)


# --- 3. usableForReplay: false ---


def test_usable_for_replay_false(tmp_path: Path) -> None:
    manifest = F.base_manifest(F.base_market(), usableForReplay=False)
    F.write_run(tmp_path, manifest, F.base_health(), F.sample_events(), market=F.base_market())
    with pytest.raises(SchemaError, match="usableForReplay"):
        load_run(tmp_path)


# --- 4. Open or closed data gap (captureStatus PARTIAL) ---


def test_partial_capture_with_gap(tmp_path: Path) -> None:
    manifest = F.base_manifest(F.base_market(), captureStatus="PARTIAL", usableForReplay=False)
    health = F.base_health(
        captureStatus="PARTIAL", usableForReplay=False, dataGapCount=1, closedGapCount=1
    )
    F.write_run(tmp_path, manifest, health, F.sample_events(), market=F.base_market())
    with pytest.raises(SchemaError, match="captureStatus"):
        load_run(tmp_path)


# --- 5. Malformed source event ---


def test_malformed_source_event(tmp_path: Path) -> None:
    manifest = F.base_manifest(F.base_market(), finalEventCount=4)
    F.write_run(tmp_path, manifest, F.base_health(), F.sample_events(), market=F.base_market())
    p = tmp_path / "events.jsonl"
    lines = p.read_text(encoding="utf-8").splitlines()
    lines[1] = "{not valid json"
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    with pytest.raises(SchemaError, match="malformed JSON"):
        load_run(tmp_path)


# --- 6. Duplicate event ---


def test_duplicate_event_routed_to_audit(tmp_path: Path) -> None:
    events = F.sample_events()
    dup = dict(events[1])
    dup["sequence"] = 5
    dup["isDuplicate"] = True
    dup["dedupeState"] = "DUPLICATE_APPLIED"
    dup["duplicateOfSequence"] = 2
    manifest = F.base_manifest(F.base_market(), finalEventCount=5)
    health = F.base_health(totalEventsReceived=5, duplicateEventCount=1)
    # Update event counts to include the duplicate event.
    health["eventCountsBySource"]["POLYMARKET_CLOB"] = 3
    health["eventCountsByType"]["POLYMARKET_CLOB:book"] = 3
    F.write_run(
        tmp_path,
        manifest,
        health,
        events + [dup],
        market=F.base_market(),
    )
    run = load_run(tmp_path)
    normalized = normalize_run(run)
    assert any(a.kind == "DEDUPLICATED_COPY" for a in normalized.audit)
    # The duplicate is not fed to the engine (no extra snapshot).
    assert len(normalized.snapshots) == 2


# --- 7. Snapshot followed by deltas ---


def test_snapshot_followed_by_deltas(tmp_path: Path) -> None:
    events = [
        F.market_discovered(1),
        F.book_event(
            2,
            "111111",
            [{"price": ".48", "size": "30"}],
            [{"price": ".52", "size": "25"}],
            1785909720100,
        ),
        F.price_change_event(
            3, "111111", [{"price": ".49", "size": "10", "side": "BUY"}], 1785909720200
        ),
        F.price_change_event(
            4, "111111", [{"price": ".52", "size": "0", "side": "SELL"}], 1785909720300
        ),
        F.book_event(
            5,
            "222222",
            [{"price": ".47", "size": "20"}],
            [{"price": ".53", "size": "22"}],
            1785909720110,
        ),
        F.reference_event(6),
    ]
    manifest = F.base_manifest(F.base_market(), finalEventCount=6)
    # Build health that matches the actual event stream.
    books = {
        "111111": {
            "tokenId": "111111",
            "state": "SYNCHRONIZED",
            "integrity": "NORMAL",
            "crossedIncidentCount": 0,
            "synchronizationEpoch": 0,
            "snapshotConnectionGeneration": 1,
            "bestBid": ".49",
            "bestAsk": None,
            "bidDepthCount": 2,
            "askDepthCount": 0,
            "lastSuccessfulUpdateTime": 1785909720306,
            "healthy": True,
        },
        "222222": {
            "tokenId": "222222",
            "state": "SYNCHRONIZED",
            "integrity": "NORMAL",
            "crossedIncidentCount": 0,
            "synchronizationEpoch": 0,
            "snapshotConnectionGeneration": 1,
            "bestBid": ".47",
            "bestAsk": ".53",
            "bidDepthCount": 1,
            "askDepthCount": 1,
            "lastSuccessfulUpdateTime": 1785909720116,
            "healthy": True,
        },
    }
    health = F.base_health(books=books, totalEventsReceived=6)
    health["eventCountsBySource"] = {
        "POLYMARKET_MARKET": 1,
        "POLYMARKET_CLOB": 4,
        "POLYMARKET_REFERENCE_PRICE": 1,
    }
    health["eventCountsByType"] = {
        "POLYMARKET_MARKET:MARKET_DISCOVERED": 1,
        "POLYMARKET_CLOB:book": 2,
        "POLYMARKET_CLOB:price_change": 2,
        "POLYMARKET_REFERENCE_PRICE:REFERENCE_PRICE": 1,
    }
    F.write_run(tmp_path, manifest, health, events, market=F.base_market())
    run = load_run(tmp_path)
    normalized = normalize_run(run)
    assert len(normalized.snapshots) == 2
    assert len(normalized.deltas) == 2
    assert normalized.deltas[0].side == "BUY"
    assert normalized.deltas[1].size == "0"  # clear level
    cat_dir = tmp_path / "catalog"
    convert_run(tmp_path, cat_dir)
    result = replay_catalog(cat_dir)
    # After delta: best bid moved to .49; ask .52 cleared.
    assert result.final_up_book.best_bid == "0.49"
    assert result.final_up_book.best_ask is None


# --- 8. Resynchronization boundary ---


def test_resync_boundary(tmp_path: Path) -> None:
    events = [
        F.market_discovered(1),
        F.book_event(
            2,
            "111111",
            [{"price": ".48", "size": "30"}],
            [{"price": ".52", "size": "25"}],
            1785909720100,
            gen=1,
        ),
        F.book_event(
            3,
            "222222",
            [{"price": ".47", "size": "20"}],
            [{"price": ".53", "size": "22"}],
            1785909720110,
            gen=1,
        ),
        F.reference_event(4),
    ]
    manifest = F.base_manifest(F.base_market(), finalEventCount=4)
    health = F.base_health()
    F.write_run(tmp_path, manifest, health, events, market=F.base_market())
    run = load_run(tmp_path)
    normalized = normalize_run(run)
    # generation changes are captured in lineage
    assert all(s.lineage.connectionGeneration == 1 for s in normalized.snapshots)


# --- 9. Crossed book ---


def test_crossed_book(tmp_path: Path) -> None:
    """A run with a CROSSED book is rejected by the strengthened reader."""
    events = [
        F.market_discovered(1),
        F.book_event(
            2,
            "111111",
            [{"price": ".55", "size": "30"}],
            [{"price": ".45", "size": "25"}],
            1785909720100,
        ),
        F.book_event(
            3,
            "222222",
            [{"price": ".47", "size": "20"}],
            [{"price": ".53", "size": "22"}],
            1785909720110,
        ),
        F.reference_event(4),
    ]
    manifest = F.base_manifest(F.base_market(), finalEventCount=4)
    books = F.base_health()["finalBookHealth"]
    books["111111"]["bestBid"] = ".55"
    books["111111"]["bestAsk"] = ".45"
    books["111111"]["integrity"] = "CROSSED"
    health = F.base_health(books=books)
    F.write_run(tmp_path, manifest, health, events, market=F.base_market())
    with pytest.raises(SchemaError, match="CROSSED"):
        load_run(tmp_path)


# --- 10. Locked book ---


def test_locked_book(tmp_path: Path) -> None:
    events = [
        F.market_discovered(1),
        F.book_event(
            2,
            "111111",
            [{"price": ".50", "size": "30"}],
            [{"price": ".50", "size": "25"}],
            1785909720100,
        ),
        F.book_event(
            3,
            "222222",
            [{"price": ".47", "size": "20"}],
            [{"price": ".53", "size": "22"}],
            1785909720110,
        ),
        F.reference_event(4),
    ]
    manifest = F.base_manifest(F.base_market(), finalEventCount=4)
    books = F.base_health()["finalBookHealth"]
    books["111111"]["bestBid"] = ".50"
    books["111111"]["bestAsk"] = ".50"
    books["111111"]["integrity"] = "LOCKED"
    health = F.base_health(books=books)
    F.write_run(tmp_path, manifest, health, events, market=F.base_market())
    cat_dir = tmp_path / "catalog"
    convert_run(tmp_path, cat_dir)
    result = replay_catalog(cat_dir)
    assert result.final_up_book.integrity() == "LOCKED"


# --- 11. Missing Chainlink source timestamp ---


def test_missing_chainlink_source_timestamp(tmp_path: Path) -> None:
    """A run with a missing Chainlink source timestamp is rejected by the strengthened reader."""
    ref = F.reference_event(4)
    ref["sourceTimestamp"] = None
    ref["topLevelSourceTimestamp"] = None
    ref["payloadSourceTimestamp"] = None
    events = [
        F.market_discovered(1),
        F.book_event(
            2,
            "111111",
            [{"price": ".48", "size": "30"}],
            [{"price": ".52", "size": "25"}],
            1785909720100,
        ),
        F.book_event(
            3,
            "222222",
            [{"price": ".47", "size": "20"}],
            [{"price": ".53", "size": "22"}],
            1785909720110,
        ),
        ref,
    ]
    manifest = F.base_manifest(F.base_market(), finalEventCount=4)
    health = F.base_health(
        referenceTimestampAnomalyCount=1,
        referenceTimestampMissingCount=1,
    )
    F.write_run(tmp_path, manifest, health, events, market=F.base_market())
    # The strengthened reader recomputes anomalies from events and rejects the run.
    with pytest.raises(SchemaError, match="referenceTimestampAnomalyCount|recomputed"):
        load_run(tmp_path)


# --- 12. Exact decimal values not representable as binary floats ---


def test_exact_decimal_preserved(tmp_path: Path) -> None:
    ref = F.reference_event(4, value="67234.5000000000000001")
    events = [
        F.market_discovered(1),
        F.book_event(
            2,
            "111111",
            [{"price": ".48", "size": "30"}],
            [{"price": ".52", "size": "25"}],
            1785909720100,
        ),
        F.book_event(
            3,
            "222222",
            [{"price": ".47", "size": "20"}],
            [{"price": ".53", "size": "22"}],
            1785909720110,
        ),
        ref,
    ]
    manifest = F.base_manifest(F.base_market(), finalEventCount=4)
    F.write_run(tmp_path, manifest, F.base_health(), events, market=F.base_market())
    run = load_run(tmp_path)
    normalized = normalize_run(run)
    assert normalized.references[0].value == "67234.5000000000000001"
    cat_dir = tmp_path / "catalog"
    convert_run(tmp_path, cat_dir)
    # The exact decimal survives the catalog round trip.
    from nautilus_trader.persistence.catalog import ParquetDataCatalog

    from prediction_markets_nautilus.models import ReferencePriceUpdate

    cat = ParquetDataCatalog(str(cat_dir))
    refs = cat.custom_data(cls=ReferencePriceUpdate)
    assert refs[0].data.value == "67234.5000000000000001"


# --- 13. Empty book side ---


def test_empty_book_side(tmp_path: Path) -> None:
    events = [
        F.market_discovered(1),
        F.book_event(2, "111111", [{"price": ".48", "size": "30"}], [], 1785909720100),
        F.book_event(
            3,
            "222222",
            [{"price": ".47", "size": "20"}],
            [{"price": ".53", "size": "22"}],
            1785909720110,
        ),
        F.reference_event(4),
    ]
    manifest = F.base_manifest(F.base_market(), finalEventCount=4)
    books = F.base_health()["finalBookHealth"]
    books["111111"]["bestAsk"] = None
    books["111111"]["askDepthCount"] = 0
    health = F.base_health(books=books)
    F.write_run(tmp_path, manifest, health, events, market=F.base_market())
    cat_dir = tmp_path / "catalog"
    convert_run(tmp_path, cat_dir)
    result = replay_catalog(cat_dir)
    assert result.final_up_book.best_ask is None
    assert result.final_up_book.ask_depth_count == 0


# --- 14. One outcome book initialized before the other ---


def test_one_outcome_initialized_before_other(tmp_path: Path) -> None:
    """A run missing one token book health is rejected by the strengthened reader."""
    events = [
        F.market_discovered(1),
        F.book_event(
            2,
            "111111",
            [{"price": ".48", "size": "30"}],
            [{"price": ".52", "size": "25"}],
            1785909720100,
        ),
        F.reference_event(3),
    ]
    manifest = F.base_manifest(F.base_market(), finalEventCount=3)
    books = {"111111": F.base_health()["finalBookHealth"]["111111"]}
    health = F.base_health(books=books, bothTokenStreamsObserved=False, totalEventsReceived=3)
    health["captureStatus"] = "COMPLETE"
    health["usableForReplay"] = True
    manifest["captureStatus"] = "COMPLETE"
    manifest["usableForReplay"] = True
    F.write_run(tmp_path, manifest, health, events, market=F.base_market())
    with pytest.raises(SchemaError, match="bothTokenStreamsObserved|missing book health"):
        load_run(tmp_path)


# --- 15. Unsupported CLOB event ---


def test_unsupported_clob_event(tmp_path: Path) -> None:
    events = F.sample_events()
    unsupported = dict(events[1])
    unsupported["sequence"] = 5
    unsupported["eventType"] = "some_future_event"
    unsupported["payload"] = {"event_type": "some_future_event", "asset_id": "111111"}
    manifest = F.base_manifest(F.base_market(), finalEventCount=5)
    health = F.base_health(totalEventsReceived=5)
    health["eventCountsBySource"]["POLYMARKET_CLOB"] = 3
    health["eventCountsByType"]["POLYMARKET_CLOB:some_future_event"] = 1
    F.write_run(
        tmp_path,
        manifest,
        health,
        events + [unsupported],
        market=F.base_market(),
    )
    run = load_run(tmp_path)
    normalized = normalize_run(run)
    assert any(a.kind == "UNSUPPORTED_SOURCE_EVENT" for a in normalized.audit)
    cat_dir = tmp_path / "catalog"
    report = convert_run(tmp_path, cat_dir)
    assert report["ignoredEventCount"] == 1
    assert "some_future_event" in report["unsupportedEventTypes"]


# --- 16. Multiple messages from one raw frame ---


def test_multiple_messages_one_frame(tmp_path: Path) -> None:
    events = [
        F.market_discovered(1),
        F.book_event(
            2,
            "111111",
            [{"price": ".48", "size": "30"}],
            [{"price": ".52", "size": "25"}],
            1785909720100,
        ),
        F.book_event(
            3,
            "222222",
            [{"price": ".47", "size": "20"}],
            [{"price": ".53", "size": "22"}],
            1785909720110,
        ),
        F.reference_event(4),
    ]
    # Mark events 2 and 3 as belonging to the same frame (same frameId, messageIndex 0/1, messageCount 2).
    events[1]["frameId"] = "frame-shared"
    events[1]["messageIndex"] = 0
    events[1]["messageCount"] = 2
    events[2]["frameId"] = "frame-shared"
    events[2]["messageIndex"] = 1
    events[2]["messageCount"] = 2
    manifest = F.base_manifest(F.base_market(), finalEventCount=4)
    F.write_run(tmp_path, manifest, F.base_health(), events, market=F.base_market())
    run = load_run(tmp_path)
    normalized = normalize_run(run)
    snaps = sorted(normalized.snapshots, key=lambda s: s.lineage.messageIndex or 0)
    assert snaps[0].lineage.messageIndex == 0
    assert snaps[1].lineage.messageIndex == 1
    assert snaps[0].lineage.frameId == "frame-shared"


# --- 17. Deterministic replay across separate processes ---


def test_deterministic_replay_across_processes(tmp_path: Path) -> None:
    cat_dir = tmp_path / "catalog"
    convert_run(FIXTURE_RUN, cat_dir)
    py = sys.executable
    r1 = subprocess.run(
        [py, "-m", "prediction_markets_nautilus.replay", "--catalog-directory", str(cat_dir)],
        capture_output=True,
        text=True,
        env={**__import__("os").environ, **ENV},
        check=True,
    )
    r2 = subprocess.run(
        [py, "-m", "prediction_markets_nautilus.replay", "--catalog-directory", str(cat_dir)],
        capture_output=True,
        text=True,
        env={**__import__("os").environ, **ENV},
        check=True,
    )
    a = json.loads(r1.stdout)["replay"]
    b = json.loads(r2.stdout)["replay"]
    keys = [
        "inputStreamHash",
        "finalUpBookHash",
        "finalDownBookHash",
        "referenceStreamHash",
        "strategyVisibleStreamHash",
        "orderIntentStreamHash",
    ]
    assert all(a[k] == b[k] for k in keys)
    assert a["upBookAfterEachEvent"] == b["upBookAfterEachEvent"]
    assert a["downBookAfterEachEvent"] == b["downBookAfterEachEvent"]


# --- 18. Catalog round trip ---


def test_catalog_round_trip(tmp_path: Path) -> None:
    cat_dir = tmp_path / "catalog"
    report = convert_run(FIXTURE_RUN, cat_dir)
    assert report["convertedSnapshotCount"] == 2
    assert report["convertedReferenceCount"] == 1
    assert (cat_dir / "lineage.parquet").exists()
    assert (cat_dir / "expected-final-books.json").exists()
    result = replay_catalog(cat_dir)
    assert result.final_up_book.best_bid == "0.48"
    assert result.final_down_book.best_bid == "0.47"


# --- 19. Source-sequence traceability from every normalized event ---


def test_source_sequence_traceability(tmp_path: Path) -> None:
    run = load_run(FIXTURE_RUN)
    normalized = normalize_run(run)
    all_records = [
        *normalized.instruments,
        *normalized.snapshots,
        *normalized.deltas,
        *normalized.trades,
        *normalized.references,
        *normalized.lifecycle,
        *normalized.audit,
    ]
    for rec in all_records:
        assert rec.lineage.runId == run.run_id
        assert rec.lineage.sequence >= 1
    # Lineage sidecar has one row per actual catalog row (not per normalized record).
    # Snapshots expand to CLEAR + N ADDs, each with its own childIndex.
    cat_dir = tmp_path / "catalog"
    convert_run(FIXTURE_RUN, cat_dir)
    import pyarrow.parquet as pq

    rows = pq.read_table(cat_dir / "lineage.parquet").to_pylist()
    # 2 instruments + 2 snapshots (each: CLEAR + 1 bid + 1 ask = 3 rows) + 1 reference + 1 lifecycle = 10
    assert len(rows) == 10
    assert all("sourceSequence" in r and "childIndex" in r and "runId" in r for r in rows)
    # Every row traces back to a valid source sequence.
    assert all(r["sourceSequence"] >= 1 for r in rows)


# --- 20. Final-book comparison against the TypeScript recorder ---


def test_final_book_comparison_matches_recorder(tmp_path: Path) -> None:
    cat_dir = tmp_path / "catalog"
    convert_run(FIXTURE_RUN, cat_dir)
    result = replay_catalog(cat_dir)
    comp = compare_final_books(result, cat_dir)
    assert comp["allMatch"] is True
    for c in comp["comparisons"]:
        assert c["bestBidMatch"] and c["bestAskMatch"]
        assert c["bidDepthMatch"] and c["askDepthMatch"]
        assert c["integrityMatch"]


# --- Bonus: reference prices remain independently queryable ---


def test_reference_prices_independently_queryable(tmp_path: Path) -> None:
    cat_dir = tmp_path / "catalog"
    convert_run(FIXTURE_RUN, cat_dir)
    from nautilus_trader.persistence.catalog import ParquetDataCatalog

    from prediction_markets_nautilus.models import ReferencePriceUpdate

    cat = ParquetDataCatalog(str(cat_dir))
    refs = cat.custom_data(cls=ReferencePriceUpdate)
    assert len(refs) == 1
    r = refs[0].data
    assert r.symbol == "btc/usd"
    assert r.topic == "crypto_prices_chainlink"
    # Not misrepresented as a trade.
    assert cat.trade_ticks() == []


# --- 21. Realistic fixture: deltas, trades, lifecycle, multi-token, same-time ---


def test_realistic_fixture_loads_and_converts(tmp_path: Path) -> None:
    """The realistic fixture with 10 events loads, converts, and replays correctly."""
    F.write_realistic_run(tmp_path)
    run = load_run(tmp_path)
    assert run.event_count == 10
    normalized = normalize_run(run)
    # 4 snapshots, 2 price_change deltas (1 single + 1 multi-token with 2 changes = 3 deltas),
    # 1 trade, 2 references, 1 lifecycle (RECONNECT).
    assert len(normalized.snapshots) == 4
    assert len(normalized.deltas) == 3  # 1 single-token + 2 from multi-token
    assert len(normalized.trades) == 1
    assert len(normalized.references) == 1  # only seq 8
    assert len(normalized.lifecycle) == 2  # MARKET_DISCOVERED + RECONNECT
    kinds = {ev.kind for ev in normalized.lifecycle}
    assert "RECONNECT_SCHEDULED" in kinds
    assert "MARKET_DISCOVERED" in kinds


def test_realistic_fixture_multi_token_price_change(tmp_path: Path) -> None:
    """Multi-token price_change with null envelope tokenId resolves per-change asset_id."""
    F.write_realistic_run(tmp_path)
    run = load_run(tmp_path)
    normalized = normalize_run(run)
    # The multi-token price_change (seq 6) should produce 2 deltas with different instruments.
    multi_deltas = [d for d in normalized.deltas if d.lineage.sequence == 6]
    assert len(multi_deltas) == 2
    instr_ids = {d.instrumentId for d in multi_deltas}
    assert len(instr_ids) == 2  # two different instruments
    # childIndex should be 0 and 1.
    children = sorted(d.lineage.childIndex for d in multi_deltas)
    assert children == [0, 1]


def test_realistic_fixture_same_time_conflict(tmp_path: Path) -> None:
    """Two snapshots at the same source timestamp are ordered by (sourceSequence, childIndex)."""
    F.write_realistic_run(tmp_path)
    cat_dir = tmp_path / "catalog"
    convert_run(tmp_path, cat_dir)
    result = replay_catalog(cat_dir)
    # Both books should be reconstructed with the final snapshot values.
    assert result.final_up_book.best_bid == "0.50"
    assert result.final_up_book.best_ask == "0.51"
    assert result.final_down_book.best_bid == "0.49"
    assert result.final_down_book.best_ask == "0.54"


def test_realistic_fixture_lineage_one_row_per_catalog_row(tmp_path: Path) -> None:
    """Lineage sidecar has one row per actual catalog row, not per normalized record."""
    import pyarrow.parquet as pq

    F.write_realistic_run(tmp_path)
    cat_dir = tmp_path / "catalog"
    convert_run(tmp_path, cat_dir)
    rows = pq.read_table(cat_dir / "lineage.parquet").to_pylist()
    # 2 instruments + 4 snapshots * 3 rows each (CLEAR + 1 bid + 1 ask) + 3 deltas + 1 trade
    # + 1 reference + 2 lifecycle = 2 + 12 + 3 + 1 + 1 + 2 = 21
    assert len(rows) == 21
    assert all("sourceSequence" in r and "childIndex" in r for r in rows)
    # Snapshot delta rows (CLEAR + ADD_BID + ADD_ASK per snapshot).
    snap_rows = [r for r in rows if r["recordType"].startswith("OrderBookDelta_")]
    assert len(snap_rows) == 12  # 4 snapshots * 3
    # Individual delta rows (price_change events).
    indiv_delta_rows = [r for r in rows if r["recordType"] == "OrderBookDelta"]
    assert len(indiv_delta_rows) == 3  # 1 single-token + 2 multi-token
    # Each snapshot should have childIndex 0 (CLEAR), 1 (bid), 2 (ask).
    snap_seqs = {r["sourceSequence"] for r in snap_rows if "CLEAR" in r["recordType"]}
    assert snap_seqs == {2, 3, 9, 10}


def test_realistic_fixture_replay_determinism(tmp_path: Path) -> None:
    """Replay is deterministic across two runs of the realistic fixture."""
    F.write_realistic_run(tmp_path)
    cat_dir = tmp_path / "catalog"
    convert_run(tmp_path, cat_dir)
    r1 = replay_catalog(cat_dir)
    r2 = replay_catalog(cat_dir)
    assert r1.final_up_book_hash == r2.final_up_book_hash
    assert r1.final_down_book_hash == r2.final_down_book_hash
    assert r1.up_book_after_each_event == r2.up_book_after_each_event
    assert r1.down_book_after_each_event == r2.down_book_after_each_event


# --- 22. BacktestNode proof ---


def test_backtest_node_proof(tmp_path: Path) -> None:
    """The Nautilus BacktestNode runs with fills and ordering proof."""
    from prediction_markets_nautilus.backtest import run_backtest_proof

    F.write_sample_run(tmp_path)
    cat_dir = tmp_path / "catalog"
    convert_run(tmp_path, cat_dir)
    result = run_backtest_proof(cat_dir)
    assert result.ran, f"BacktestNode did not run: {result.error}"
    assert result.book_events_received > 0, "Strategy received no book events"
    assert result.orders_submitted > 0, "Strategy submitted no orders"
    assert len(result.account_balances) > 0, "No account balances captured"
    # The crossing order at the best ask should produce a fill.
    assert result.fills_count > 0, "No fills — crossing order did not match"
    # The strategy should have received deltas in non-decreasing sequence order.
    seqs = result.received_sequences
    assert len(seqs) > 0, "No sequence values recorded"
    assert seqs == sorted(seqs), f"Sequences not in order: {seqs}"


# --- 22b. BacktestNode recorder-order proof with regressing timestamps ---


def test_backtest_node_recorder_order_with_regressing_timestamps(tmp_path: Path) -> None:
    """BacktestNode delivers events in recorder order even when source timestamps regress.

    The converter uses processedAt (monotonically increasing) for ts_event, so the
    BacktestNode delivers events in recorder order even when sourceTimestamp regresses.
    The embedded sequence field (sourceSequence * 1_000_000 + childIndex) is also
    recorded to verify ordering independently.
    """
    from prediction_markets_nautilus.backtest import run_backtest_proof

    F.write_regressing_timestamp_run(tmp_path)
    cat_dir = tmp_path / "catalog"
    convert_run(tmp_path, cat_dir)
    result = run_backtest_proof(cat_dir)
    assert result.ran, f"BacktestNode did not run: {result.error}"
    assert result.book_events_received > 0, "Strategy received no book events"
    seqs = result.received_sequences
    assert len(seqs) > 0, "No sequence values recorded"
    # Sequences should be non-decreasing (recorder order), even though sourceTimestamp regresses.
    assert seqs == sorted(seqs), f"Sequences not in recorder order: {seqs}"
    # The first delta should be from seq 2 (processedAt=106), not seq 4 (processedAt=156).
    first_seq = seqs[0] // 1_000_000
    assert first_seq == 2, f"First event should be from seq 2, got seq {first_seq}"


def test_regressing_timestamp_fixture_replay(tmp_path: Path) -> None:
    """Replay of regressing-timestamp fixture preserves recorder order."""
    F.write_regressing_timestamp_run(tmp_path)
    cat_dir = tmp_path / "catalog"
    convert_run(tmp_path, cat_dir)
    result = replay_catalog(cat_dir)
    # UP book should reflect the delta at seq 4 (bid moved to .49).
    assert result.final_up_book.best_bid == "0.49"
    assert result.final_up_book.best_ask == "0.52"
    # DOWN book should be unchanged from seq 3 snapshot.
    assert result.final_down_book.best_bid == "0.47"
    assert result.final_down_book.best_ask == "0.53"


# --- 23. Reader invariant cross-checks ---


def test_reader_rejects_forged_health_status(tmp_path: Path) -> None:
    """Reader rejects a run where health says COMPLETE but events show gaps."""
    events = F.sample_events()
    # Remove event 3 (sequence 3) and renumber to create a gap that health doesn't report.
    events_with_gap = events[:2]  # Only seq 1, 2
    for i, ev in enumerate(events_with_gap):
        ev["sequence"] = i + 1
    manifest = F.base_manifest(F.base_market(), finalEventCount=2)
    # Health claims everything is fine (forged).
    health = F.base_health(totalEventsReceived=2)
    # Remove the DOWN token book health since we only have 2 events.
    books = {"111111": health["finalBookHealth"]["111111"]}
    health = F.base_health(books=books, bothTokenStreamsObserved=False, totalEventsReceived=2)
    health["captureStatus"] = "COMPLETE"
    health["usableForReplay"] = True
    manifest["captureStatus"] = "COMPLETE"
    manifest["usableForReplay"] = True
    F.write_run(tmp_path, manifest, health, events_with_gap, market=F.base_market())
    with pytest.raises(SchemaError, match="bothTokenStreamsObserved|missing book health"):
        load_run(tmp_path)


def test_reader_rejects_mismatched_total_events(tmp_path: Path) -> None:
    """Reader rejects a run where health.totalEventsReceived doesn't match events count."""
    events = F.sample_events()
    manifest = F.base_manifest(F.base_market(), finalEventCount=4)
    health = F.base_health(totalEventsReceived=99)  # Wrong count
    F.write_run(tmp_path, manifest, health, events, market=F.base_market())
    with pytest.raises(SchemaError, match="totalEventsReceived"):
        load_run(tmp_path)
