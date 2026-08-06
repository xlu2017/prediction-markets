"""Hash content-coverage and provenance fail-closed tests."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from prediction_markets_nautilus.convert import convert_run
from prediction_markets_nautilus.replay import replay_catalog

from . import fixtures as F


def _synthetic_hashes(events: list[dict], tmp_path: Path, **health_overrides) -> dict:
    run_dir = tmp_path / "run"
    cat_dir = tmp_path / "catalog"
    m = F.base_market()
    manifest = F.base_manifest(m, finalEventCount=len(events))
    F.write_run(run_dir, manifest, F.base_health(**health_overrides), events, market=m)
    convert_run(run_dir, cat_dir, verify=False, min_reference_observations=1)
    return replay_catalog(cat_dir, allow_unverified=True).to_dict()


def _health_with_bid(bid: str, ask: str | None = None) -> dict:
    """Return a finalBookHealth dict for a single-token (111111) book."""
    books = copy.deepcopy(F.base_health()["finalBookHealth"])
    books["111111"]["bestBid"] = bid
    if ask is not None:
        books["111111"]["bestAsk"] = ask
    return books


TRADE_SOURCE_COUNTS = {
    "POLYMARKET_MARKET": 1,
    "POLYMARKET_CLOB": 3,
    "POLYMARKET_REFERENCE_PRICE": 1,
}
TRADE_TYPE_COUNTS = {
    "POLYMARKET_MARKET:MARKET_DISCOVERED": 1,
    "POLYMARKET_CLOB:book": 2,
    "POLYMARKET_CLOB:trade": 1,
    "POLYMARKET_REFERENCE_PRICE:REFERENCE_PRICE": 1,
}


def test_hash_sensitivity_book_price_alters_engine_and_strategy(tmp_path: Path) -> None:
    """Changing a book price alters both the engine and strategy-visible input hashes."""
    base = F.sample_events()
    modified = copy.deepcopy(base)
    modified[1]["payload"]["bids"][0]["price"] = ".49"
    h_base = _synthetic_hashes(base, tmp_path)
    h_mod = _synthetic_hashes(modified, tmp_path / "mod", books=_health_with_bid(".49", ".52"))
    assert h_mod["normalizedEngineInputHash"] != h_base["normalizedEngineInputHash"]
    assert h_mod["strategyVisibleInputHash"] != h_base["strategyVisibleInputHash"]


def test_hash_sensitivity_book_size_alters_engine_and_strategy(tmp_path: Path) -> None:
    """Changing a book size alters both the engine and strategy-visible input hashes."""
    base = F.sample_events()
    modified = copy.deepcopy(base)
    modified[1]["payload"]["bids"][0]["size"] = "99"
    h_base = _synthetic_hashes(base, tmp_path)
    h_mod = _synthetic_hashes(modified, tmp_path / "mod")
    assert h_mod["normalizedEngineInputHash"] != h_base["normalizedEngineInputHash"]
    assert h_mod["strategyVisibleInputHash"] != h_base["strategyVisibleInputHash"]


def test_hash_sensitivity_book_ask_price_alters_hashes(tmp_path: Path) -> None:
    """Changing the ask price is visible in the engine and strategy hashes."""
    base = F.sample_events()
    modified = copy.deepcopy(base)
    modified[1]["payload"]["asks"][0]["price"] = ".55"
    h_base = _synthetic_hashes(base, tmp_path)
    h_mod = _synthetic_hashes(modified, tmp_path / "mod", books=_health_with_bid(".48", ".55"))
    assert h_mod["normalizedEngineInputHash"] != h_base["normalizedEngineInputHash"]
    assert h_mod["strategyVisibleInputHash"] != h_base["strategyVisibleInputHash"]


def test_hash_sensitivity_book_timestamp_alters_engine_and_strategy(tmp_path: Path) -> None:
    """Changing a book processedAt (ts_event) alters both engine and strategy hashes."""
    base = F.sample_events()
    modified = copy.deepcopy(base)
    modified[1]["sourceTimestamp"] = 1_800_000_000_000
    modified[1]["receivedAt"] = 1_800_000_000_005
    modified[1]["processedAt"] = 1_800_000_000_006
    h_base = _synthetic_hashes(base, tmp_path)
    h_mod = _synthetic_hashes(modified, tmp_path / "mod")
    assert h_mod["normalizedEngineInputHash"] != h_base["normalizedEngineInputHash"]
    assert h_mod["strategyVisibleInputHash"] != h_base["strategyVisibleInputHash"]


def test_hash_sensitivity_reference_value_alters_engine_and_strategy(tmp_path: Path) -> None:
    """Changing the reference value alters both the engine and strategy-visible hashes."""
    base = F.sample_events()
    modified = copy.deepcopy(base)
    modified[3]["payload"]["payload"]["value"] = "99999.99"
    h_base = _synthetic_hashes(base, tmp_path)
    h_mod = _synthetic_hashes(modified, tmp_path / "mod")
    assert h_mod["normalizedEngineInputHash"] != h_base["normalizedEngineInputHash"]
    assert h_mod["strategyVisibleInputHash"] != h_base["strategyVisibleInputHash"]


def test_hash_sensitivity_reference_topic_alters_engine_and_strategy(tmp_path: Path) -> None:
    """Changing the reference topic alters both the engine and strategy-visible hashes."""
    base = F.sample_events()
    modified = copy.deepcopy(base)
    modified[3]["payload"]["topic"] = "crypto_prices_other"
    h_base = _synthetic_hashes(base, tmp_path)
    h_mod = _synthetic_hashes(modified, tmp_path / "mod")
    assert h_mod["normalizedEngineInputHash"] != h_base["normalizedEngineInputHash"]
    assert h_mod["strategyVisibleInputHash"] != h_base["strategyVisibleInputHash"]


def test_hash_sensitivity_reference_timestamp_alters_hashes(tmp_path: Path) -> None:
    """Changing reference timestamps alters the engine and strategy hashes."""
    base = F.sample_events()
    modified = copy.deepcopy(base)
    modified[3]["sourceTimestamp"] = 1_800_000_000_000
    modified[3]["topLevelSourceTimestamp"] = 1_800_000_000_000
    modified[3]["payloadSourceTimestamp"] = 1_800_000_000_000
    modified[3]["receivedAt"] = 1_800_000_000_005
    modified[3]["processedAt"] = 1_800_000_000_006
    h_base = _synthetic_hashes(base, tmp_path)
    h_mod = _synthetic_hashes(modified, tmp_path / "mod")
    assert h_mod["normalizedEngineInputHash"] != h_base["normalizedEngineInputHash"]
    assert h_mod["strategyVisibleInputHash"] != h_base["strategyVisibleInputHash"]


def test_hash_sensitivity_lifecycle_payload_alters_engine_only(tmp_path: Path) -> None:
    """Changing a lifecycle payload alters source corpus and engine, not strategy."""
    base = F.sample_events()
    modified = copy.deepcopy(base)
    modified[0]["payload"]["active"] = False
    h_base = _synthetic_hashes(base, tmp_path)
    h_mod = _synthetic_hashes(modified, tmp_path / "mod")
    assert h_mod["sourceCorpusHash"] != h_base["sourceCorpusHash"]
    assert h_mod["normalizedEngineInputHash"] != h_base["normalizedEngineInputHash"]
    assert h_mod["strategyVisibleInputHash"] == h_base["strategyVisibleInputHash"]


def test_hash_sensitivity_trade_alters_engine_only(tmp_path: Path) -> None:
    """Adding a trade alters the engine hash, not the strategy-visible input hash."""
    base = F.sample_events()
    trade = F.trade_event(5, "111111", ".49", "10", "BUY", 1785909720120)
    modified = copy.deepcopy(base)
    modified.append(trade)
    h_base = _synthetic_hashes(base, tmp_path)
    h_mod = _synthetic_hashes(
        modified,
        tmp_path / "mod",
        totalEventsReceived=5,
        eventCountsBySource=TRADE_SOURCE_COUNTS,
        eventCountsByType=TRADE_TYPE_COUNTS,
    )
    assert h_mod["sourceCorpusHash"] != h_base["sourceCorpusHash"]
    assert h_mod["normalizedEngineInputHash"] != h_base["normalizedEngineInputHash"]
    assert h_mod["strategyVisibleInputHash"] == h_base["strategyVisibleInputHash"]


def test_hash_sensitivity_trade_side_alters_engine_only(tmp_path: Path) -> None:
    """Changing a trade aggressor side alters the engine hash, not strategy."""
    base = F.sample_events()
    buy = copy.deepcopy(base)
    buy.append(F.trade_event(5, "111111", ".49", "10", "BUY", 1785909720120))
    sell = copy.deepcopy(base)
    sell.append(F.trade_event(5, "111111", ".49", "10", "SELL", 1785909720120))
    health = {
        "totalEventsReceived": 5,
        "eventCountsBySource": TRADE_SOURCE_COUNTS,
        "eventCountsByType": TRADE_TYPE_COUNTS,
    }
    h_base = _synthetic_hashes(buy, tmp_path, **health)
    h_mod = _synthetic_hashes(sell, tmp_path / "mod", **health)
    assert h_mod["sourceCorpusHash"] != h_base["sourceCorpusHash"]
    assert h_mod["normalizedEngineInputHash"] != h_base["normalizedEngineInputHash"]
    assert h_mod["strategyVisibleInputHash"] == h_base["strategyVisibleInputHash"]


def test_hash_sensitivity_audit_only_alters_source_corpus_only(tmp_path: Path) -> None:
    """Adding a duplicate (audit-only) event changes source corpus, not engine or strategy."""
    base = F.sample_events()
    dup = copy.deepcopy(base[1])
    dup["sequence"] = 5
    dup["isDuplicate"] = True
    dup["dedupeState"] = "DUPLICATE_APPLIED"
    dup["duplicateOfSequence"] = 2
    modified = base + [dup]
    h_base = _synthetic_hashes(base, tmp_path)
    h_mod = _synthetic_hashes(
        modified,
        tmp_path / "mod",
        totalEventsReceived=5,
        duplicateEventCount=1,
        eventCountsBySource={
            "POLYMARKET_MARKET": 1,
            "POLYMARKET_CLOB": 3,
            "POLYMARKET_REFERENCE_PRICE": 1,
        },
        eventCountsByType={
            "POLYMARKET_MARKET:MARKET_DISCOVERED": 1,
            "POLYMARKET_CLOB:book": 3,
            "POLYMARKET_REFERENCE_PRICE:REFERENCE_PRICE": 1,
        },
    )
    assert h_mod["sourceCorpusHash"] != h_base["sourceCorpusHash"]
    assert h_mod["normalizedEngineInputHash"] == h_base["normalizedEngineInputHash"]
    assert h_mod["strategyVisibleInputHash"] == h_base["strategyVisibleInputHash"]


def test_hash_sensitivity_event_reordering_changes_hashes(tmp_path: Path) -> None:
    """Swapping the order of the two book events changes the canonical hashes."""
    base = F.sample_events()
    # File order is now market, DOWN, UP, reference with contiguous sequence 1..4.
    modified = copy.deepcopy([base[0], base[2], base[1], base[3]])
    modified[1]["sequence"] = 2
    modified[2]["sequence"] = 3
    h_base = _synthetic_hashes(base, tmp_path)
    h_mod = _synthetic_hashes(modified, tmp_path / "mod")
    assert h_mod["sourceCorpusHash"] != h_base["sourceCorpusHash"]
    assert h_mod["normalizedEngineInputHash"] != h_base["normalizedEngineInputHash"]
    assert h_mod["strategyVisibleInputHash"] != h_base["strategyVisibleInputHash"]


def test_replay_fails_closed_when_source_corpus_hash_missing(tmp_path: Path) -> None:
    """Replay must raise when the source-corpus integrity artifact is missing."""
    base = F.sample_events()
    run_dir = tmp_path / "run"
    cat_dir = tmp_path / "catalog"
    m = F.base_market()
    manifest = F.base_manifest(m, finalEventCount=4)
    F.write_run(run_dir, manifest, F.base_health(), base, market=m)
    convert_run(run_dir, cat_dir, verify=False, min_reference_observations=1)
    (cat_dir / "source-corpus-hash.json").unlink()
    with pytest.raises(ValueError, match="source-corpus-hash.json missing"):
        replay_catalog(cat_dir, allow_unverified=True)


def test_replay_rejects_malformed_source_corpus_hash(tmp_path: Path) -> None:
    """Replay must reject a non-64-character-hex source corpus hash."""
    base = F.sample_events()
    run_dir = tmp_path / "run"
    cat_dir = tmp_path / "catalog"
    m = F.base_market()
    manifest = F.base_manifest(m, finalEventCount=4)
    F.write_run(run_dir, manifest, F.base_health(), base, market=m)
    convert_run(run_dir, cat_dir, verify=False, min_reference_observations=1)
    (cat_dir / "source-corpus-hash.json").write_text(
        json.dumps({"sourceCorpusHash": "deadbeef"}), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="invalid sourceCorpusHash"):
        replay_catalog(cat_dir, allow_unverified=True)


def test_replay_rejects_missing_integrity_manifest(tmp_path: Path) -> None:
    """Replay must raise when the integrity manifest is missing."""
    base = F.sample_events()
    run_dir = tmp_path / "run"
    cat_dir = tmp_path / "catalog"
    m = F.base_market()
    manifest = F.base_manifest(m, finalEventCount=4)
    F.write_run(run_dir, manifest, F.base_health(), base, market=m)
    convert_run(run_dir, cat_dir, verify=False, min_reference_observations=1)
    (cat_dir / "integrity-manifest.json").unlink()
    with pytest.raises(ValueError, match="integrity-manifest.json missing"):
        replay_catalog(cat_dir, allow_unverified=True)


def test_replay_rejects_tampered_catalog(tmp_path: Path) -> None:
    """Replay must raise when the catalog data has been tampered with."""
    base = F.sample_events()
    run_dir = tmp_path / "run"
    cat_dir = tmp_path / "catalog"
    m = F.base_market()
    manifest = F.base_manifest(m, finalEventCount=4)
    F.write_run(run_dir, manifest, F.base_health(), base, market=m)
    convert_run(run_dir, cat_dir, verify=False, min_reference_observations=1)

    up_path = next(
        p
        for p in cat_dir.rglob("*.parquet")
        if "111111" in str(p) and "order_book_deltas" in str(p)
    )
    table = pq.read_table(up_path)
    rows = table.to_pydict()
    for i, side in enumerate(rows["side"]):
        if side == 1:
            rows["side"][i] = 2
            break
    new_table = pa.Table.from_pydict(rows, schema=table.schema)
    pq.write_table(new_table, up_path)

    with pytest.raises(ValueError, match="integrity mismatch"):
        replay_catalog(cat_dir, allow_unverified=True)
