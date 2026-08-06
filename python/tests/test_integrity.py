"""Post-conversion integrity manifest mutation tests."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from prediction_markets_nautilus.convert import convert_run
from prediction_markets_nautilus.replay import replay_catalog

from . import fixtures as F


def _convert(tmp_path: Path) -> tuple[Path, Path]:
    base = F.sample_events()
    run_dir = tmp_path / "run"
    cat_dir = tmp_path / "catalog"
    m = F.base_market()
    manifest = F.base_manifest(m, finalEventCount=len(base))
    F.write_run(run_dir, manifest, F.base_health(), base, market=m)
    convert_run(run_dir, cat_dir, verify=False, min_reference_observations=1)
    return cat_dir, run_dir


def test_replay_rejects_missing_lineage_sidecar(tmp_path: Path) -> None:
    cat_dir, _ = _convert(tmp_path)
    (cat_dir / "lineage.parquet").unlink()
    with pytest.raises(ValueError, match="lineageSidecarHash mismatch"):
        replay_catalog(cat_dir, allow_unverified=True)


def test_replay_rejects_missing_conversion_report(tmp_path: Path) -> None:
    cat_dir, _ = _convert(tmp_path)
    (cat_dir / "conversion-report.json").unlink()
    with pytest.raises(ValueError, match="conversion-report.json missing"):
        replay_catalog(cat_dir, allow_unverified=True)


def test_replay_rejects_tampered_expected_final_books(tmp_path: Path) -> None:
    cat_dir, _ = _convert(tmp_path)
    path = cat_dir / "expected-final-books.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    for book in data.values():
        book["bestBid"] = ".01"
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    with pytest.raises(ValueError, match="expectedFinalBooksHash mismatch"):
        replay_catalog(cat_dir, allow_unverified=True)


def test_replay_rejects_tampered_conversion_report(tmp_path: Path) -> None:
    cat_dir, _ = _convert(tmp_path)
    path = cat_dir / "conversion-report.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    data["convertedTradeCount"] = 999
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    with pytest.raises(ValueError, match="conversionReportHash mismatch"):
        replay_catalog(cat_dir, allow_unverified=True)


def test_replay_rejects_tampered_normalized_source_input(tmp_path: Path) -> None:
    cat_dir, _ = _convert(tmp_path)
    path = cat_dir / "normalized-source-input.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    data["records"][0]["tickSize"] = "0.99"
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    with pytest.raises(ValueError, match="normalizedSourceInputHash mismatch"):
        replay_catalog(cat_dir, allow_unverified=True)


def test_replay_rejects_tampered_lineage_sidecar(tmp_path: Path) -> None:
    cat_dir, _ = _convert(tmp_path)
    path = cat_dir / "lineage.parquet"
    table = pq.read_table(path)
    rows = table.to_pylist()
    if rows:
        rows[0]["recordType"] = "Tampered"
        pq.write_table(pa.Table.from_pylist(rows), path)
    with pytest.raises(ValueError, match="lineageSidecarHash mismatch"):
        replay_catalog(cat_dir, allow_unverified=True)


def test_replay_rejects_tampered_catalog_deltas(tmp_path: Path) -> None:
    cat_dir, _ = _convert(tmp_path)
    up_path = next(p for p in cat_dir.rglob("*.parquet") if "order_book_deltas" in str(p))
    table = pq.read_table(up_path)
    rows = table.to_pydict()
    for i, side in enumerate(rows["side"]):
        if side == 1:
            rows["side"][i] = 2
            break
    new_table = pa.Table.from_pydict(rows, schema=table.schema)
    pq.write_table(new_table, up_path)
    with pytest.raises(ValueError, match="normalizedEngineInputHash mismatch"):
        replay_catalog(cat_dir, allow_unverified=True)
