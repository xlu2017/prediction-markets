"""Real recording acceptance gate for the Nautilus replay pipeline.

Runs a source run through the full conversion -> replay -> backtest chain, compares the
replayed final books against the expected health sidecar, and emits a JSON acceptance
report with timing, file sizes, and integrity metrics.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

repo_root = Path(__file__).resolve().parents[2]
src_root = repo_root / "python" / "src"
if str(src_root) not in sys.path:
    sys.path.insert(0, str(src_root))

from prediction_markets_nautilus.backtest import run_backtest_proof
from prediction_markets_nautilus.convert import convert_run
from prediction_markets_nautilus.replay import compare_final_books, replay_catalog


def _human_size(size: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}TB"


def _catalog_size(catalog_directory: Path) -> int:
    return sum(p.stat().st_size for p in catalog_directory.rglob("*") if p.is_file())


def main() -> int:
    parser = argparse.ArgumentParser(prog="real_recording_gate")
    parser.add_argument("--run-directory", required=True, help="schema-v2 recording to evaluate")
    parser.add_argument("--catalog-directory", required=True, help="where to write the converted catalog")
    parser.add_argument("--output", default=None, help="JSON report output path")
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="skip the TypeScript source verifier (not recommended for real recordings)",
    )
    parser.add_argument(
        "--min-reference-observations",
        type=int,
        default=5,
        help="minimum unique Chainlink observations required",
    )
    parser.add_argument(
        "--allow-unverified",
        action="store_true",
        help="allow replay/backtest of a catalog that was not TypeScript-verified",
    )
    args = parser.parse_args()

    run_dir = Path(args.run_directory)
    cat_dir = Path(args.catalog_directory)
    accepted = True
    report: dict[str, object] = {
        "runDirectory": str(run_dir),
        "catalogDirectory": str(cat_dir),
        "verified": not args.no_verify,
        "acceptanceDecision": "ACCEPTED",
        "stages": {},
    }

    # Conversion
    t0 = time.perf_counter()
    try:
        conversion_report = convert_run(
            run_dir,
            cat_dir,
            verify=not args.no_verify,
            min_reference_observations=args.min_reference_observations,
        )
    except Exception as e:
        accepted = False
        report["acceptanceDecision"] = "REJECTED"
        report["rejectionReason"] = f"conversion failed: {e}"
        report["stages"]["conversion"] = {"ok": False, "error": str(e), "elapsedSeconds": time.perf_counter() - t0}
        _emit(report, args.output)
        return 1

    report["stages"]["conversion"] = {
        "ok": True,
        "elapsedSeconds": time.perf_counter() - t0,
        "sourceEventCount": conversion_report.get("sourceEventCount"),
        "catalogRowCount": conversion_report.get("catalogRowCount"),
    }

    # Replay with fail-closed integrity check
    t0 = time.perf_counter()
    try:
        replay_result = replay_catalog(cat_dir, allow_unverified=args.allow_unverified)
    except Exception as e:
        accepted = False
        report["acceptanceDecision"] = "REJECTED"
        report["rejectionReason"] = f"replay/integrity verification failed: {e}"
        report["stages"]["replay"] = {"ok": False, "error": str(e), "elapsedSeconds": time.perf_counter() - t0}
        _emit(report, args.output)
        return 1

    report["stages"]["replay"] = {
        "ok": True,
        "elapsedSeconds": time.perf_counter() - t0,
        "eventCount": replay_result.event_count,
        "referenceCount": replay_result.reference_count,
        "sourceCorpusHash": replay_result.source_corpus_hash,
        "normalizedEngineInputHash": replay_result.normalized_engine_input_hash,
        "strategyVisibleInputHash": replay_result.strategy_visible_input_hash,
        "finalUpBookHash": replay_result.final_up_book_hash,
        "finalDownBookHash": replay_result.final_down_book_hash,
        "referenceStreamHash": replay_result.reference_stream_hash,
        "orderIntentStreamHash": replay_result.order_intent_stream_hash,
    }

    # Backtest / ledger proof
    t0 = time.perf_counter()
    backtest = run_backtest_proof(
        cat_dir, allow_unverified=args.allow_unverified
    )
    report["stages"]["backtest"] = {
        "ok": backtest.ran,
        "elapsedSeconds": time.perf_counter() - t0,
        "ran": backtest.ran,
        "bookEventsReceived": backtest.book_events_received,
        "referenceEventsReceived": backtest.reference_events_received,
        "ordersSubmitted": backtest.orders_submitted,
        "fillsCount": backtest.fills_count,
        "ledgerMatch": backtest.ledger_match,
        "error": backtest.error,
    }
    if not backtest.ran:
        accepted = False
        report["acceptanceDecision"] = "REJECTED"
        report["rejectionReason"] = f"backtest proof failed: {backtest.error}"

    # Final book comparison
    comparison = compare_final_books(replay_result, cat_dir)
    report["stages"]["finalBookComparison"] = {
        "allMatch": comparison["allMatch"],
        "comparisons": comparison["comparisons"],
    }
    if not comparison["allMatch"]:
        accepted = False
        report["acceptanceDecision"] = "REJECTED"
        report["rejectionReason"] = "final book comparison mismatch"

    report["catalogSizeBytes"] = _catalog_size(cat_dir)
    report["catalogSizeHuman"] = _human_size(report["catalogSizeBytes"])
    if not accepted:
        _emit(report, args.output)
        return 1

    _emit(report, args.output)
    return 0


def _emit(report: dict[str, object], output: str | None) -> None:
    text = json.dumps(report, indent=2)
    if output:
        Path(output).write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
