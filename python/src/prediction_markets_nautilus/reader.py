"""Schema-v2 run reader and validator.

Loads ``manifest.json``, ``health.json``, ``market.json`` and streams ``events.jsonl``,
validating schema version 2, run usability, contiguous sequences, and run/market/condition
identity. Decimal values are preserved as strings (the recorder normalizes them to strings).
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from .schema_v2 import (
    GapError,
    MarketSpecification,
    RawEventEnvelope,
    SchemaError,
    validate_envelope,
    validate_health,
    validate_manifest,
    validate_market,
)


@dataclass(frozen=True)
class RunDirectory:
    path: Path
    manifest: dict
    health: dict
    market: MarketSpecification
    events: list[RawEventEnvelope]

    @property
    def run_id(self) -> str:
        return self.manifest["runId"]

    @property
    def event_count(self) -> int:
        return len(self.events)


def _load_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _iter_jsonl(path: Path) -> Iterator[object]:
    with path.open("r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                yield json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise SchemaError(f"malformed JSON at {path.name}:{lineno}: {exc}") from exc


def load_run(run_directory: str | Path) -> RunDirectory:
    """Load and validate a schema-v2 run directory.

    Requires ``completionStatus == COMPLETED``, ``captureStatus == COMPLETE`` and
    ``usableForReplay == true``. Independently cross-checks recorder invariants rather
    than trusting health status fields:

    - manifest/health run ID and startedAt consistency
    - manifest/health threshold consistency (clobStaleAfterMs, referenceStaleAfterMs)
    - event count consistency (finalEventCount, totalEventsReceived)
    - contiguous sequence numbers
    - run/market/condition identity across all events
    - no open/closed gaps
    - no malformed events
    - no quality failures
    - both token streams observed
    - reference price stream observed and fresh
    - reference timestamp anomalies recomputed from events
    - final book health: expected token IDs present, state SYNCHRONIZED, healthy, not crossed
    """
    root = Path(run_directory)
    if not root.is_dir():
        raise SchemaError(f"run directory does not exist: {root}")

    manifest_path = root / "manifest.json"
    health_path = root / "health.json"
    market_path = root / "market.json"
    events_path = root / "events.jsonl"
    for p in (manifest_path, health_path, events_path):
        if not p.exists():
            raise SchemaError(f"missing required file: {p}")

    manifest = validate_manifest(_load_json(manifest_path))
    health = validate_health(_load_json(health_path))
    market = (
        validate_market(_load_json(market_path)) if market_path.exists() else manifest["market"]
    )

    errors: list[str] = []

    # --- Status checks ---
    if manifest["completionStatus"] != "COMPLETED":
        errors.append(f"completionStatus is {manifest['completionStatus']}, expected COMPLETED")
    if manifest["captureStatus"] != "COMPLETE":
        errors.append(f"manifest.captureStatus is {manifest['captureStatus']}, expected COMPLETE")
    if not manifest["usableForReplay"]:
        errors.append("manifest.usableForReplay is false")
    if health.get("captureStatus") != "COMPLETE":
        errors.append(f"health.captureStatus is {health.get('captureStatus')}, expected COMPLETE")
    if not health.get("usableForReplay"):
        errors.append("health.usableForReplay is false")

    # --- Cross-file consistency ---
    run_id = manifest["runId"]
    if health.get("runId") != run_id:
        errors.append(f"health.runId {health.get('runId')!r} != manifest.runId {run_id!r}")
    if health.get("startedAt") != manifest.get("startedAt"):
        errors.append(
            f"health.startedAt {health.get('startedAt')!r} != manifest.startedAt {manifest.get('startedAt')!r}"
        )
    if health.get("clobStaleAfterMs") != manifest.get("clobStaleAfterMs"):
        errors.append(
            f"health.clobStaleAfterMs {health.get('clobStaleAfterMs')} != "
            f"manifest.clobStaleAfterMs {manifest.get('clobStaleAfterMs')}"
        )
    if health.get("referenceStaleAfterMs") != manifest.get("referenceStaleAfterMs"):
        errors.append(
            f"health.referenceStaleAfterMs {health.get('referenceStaleAfterMs')} != "
            f"manifest.referenceStaleAfterMs {manifest.get('referenceStaleAfterMs')}"
        )

    # --- Health invariant checks (don't trust status — check the actual counts) ---
    if health.get("malformedEventCount", 0) > 0:
        errors.append(f"malformedEventCount is {health.get('malformedEventCount')}, expected 0")
    if health.get("staleFeedIncidents", 0) > 0:
        errors.append(f"staleFeedIncidents is {health.get('staleFeedIncidents')}, expected 0")
    if health.get("dataGapCount", 0) > 0:
        errors.append(f"dataGapCount is {health.get('dataGapCount')}, expected 0")
    if health.get("closedGapCount", 0) > 0:
        errors.append(f"closedGapCount is {health.get('closedGapCount')}, expected 0")
    open_gaps = health.get("openGapsByFeed", {})
    if isinstance(open_gaps, dict) and len(open_gaps) > 0:
        errors.append(f"openGapsByFeed is non-empty: {list(open_gaps.keys())}")
    quality_failures = health.get("qualityFailures", [])
    if isinstance(quality_failures, list) and len(quality_failures) > 0:
        codes = [f.get("code", "?") for f in quality_failures if isinstance(f, dict)]
        errors.append(f"qualityFailures is non-empty: {codes}")
    if not health.get("bothTokenStreamsObserved", False):
        errors.append("bothTokenStreamsObserved is false")
    if not health.get("referencePriceStreamObserved", False):
        errors.append("referencePriceStreamObserved is false")
    if not health.get("referenceFresh", False):
        errors.append("referenceFresh is false")
    if health.get("referenceTimestampAnomalyCount", 0) > 0:
        errors.append(
            f"referenceTimestampAnomalyCount is {health.get('referenceTimestampAnomalyCount')}, expected 0"
        )

    # --- Final book health checks ---
    expected_market_id = manifest["market"].marketId
    expected_condition_id = manifest["market"].conditionId
    expected_token_ids = {manifest["market"].upTokenId, manifest["market"].downTokenId}
    fbh = health.get("finalBookHealth", {})
    if not isinstance(fbh, dict):
        errors.append("finalBookHealth is not an object")
    else:
        for tid, book in fbh.items():
            if not isinstance(book, dict):
                errors.append(f"finalBookHealth.{tid} is not an object")
                continue
            if tid not in expected_token_ids:
                errors.append(f"finalBookHealth has unexpected token ID {tid}")
            if book.get("state") != "SYNCHRONIZED":
                errors.append(
                    f"book for token {tid} state is {book.get('state')}, expected SYNCHRONIZED"
                )
            if not book.get("healthy", False):
                errors.append(f"book for token {tid} is not healthy")
            if book.get("integrity") == "CROSSED":
                errors.append(f"book for token {tid} is CROSSED")
            if book.get("crossedIncidentCount", 0) > 0:
                errors.append(
                    f"book for token {tid} had {book.get('crossedIncidentCount')} crossed incident(s)"
                )
        for expected in expected_token_ids:
            if expected not in fbh:
                errors.append(f"missing book health for expected token ID {expected}")

    # --- Load and validate events ---
    events: list[RawEventEnvelope] = []
    expected_seq = 1
    for raw in _iter_jsonl(events_path):
        env = validate_envelope(raw, run_id)
        if env.sequence != expected_seq:
            raise GapError(f"non-contiguous sequence: expected {expected_seq}, got {env.sequence}")
        expected_seq += 1
        if env.marketId is not None and env.marketId != expected_market_id:
            errors.append(
                f"envelope seq {env.sequence} marketId {env.marketId!r} != run market {expected_market_id!r}"
            )
        if env.conditionId is not None and env.conditionId != expected_condition_id:
            errors.append(
                f"envelope seq {env.sequence} conditionId {env.conditionId!r} != run condition {expected_condition_id!r}"
            )
        events.append(env)

    # --- Event count consistency ---
    final_count = manifest["finalEventCount"]
    if final_count is not None and final_count != len(events):
        errors.append(f"manifest.finalEventCount {final_count} != events count {len(events)}")
    total_received = health.get("totalEventsReceived")
    if isinstance(total_received, int) and total_received != len(events):
        errors.append(f"health.totalEventsReceived {total_received} != events count {len(events)}")

    # --- Recompute reference anomalies from events ---
    ref_obs: set[int] = set()
    recomp_missing = 0
    recomp_stale = 0
    recomp_future = 0
    ref_stale_after = health.get("referenceStaleAfterMs") or 10000
    ref_clock_skew = health.get("referenceClockSkewMs") or 5000
    for env in events:
        # Only accepted reference observations affect reference health.
        if env.eventType == "REFERENCE_PRICE" and env.dedupeState == "UNIQUE_APPLIED":
            if env.sourceTimestamp is not None:
                ref_obs.add(env.sourceTimestamp)
                source_age = env.receivedAt - env.sourceTimestamp
                if source_age < -ref_clock_skew:
                    recomp_future += 1
                elif source_age > ref_stale_after:
                    recomp_stale += 1
            else:
                recomp_missing += 1
    recomp_anomaly = recomp_missing + recomp_stale + recomp_future
    if recomp_anomaly != health.get("referenceTimestampAnomalyCount", 0):
        errors.append(
            f"recomputed reference anomaly count {recomp_anomaly} != "
            f"health.referenceTimestampAnomalyCount {health.get('referenceTimestampAnomalyCount', 0)}"
        )
    if recomp_missing != health.get("referenceTimestampMissingCount", 0):
        errors.append(
            f"recomputed missing count {recomp_missing} != "
            f"health.referenceTimestampMissingCount {health.get('referenceTimestampMissingCount', 0)}"
        )
    if recomp_stale != health.get("referenceStaleAtReceiptCount", 0):
        errors.append(
            f"recomputed stale-at-receipt count {recomp_stale} != "
            f"health.referenceStaleAtReceiptCount {health.get('referenceStaleAtReceiptCount', 0)}"
        )
    if recomp_future != health.get("referenceFutureSourceSkewCount", 0):
        errors.append(
            f"recomputed future-skew count {recomp_future} != "
            f"health.referenceFutureSourceSkewCount {health.get('referenceFutureSourceSkewCount', 0)}"
        )

    # --- Verify eventCountsBySource and eventCountsByType ---
    # The recorder increments counters for every persisted envelope, including
    # audit-only events (DUPLICATE_APPLIED, RECEIVED_NOT_APPLIED). Use canonical
    # dictionary equality for exact comparison.
    recomp_by_source: dict[str, int] = {}
    recomp_by_type: dict[str, int] = {}
    for env in events:
        recomp_by_source[env.source] = recomp_by_source.get(env.source, 0) + 1
        type_key = f"{env.source}:{env.eventType}"
        recomp_by_type[type_key] = recomp_by_type.get(type_key, 0) + 1
    health_by_source = health.get("eventCountsBySource", {}) or {}
    if isinstance(health_by_source, dict) and recomp_by_source != health_by_source:
        errors.append(
            f"eventCountsBySource mismatch: health={health_by_source} != recomputed={recomp_by_source}"
        )
    health_by_type = health.get("eventCountsByType", {}) or {}
    if isinstance(health_by_type, dict) and recomp_by_type != health_by_type:
        errors.append(
            f"eventCountsByType mismatch: health={health_by_type} != recomputed={recomp_by_type}"
        )

    # --- Reconstruct final books from events and compare with finalBookHealth ---
    up_token = manifest["market"].upTokenId
    down_token = manifest["market"].downTokenId
    for token_id, expected_book in (
        [
            (up_token, fbh.get(up_token)),
            (down_token, fbh.get(down_token)),
        ]
        if isinstance(fbh, dict)
        else []
    ):
        if not isinstance(expected_book, dict):
            continue
        recon_bids: dict[str, str] = {}
        recon_asks: dict[str, str] = {}
        for env in events:
            # Only replay events the recorder actually applied.
            if env.dedupeState != "UNIQUE_APPLIED":
                continue
            # Skip events that don't affect this token.
            if env.eventType == "book":
                if env.tokenId != token_id:
                    continue
            elif env.eventType == "price_change":
                pass  # check per-change asset_id below
            else:
                continue
            payload = env.payload if isinstance(env.payload, dict) else {}
            if env.eventType == "book":
                # Snapshot: clear and rebuild.
                recon_bids.clear()
                recon_asks.clear()
                for lv in payload.get("bids", []):
                    if (
                        isinstance(lv, dict)
                        and isinstance(lv.get("price"), str)
                        and isinstance(lv.get("size"), str)
                    ):
                        if lv["size"] != "0":
                            recon_bids[lv["price"]] = lv["size"]
                        else:
                            recon_bids.pop(lv["price"], None)
                for lv in payload.get("asks", []):
                    if (
                        isinstance(lv, dict)
                        and isinstance(lv.get("price"), str)
                        and isinstance(lv.get("size"), str)
                    ):
                        if lv["size"] != "0":
                            recon_asks[lv["price"]] = lv["size"]
                        else:
                            recon_asks.pop(lv["price"], None)
            elif env.eventType == "price_change":
                for ch in payload.get("price_changes", []):
                    if not isinstance(ch, dict):
                        continue
                    ch_token = ch.get("asset_id") or env.tokenId
                    if ch_token != token_id:
                        continue
                    price = ch.get("price")
                    size = ch.get("size")
                    side = ch.get("side")
                    if not isinstance(price, str) or not isinstance(size, str):
                        continue
                    # Reject unknown sides — do not default to SELL.
                    if side not in ("BUY", "SELL"):
                        errors.append(f"price_change for {token_id} has unknown side {side!r}")
                        continue
                    book = recon_bids if side == "BUY" else recon_asks
                    if size != "0":
                        book[price] = size
                    else:
                        book.pop(price, None)
        # Compare reconstructed best bid/ask with health using Decimal ordering.
        from decimal import Decimal as _Decimal

        recon_best_bid = max(recon_bids, key=lambda p: _Decimal(p)) if recon_bids else None
        recon_best_ask = min(recon_asks, key=lambda p: _Decimal(p)) if recon_asks else None
        exp_best_bid = expected_book.get("bestBid")
        exp_best_ask = expected_book.get("bestAsk")
        if exp_best_bid is not None and recon_best_bid is not None:
            if Decimal(exp_best_bid) != Decimal(recon_best_bid):
                errors.append(
                    f"reconstructed best bid for {token_id} = {recon_best_bid} != health {exp_best_bid}"
                )
        if exp_best_ask is not None and recon_best_ask is not None:
            if Decimal(exp_best_ask) != Decimal(recon_best_ask):
                errors.append(
                    f"reconstructed best ask for {token_id} = {recon_best_ask} != health {exp_best_ask}"
                )
        exp_bid_depth = expected_book.get("bidDepthCount")
        if isinstance(exp_bid_depth, int) and exp_bid_depth != len(recon_bids):
            errors.append(
                f"reconstructed bid depth for {token_id} = {len(recon_bids)} != health {exp_bid_depth}"
            )
        exp_ask_depth = expected_book.get("askDepthCount")
        if isinstance(exp_ask_depth, int) and exp_ask_depth != len(recon_asks):
            errors.append(
                f"reconstructed ask depth for {token_id} = {len(recon_asks)} != health {exp_ask_depth}"
            )

    if errors:
        msg = "\n  - " + "\n  - ".join(errors)
        raise SchemaError(f"run verification failed with {len(errors)} error(s):{msg}")

    return RunDirectory(path=root, manifest=manifest, health=health, market=market, events=events)
