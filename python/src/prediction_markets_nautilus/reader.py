"""Schema-v2 run reader and validator.

Loads ``manifest.json``, ``health.json``, ``market.json`` and streams ``events.jsonl``,
validating schema version 2, run usability, contiguous sequences, and run/market/condition
identity. Decimal values are preserved as strings (the recorder normalizes them to strings).
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
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
    ``usableForReplay == true``. Verifies contiguous sequence numbers and run/market/condition
    identity. Rejects malformed or incomplete runs.
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

    if manifest["completionStatus"] != "COMPLETED":
        raise SchemaError(f"run not COMPLETED: {manifest['completionStatus']}")
    if manifest["captureStatus"] != "COMPLETE":
        raise SchemaError(f"captureStatus not COMPLETE: {manifest['captureStatus']}")
    if not manifest["usableForReplay"]:
        raise SchemaError("manifest.usableForReplay is false")
    if health.get("captureStatus") != "COMPLETE":
        raise SchemaError(f"health.captureStatus not COMPLETE: {health.get('captureStatus')}")
    if not health.get("usableForReplay"):
        raise SchemaError("health.usableForReplay is false")

    run_id = manifest["runId"]
    expected_market_id = manifest["market"].marketId
    expected_condition_id = manifest["market"].conditionId

    events: list[RawEventEnvelope] = []
    expected_seq = 1
    for raw in _iter_jsonl(events_path):
        env = validate_envelope(raw, run_id)
        if env.sequence != expected_seq:
            raise GapError(f"non-contiguous sequence: expected {expected_seq}, got {env.sequence}")
        expected_seq += 1
        if env.marketId is not None and env.marketId != expected_market_id:
            raise SchemaError(
                f"envelope.marketId {env.marketId!r} != run market {expected_market_id!r}"
            )
        if env.conditionId is not None and env.conditionId != expected_condition_id:
            raise SchemaError(
                f"envelope.conditionId {env.conditionId!r} != run condition {expected_condition_id!r}"
            )
        events.append(env)

    final_count = manifest["finalEventCount"]
    if final_count is not None and final_count != len(events):
        raise SchemaError(f"manifest.finalEventCount {final_count} != events count {len(events)}")

    return RunDirectory(path=root, manifest=manifest, health=health, market=market, events=events)
