"""Deterministic hashing helpers for replay verification."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any


def _default(obj: Any) -> Any:
    if is_dataclass(obj) and not isinstance(obj, type):
        return asdict(obj)
    if isinstance(obj, (set, tuple)):
        return sorted(obj)
    return str(obj)


def canonical_json(obj: Any) -> str:
    """Canonical JSON with sorted keys for deterministic hashing."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=_default)


def hash_value(obj: Any) -> str:
    """SHA-256 hex digest of the canonical JSON encoding of ``obj``."""
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


def hash_json_file(path: Path) -> str:
    """Canonical hash of a JSON file (re-serialized for normalization)."""
    return hash_value(json.loads(path.read_text(encoding="utf-8")))


def hash_parquet_rows(path: Path) -> str:
    """Canonical hash of a Parquet sidecar sorted by catalogRecordId."""
    if not path.exists():
        return ""
    import pyarrow.parquet as pq

    table = pq.read_table(path)
    rows = table.to_pylist()
    rows.sort(
        key=lambda r: (
            r.get("catalogRecordId", ""),
            r.get("sourceSequence", 0),
            r.get("childIndex", 0),
        )
    )
    return hash_value(rows)
