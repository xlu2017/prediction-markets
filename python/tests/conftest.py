"""Conftest: make the repo fixture and package importable."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

REPO_ROOT = ROOT.parent
FIXTURE_RUN = REPO_ROOT / "fixtures" / "sample-run"
