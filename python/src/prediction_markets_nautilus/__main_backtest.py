"""CLI entry point for the BacktestNode proof.

Separated from ``backtest.py`` to avoid the ``__main__`` / importable module
dual-instance problem: when run as ``python -m prediction_markets_nautilus.backtest``,
the module is loaded as ``__main__``, but ``ImportableStrategyConfig`` imports
``prediction_markets_nautilus.backtest:ProofStrategy`` separately. Class variables
modified by the strategy would then be on a different class object than the one
``run_backtest_proof()`` reads from.

By keeping the CLI in this separate module, ``backtest.py`` is always imported
as ``prediction_markets_nautilus.backtest`` — never as ``__main__`` — so the
class variables are consistent.
"""

from __future__ import annotations

import json
from pathlib import Path

from prediction_markets_nautilus.backtest import run_backtest_proof


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="prediction_markets_nautilus.backtest")
    parser.add_argument("--catalog-directory", required=True)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    result = run_backtest_proof(args.catalog_directory)
    report = {"backtestProof": result.to_dict()}
    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
