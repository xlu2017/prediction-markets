"""Nautilus BacktestNode proof: runs an actual backtest engine with a strategy.

This module proves that NautilusTrader's BacktestNode can:
- load data from the converted Parquet catalog
- feed order-book deltas and custom reference-price data to a strategy
- simulate order submission, matching, and fills
- maintain portfolio/account state

The strategy is deliberately trivial: it places a single limit buy order at a
price below the best ask, waits for the book to cross it, and records the fill.
No real execution client is connected — this is a simulated backtest.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price
from nautilus_trader.trading.strategy import Strategy

from .models import ReferencePriceUpdate

_MS_TO_NS = 1_000_000


@dataclass
class BacktestProofResult:
    """Result of the BacktestNode proof run."""

    ran: bool
    book_events_received: int = 0
    reference_events_received: int = 0
    orders_submitted: int = 0
    fills_count: int = 0
    account_balances: list[str] = field(default_factory=list)
    positions: list[str] = field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "ran": self.ran,
            "bookEventsReceived": self.book_events_received,
            "referenceEventsReceived": self.reference_events_received,
            "ordersSubmitted": self.orders_submitted,
            "fillsCount": self.fills_count,
            "accountBalances": self.account_balances,
            "positions": self.positions,
            "error": self.error,
        }


class ProofStrategyConfig(StrategyConfig, frozen=True):
    instrument_id: str = ""


class ProofStrategy(Strategy):
    """Trivial strategy that receives book + reference events and places one limit order.

    Uses class-level state to track events received (the BacktestNode instantiates
    strategies via importable paths, so the strategy class must be at module level).
    """

    book_events: int = 0
    reference_events: int = 0
    orders_submitted: int = 0
    _order_submitted: bool = False
    _final_account_balances: list[str] = []
    _final_positions: list[str] = []
    _final_fills_count: int = 0

    def on_start(self) -> None:
        from nautilus_trader.model.data import DataType

        iid_str = self.config.instrument_id
        iid = InstrumentId.from_str(iid_str) if iid_str else None
        if iid is not None:
            self.instrument = self.cache.instrument(iid)
        else:
            self.instrument = None
        if self.instrument is None:
            self.log.error("Instrument not found")
            self.stop()
            return
        self.subscribe_order_book_deltas(instrument_id=self.instrument.id)
        self.subscribe_data(
            DataType(ReferencePriceUpdate, {"topic": "crypto_prices_chainlink"}),
            instrument_id=self.instrument.id,
        )

    def on_order_book_deltas(self, deltas) -> None:
        type(self).book_events += 1
        if not type(self)._order_submitted and self.instrument is not None:
            try:
                order = self.order_factory.limit(
                    instrument_id=self.instrument.id,
                    order_side=OrderSide.BUY,
                    quantity=self.instrument.make_qty(Decimal("10")),
                    price=Price.from_str("0.45"),
                    time_in_force=TimeInForce.GTC,
                )
                self.submit_order(order)
                type(self)._order_submitted = True
                type(self).orders_submitted += 1
            except Exception as e:
                self.log.warning(f"Order submission failed: {e}")

    def on_data(self, data) -> None:
        if isinstance(data, ReferencePriceUpdate):
            type(self).reference_events += 1

    def on_stop(self) -> None:
        """Capture final portfolio state before the engine disposes."""
        type(self)._final_account_balances = []
        type(self)._final_positions = []
        type(self)._final_fills_count = 0
        try:
            from nautilus_trader.model.identifiers import Venue

            venue = Venue(str(self.instrument.id.venue))
            account = self.cache.account_for_venue(venue)
            if account is not None:
                type(self)._final_account_balances.append(repr(account))
                for currency in account.balances():
                    bal = account.balance(currency)
                    if bal is not None:
                        type(self)._final_account_balances.append(repr(bal))
            for order in self.cache.orders():
                if order.is_filled:
                    type(self)._final_fills_count += 1
            for pos in self.cache.positions():
                type(self)._final_positions.append(str(pos))
        except Exception:
            pass


def run_backtest_proof(catalog_directory: str | Path) -> BacktestProofResult:
    """Run a Nautilus BacktestNode against the converted catalog.

    Returns a BacktestProofResult with evidence that the engine ran, received
    book and reference events, and maintained portfolio state.
    """
    cat_dir = Path(catalog_directory)

    try:
        from nautilus_trader.backtest.node import (
            BacktestDataConfig,
            BacktestEngineConfig,
            BacktestNode,
            BacktestRunConfig,
            BacktestVenueConfig,
        )
        from nautilus_trader.config import ImportableStrategyConfig
        from nautilus_trader.model.enums import AccountType, OmsType
        from nautilus_trader.persistence.catalog import ParquetDataCatalog
    except ImportError as e:
        return BacktestProofResult(ran=False, error=f"Nautilus import failed: {e}")

    cat = ParquetDataCatalog(str(cat_dir))
    instruments = cat.instruments()
    if not instruments:
        return BacktestProofResult(ran=False, error="no instruments in catalog")

    up_instr = next((i for i in instruments if i.outcome == "UP"), instruments[0])

    # Determine time range from the catalog data.
    all_deltas = [d for d in cat.order_book_deltas() if d.instrument_id == up_instr.id]
    if not all_deltas:
        return BacktestProofResult(ran=False, error="no order book deltas in catalog")
    start_ns = min(d.ts_event for d in all_deltas)
    end_ns = max(d.ts_event for d in all_deltas)
    end_ns += 60 * 1_000_000_000  # +60s buffer

    # Reset class-level state.
    ProofStrategy.book_events = 0
    ProofStrategy.reference_events = 0
    ProofStrategy.orders_submitted = 0
    ProofStrategy._order_submitted = False
    ProofStrategy._final_account_balances = []
    ProofStrategy._final_positions = []
    ProofStrategy._final_fills_count = 0
    ProofStrategy._instrument_id = str(up_instr.id)

    # Build the backtest configuration.
    venue_config = BacktestVenueConfig(
        name=str(up_instr.id.venue),
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,
        book_type="L2_MBP",
        starting_balances=["1_000_000 USDC"],
    )

    data_configs = [
        BacktestDataConfig(
            catalog_path=str(cat_dir),
            data_cls="nautilus_trader.model.data:OrderBookDelta",
            instrument_id=up_instr.id,
            start_time=start_ns,
            end_time=end_ns,
        ),
        BacktestDataConfig(
            catalog_path=str(cat_dir),
            data_cls="prediction_markets_nautilus.models:ReferencePriceUpdate",
            instrument_id=up_instr.id,
            start_time=start_ns,
            end_time=end_ns,
        ),
    ]

    strategy_config = ImportableStrategyConfig(
        strategy_path="prediction_markets_nautilus.backtest:ProofStrategy",
        config_path="prediction_markets_nautilus.backtest:ProofStrategyConfig",
        config={"instrument_id": str(up_instr.id)},
    )

    engine_config = BacktestEngineConfig(
        strategies=[strategy_config],
        run_analysis=False,
    )

    run_config = BacktestRunConfig(
        engine=engine_config,
        data=data_configs,
        venues=[venue_config],
    )

    try:
        node = BacktestNode(configs=[run_config])
    except Exception as e:
        return BacktestProofResult(ran=False, error=f"BacktestNode creation failed: {e}")

    try:
        results = node.run()
    except Exception as e:
        return BacktestProofResult(
            ran=False,
            book_events_received=ProofStrategy.book_events,
            reference_events_received=ProofStrategy.reference_events,
            error=f"BacktestNode.run() failed: {e}",
        )

    # Extract portfolio/account state.
    fills_count = 0
    account_balances: list[str] = []
    positions: list[str] = []

    if results:
        fills_count = ProofStrategy._final_fills_count
        account_balances = list(ProofStrategy._final_account_balances)
        positions = list(ProofStrategy._final_positions)

    return BacktestProofResult(
        ran=True,
        book_events_received=ProofStrategy.book_events,
        reference_events_received=ProofStrategy.reference_events,
        orders_submitted=ProofStrategy.orders_submitted,
        fills_count=fills_count,
        account_balances=account_balances,
        positions=positions,
    )


def _main() -> int:
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
    raise SystemExit(_main())
