"""Nautilus BacktestNode proof: runs an actual backtest engine with a strategy.

This module proves that NautilusTrader's BacktestNode can:
- load data from the converted Parquet catalog
- feed order-book deltas and custom reference-price data to a strategy
- deliver events in recorder order (sourceSequence, childIndex) even when
  source timestamps regress or are equal
- simulate order submission, matching, and fills
- maintain portfolio/account state

The strategy places a crossing limit buy order (at the best ask price) to
prove fill simulation and portfolio accounting. It also records the
``sequence`` field of each received delta to prove recorder-order delivery.
"""

from __future__ import annotations

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
    fill_details: list[str] = field(default_factory=list)
    account_balances: list[str] = field(default_factory=list)
    positions: list[str] = field(default_factory=list)
    received_sequences: list[int] = field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "ran": self.ran,
            "bookEventsReceived": self.book_events_received,
            "referenceEventsReceived": self.reference_events_received,
            "ordersSubmitted": self.orders_submitted,
            "fillsCount": self.fills_count,
            "fillDetails": self.fill_details,
            "accountBalances": self.account_balances,
            "positions": self.positions,
            "receivedSequences": self.received_sequences,
            "error": self.error,
        }


class ProofStrategyConfig(StrategyConfig, frozen=True):
    instrument_id: str = ""
    cross_price: str = ""


class ProofStrategy(Strategy):
    """Strategy that receives book + reference events and places a crossing limit order.

    Records the ``sequence`` field of each received delta to prove recorder-order
    delivery. Places a limit buy at the best ask price to force a fill.

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
    _received_sequences: list[int] = []
    _fill_details: list[str] = []

    def on_start(self) -> None:
        from nautilus_trader.model.data import DataType
        from nautilus_trader.model.identifiers import ClientId

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
        # Subscribe to Chainlink reference data through the Nautilus event engine.
        # The BacktestNode loads it via BacktestDataConfig with client_id="CHAINLINK",
        # the DataEngine delivers it, and on_data() receives it — proving the full
        # CLOB + Chainlink joint delivery path.
        self.subscribe_data(
            DataType(ReferencePriceUpdate),
            client_id=ClientId("CHAINLINK"),
        )

    def on_order_book_deltas(self, deltas) -> None:
        type(self).book_events += 1
        # Record the sequence field (sourceSequence * 1_000_000 + childIndex) to
        # prove the engine delivers events in recorder order.
        try:
            delta_list = list(deltas.deltas) if hasattr(deltas, "deltas") else [deltas]
        except Exception:
            delta_list = []
        for d in delta_list:
            type(self)._received_sequences.append(d.sequence)
        if not type(self)._order_submitted and self.instrument is not None:
            # After the snapshot deltas are applied, submit a crossing limit order
            # at the best ask price to force a fill.
            cross_price_str = self.config.cross_price or "0.52"
            try:
                order = self.order_factory.limit(
                    instrument_id=self.instrument.id,
                    order_side=OrderSide.BUY,
                    quantity=self.instrument.make_qty(Decimal("10")),
                    price=Price.from_str(cross_price_str),
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

    def on_order_filled(self, event) -> None:
        type(self)._final_fills_count += 1
        type(self)._fill_details.append(
            f"filled: side={event.order_side} qty={event.last_qty} px={event.last_px}"
        )
        # Capture account balance immediately after the fill.
        try:
            from nautilus_trader.model.identifiers import Venue

            venue = Venue(str(self.instrument.id.venue))
            account = self.cache.account_for_venue(venue)
            if account is not None:
                type(self)._final_account_balances = [repr(account)]
                for currency in account.balances():
                    bal = account.balance(currency)
                    if bal is not None:
                        type(self)._final_account_balances.append(repr(bal))
            for pos in self.cache.positions():
                type(self)._final_positions.append(str(pos))
        except Exception:
            pass

    def on_stop(self) -> None:
        """Capture final portfolio state before the engine disposes."""
        try:
            from nautilus_trader.model.identifiers import Venue

            venue = Venue(str(self.instrument.id.venue))
            account = self.cache.account_for_venue(venue)
            if account is not None and not type(self)._final_account_balances:
                type(self)._final_account_balances.append(repr(account))
                for currency in account.balances():
                    bal = account.balance(currency)
                    if bal is not None:
                        type(self)._final_account_balances.append(repr(bal))
            if not type(self)._final_positions:
                for pos in self.cache.positions():
                    type(self)._final_positions.append(str(pos))
        except Exception:
            pass


def run_backtest_proof(catalog_directory: str | Path) -> BacktestProofResult:
    """Run a Nautilus BacktestNode against the converted catalog.

    Returns a BacktestProofResult with evidence that the engine ran, received
    book and reference events, delivered events in recorder order, simulated
    a fill, and maintained portfolio state.
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

    # Determine the best ask from the first snapshot to place a crossing order.
    # The first snapshot for the UP token has asks at some price; we buy at that
    # price to guarantee a fill.
    cross_price = "0.52"
    for d in all_deltas:
        if d.order is not None and str(d.order.side) == "SELL":
            cross_price = str(d.order.price)
            break

    # Reset class-level state.
    ProofStrategy.book_events = 0
    ProofStrategy.reference_events = 0
    ProofStrategy.orders_submitted = 0
    ProofStrategy._order_submitted = False
    ProofStrategy._final_account_balances = []
    ProofStrategy._final_positions = []
    ProofStrategy._final_fills_count = 0
    ProofStrategy._received_sequences = []
    ProofStrategy._fill_details = []

    # Build the backtest configuration.
    venue_config = BacktestVenueConfig(
        name=str(up_instr.id.venue),
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,
        book_type="L2_MBP",
        starting_balances=["1_000_000 pUSD"],
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
            client_id="CHAINLINK",
            start_time=start_ns,
            end_time=end_ns,
        ),
    ]

    strategy_config = ImportableStrategyConfig(
        strategy_path="prediction_markets_nautilus.backtest:ProofStrategy",
        config_path="prediction_markets_nautilus.backtest:ProofStrategyConfig",
        config={
            "instrument_id": str(up_instr.id),
            "cross_price": cross_price,
        },
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
    received_sequences: list[int] = []

    if results:
        fills_count = ProofStrategy._final_fills_count
        account_balances = list(ProofStrategy._final_account_balances)
        positions = list(ProofStrategy._final_positions)
        received_sequences = list(ProofStrategy._received_sequences)
        fill_details = list(ProofStrategy._fill_details)

    return BacktestProofResult(
        ran=True,
        book_events_received=ProofStrategy.book_events,
        reference_events_received=ProofStrategy.reference_events,
        orders_submitted=ProofStrategy.orders_submitted,
        fills_count=fills_count,
        fill_details=fill_details,
        account_balances=account_balances,
        positions=positions,
        received_sequences=received_sequences,
    )
