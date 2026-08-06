"""Normalized intermediate format and custom Nautilus data types.

Every normalized record carries an immutable :class:`Lineage` block mapping back to the exact
schema-v2 source envelope. Decimal price/quantity fields stay ``str`` until the Nautilus
boundary.

Note: ``from __future__ import annotations`` is intentionally omitted because Nautilus'
``customdataclass`` reads ``__annotations__`` directly and requires real types (not stringized
annotations) to build the Arrow schema.
"""

from dataclasses import dataclass, field
from typing import Any

from nautilus_trader.core.data import Data
from nautilus_trader.model.custom import customdataclass

CONTRACT_VERSION = 1


@dataclass(frozen=True)
class Lineage:
    """Source-envelope lineage for one normalized record.

    ``sourceSequence`` is the schema-v2 envelope sequence (canonical recorder order).
    ``childIndex`` disambiguates multiple catalog rows expanded from one envelope
    (e.g. one snapshot becomes CLEAR + N ADDs, each with a distinct childIndex).
    Replay must order by ``(sourceSequence, childIndex)`` to preserve recorder order.
    """

    runId: str
    sequence: int  # alias for sourceSequence, kept for backward compat
    frameId: str | None
    messageIndex: int | None
    messageCount: int | None
    connectionGeneration: int | None
    marketId: str
    conditionId: str | None
    tokenId: str | None
    rawEventType: str
    sourceTimestamp: int | None
    receivedAt: int
    processedAt: int
    childIndex: int = 0  # disambiguates expanded rows from one envelope

    @property
    def sourceSequence(self) -> int:
        return self.sequence

    def to_dict(self) -> dict:
        return {
            "runId": self.runId,
            "sourceSequence": self.sourceSequence,
            "childIndex": self.childIndex,
            "frameId": self.frameId,
            "messageIndex": self.messageIndex,
            "messageCount": self.messageCount,
            "connectionGeneration": self.connectionGeneration,
            "marketId": self.marketId,
            "conditionId": self.conditionId,
            "tokenId": self.tokenId,
            "rawEventType": self.rawEventType,
            "sourceTimestamp": self.sourceTimestamp,
            "receivedAt": self.receivedAt,
            "processedAt": self.processedAt,
        }


@dataclass(frozen=True)
class NormalizedInstrument:
    lineage: Lineage
    instrumentId: str
    conditionId: str
    tokenId: str
    outcome: str  # "UP" | "DOWN"
    slug: str
    question: str
    opensAt: str
    closesAt: str
    tickSize: str | None
    minOrderSize: str | None
    collateralCurrency: str
    rawGammaText: str


@dataclass(frozen=True)
class Level:
    price: str
    size: str


@dataclass(frozen=True)
class NormalizedBookSnapshot:
    lineage: Lineage
    instrumentId: str
    bids: list[Level]
    asks: list[Level]
    sourceTimestamp: int | None


@dataclass(frozen=True)
class NormalizedBookDelta:
    lineage: Lineage
    instrumentId: str
    side: str  # "BUY" | "SELL"
    price: str
    size: str  # "0" clears the level
    sourceTimestamp: int | None


@dataclass(frozen=True)
class NormalizedTrade:
    lineage: Lineage
    instrumentId: str
    price: str
    size: str
    aggressorSide: str  # "BUY" | "SELL" | "UNKNOWN"
    sourceTimestamp: int | None


@dataclass(frozen=True)
class NormalizedReferencePrice:
    lineage: Lineage
    symbol: str
    topic: str
    value: str  # exact source decimal lexeme
    topLevelSourceTimestamp: int | None
    payloadSourceTimestamp: int | None
    sourceTimestamp: int | None


@dataclass(frozen=True)
class NormalizedLifecycleEvent:
    lineage: Lineage
    kind: str  # "CONNECTION_GENERATION" | "RESYNC_BOUNDARY" | "MARKET_DISCOVERED"
    generation: int | None
    detail: str | None


@dataclass(frozen=True)
class NormalizedAuditEvent:
    lineage: Lineage
    kind: str
    originalEventType: str
    originalPayload: Any
    rawText: str | None
    rawMessageText: str | None


@dataclass
class NormalizedRun:
    """All normalized records derived from one schema-v2 run."""

    runId: str
    instruments: list[NormalizedInstrument] = field(default_factory=list)
    snapshots: list[NormalizedBookSnapshot] = field(default_factory=list)
    deltas: list[NormalizedBookDelta] = field(default_factory=list)
    trades: list[NormalizedTrade] = field(default_factory=list)
    references: list[NormalizedReferencePrice] = field(default_factory=list)
    lifecycle: list[NormalizedLifecycleEvent] = field(default_factory=list)
    audit: list[NormalizedAuditEvent] = field(default_factory=list)


# --- Custom Nautilus data types -------------------------------------------------


@customdataclass
class ReferencePriceUpdate(Data):
    """Chainlink BTC/USD settlement reference price.

    A custom Nautilus data type so reference prices remain semantically distinct from
    Polymarket trades, outcome-token quotes, exchange mark prices, and fills. The exact source
    decimal is preserved as ``value`` (a string).
    """

    symbol: str = ""
    topic: str = ""
    value: str = ""
    topLevelSourceTimestamp: int = 0
    payloadSourceTimestamp: int = 0


@customdataclass
class LifecycleEvent(Data):
    """Connection-generation / resync-boundary lifecycle marker (audit/catalog sidecar)."""

    kind: str = ""
    generation: int = 0
    detail: str = ""
