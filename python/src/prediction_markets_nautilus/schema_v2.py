"""Schema-v2 contract mirror and validation.

The TypeScript recorder's Zod schemas (``src/types.ts``) are the single source of truth for the
schema-v2 on-disk contract. The recorder does not currently emit a JSON Schema artifact, so this
module mirrors the field contracts required for replay and validates them with strict,
decimal-preserving rules.

Schema source-of-truth approach
-------------------------------
Three cross-language schema mechanisms were considered:

1. **JSON Schema generated from Zod** (``zod-to-json-schema``): would give a single source of
   truth, but the recorder does not emit a JSON Schema artifact today and adding that generation
   to the TypeScript recorder is out of scope for this read-only spike.
2. **Pydantic models**: idiomatic Python, but risks coercing decimal strings through binary
   floats if field types are not strictly ``StrictStr``.
3. **Manual mirror with strict validation**: chosen. The envelope is simple, and we need exact
   control over decimal preservation (prices/quantities stay ``str`` until Nautilus boundary).

The selected approach mirrors the TypeScript field contracts here and validates against the
committed sample fixture plus synthetic fixtures in tests. If the recorder later emits a JSON
Schema artifact, this module can be regenerated from it without changing the public API.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

SCHEMA_VERSION = 2

EVENT_SOURCES = ("POLYMARKET_CLOB", "POLYMARKET_MARKET", "POLYMARKET_REFERENCE_PRICE")
DEDUPE_STATES = ("UNIQUE_APPLIED", "DUPLICATE_APPLIED", "RECEIVED_NOT_APPLIED")
COMPLETION_STATUSES = ("COMPLETED", "FAILED", "RUNNING")
CAPTURE_STATUSES = ("COMPLETE", "PARTIAL", "UNUSABLE")


class SchemaError(ValueError):
    """Raised when a schema-v2 artifact fails validation."""


class GapError(SchemaError):
    """Raised when event sequence numbers are non-contiguous."""


@dataclass(frozen=True)
class MarketSpecification:
    eventId: str
    marketId: str
    conditionId: str
    slug: str
    question: str
    opensAt: str
    closesAt: str
    resolutionSource: str
    referenceSymbol: str
    referenceTopic: str
    upTokenId: str
    downTokenId: str
    tickSize: str | None
    active: bool
    closed: bool
    rawMetadata: Any
    rawGammaText: str


@dataclass(frozen=True)
class RawEventEnvelope:
    """A single schema-v2 event line, with decimal-bearing payload left as parsed JSON.

    The recorder normalizes decimal price/quantity fields to strings in ``payload``, so a
    standard JSON parse preserves them as ``str`` (never ``float``).
    """

    runId: str
    sequence: int
    source: str
    eventType: str
    marketId: str | None
    conditionId: str | None
    sourceMarketId: str | None
    tokenId: str | None
    observedTokenIds: list[str] | None
    sourceTimestamp: int | None
    topLevelSourceTimestamp: int | None
    payloadSourceTimestamp: int | None
    receivedAt: int
    processedAt: int
    connectionGeneration: int | None
    frameId: str | None
    messageIndex: int | None
    messageCount: int | None
    isDuplicate: bool
    dedupeState: str
    duplicateOfSequence: int | None
    dedupeReason: str | None
    payload: Any
    rawText: str | None
    rawMessageText: str | None


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise SchemaError(msg)


def _is_str(v: Any) -> bool:
    return isinstance(v, str)


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _is_opt_int(v: Any) -> bool:
    return v is None or (_is_int(v))


def _is_opt_str(v: Any) -> bool:
    return v is None or _is_str(v)


def validate_market(raw: Any) -> MarketSpecification:
    _require(isinstance(raw, dict), "market is not an object")
    _require(raw.get("schemaVersion") == SCHEMA_VERSION, "market.schemaVersion is not 2")
    m = raw.get("market", raw)
    _require(isinstance(m, dict), "market.market is not an object")
    _require(_is_str(m.get("eventId")), "market.eventId is not a string")
    _require(_is_str(m.get("marketId")), "market.marketId is not a string")
    _require(_is_str(m.get("conditionId")), "market.conditionId is not a string")
    _require(_is_str(m.get("slug")), "market.slug is not a string")
    _require(_is_str(m.get("upTokenId")), "market.upTokenId is not a string")
    _require(_is_str(m.get("downTokenId")), "market.downTokenId is not a string")
    _require(_is_str(m.get("referenceSymbol")), "market.referenceSymbol is not a string")
    _require(_is_str(m.get("referenceTopic")), "market.referenceTopic is not a string")
    _require(_is_str(m.get("opensAt")), "market.opensAt is not a string")
    _require(_is_str(m.get("closesAt")), "market.closesAt is not a string")
    _require(
        m.get("tickSize") is None or _is_str(m.get("tickSize")),
        "market.tickSize is not a string or null",
    )
    _require(isinstance(m.get("active"), bool), "market.active is not a boolean")
    _require(isinstance(m.get("closed"), bool), "market.closed is not a boolean")
    _require(_is_str(m.get("rawGammaText")), "market.rawGammaText is not a string")
    return MarketSpecification(
        eventId=m["eventId"],
        marketId=m["marketId"],
        conditionId=m["conditionId"],
        slug=m["slug"],
        question=m.get("question", ""),
        opensAt=m["opensAt"],
        closesAt=m["closesAt"],
        resolutionSource=m.get("resolutionSource", ""),
        referenceSymbol=m["referenceSymbol"],
        referenceTopic=m["referenceTopic"],
        upTokenId=m["upTokenId"],
        downTokenId=m["downTokenId"],
        tickSize=m.get("tickSize"),
        active=m["active"],
        closed=m["closed"],
        rawMetadata=m.get("rawMetadata"),
        rawGammaText=m["rawGammaText"],
    )


def validate_manifest(raw: Any) -> dict:
    _require(isinstance(raw, dict), "manifest is not an object")
    _require(raw.get("schemaVersion") == SCHEMA_VERSION, "manifest.schemaVersion is not 2")
    _require(_is_str(raw.get("runId")), "manifest.runId is not a string")
    _require(_is_str(raw.get("startedAt")), "manifest.startedAt is not a string")
    status = raw.get("completionStatus")
    _require(status in COMPLETION_STATUSES, f"manifest.completionStatus invalid: {status!r}")
    cap = raw.get("captureStatus")
    _require(cap in CAPTURE_STATUSES, f"manifest.captureStatus invalid: {cap!r}")
    _require(
        isinstance(raw.get("usableForReplay"), bool), "manifest.usableForReplay is not a boolean"
    )
    _require(
        raw.get("finalEventCount") is None or _is_int(raw.get("finalEventCount")),
        "manifest.finalEventCount invalid",
    )
    market = validate_market(raw.get("market"))
    return {
        "runId": raw["runId"],
        "startedAt": raw["startedAt"],
        "endedAt": raw.get("endedAt"),
        "completionStatus": status,
        "captureStatus": cap,
        "usableForReplay": raw["usableForReplay"],
        "finalEventCount": raw.get("finalEventCount"),
        "market": market,
        "clobStaleAfterMs": raw.get("clobStaleAfterMs"),
        "referenceStaleAfterMs": raw.get("referenceStaleAfterMs"),
    }


def validate_health(raw: Any) -> dict:
    _require(isinstance(raw, dict), "health is not an object")
    _require(raw.get("schemaVersion") == SCHEMA_VERSION, "health.schemaVersion is not 2")
    _require(_is_str(raw.get("runId")), "health.runId is not a string")
    _require(raw.get("captureStatus") in CAPTURE_STATUSES, "health.captureStatus invalid")
    _require(
        isinstance(raw.get("usableForReplay"), bool), "health.usableForReplay is not a boolean"
    )
    fbh = raw.get("finalBookHealth", {})
    _require(isinstance(fbh, dict), "health.finalBookHealth is not an object")
    for tid, book in fbh.items():
        _require(isinstance(book, dict), f"finalBookHealth.{tid} is not an object")
        _require(_is_str(book.get("tokenId")), f"finalBookHealth.{tid}.tokenId is not a string")
        _require(
            book.get("bestBid") is None or _is_str(book.get("bestBid")),
            f"finalBookHealth.{tid}.bestBid invalid",
        )
        _require(
            book.get("bestAsk") is None or _is_str(book.get("bestAsk")),
            f"finalBookHealth.{tid}.bestAsk invalid",
        )
        _require(_is_int(book.get("bidDepthCount")), f"finalBookHealth.{tid}.bidDepthCount invalid")
        _require(_is_int(book.get("askDepthCount")), f"finalBookHealth.{tid}.askDepthCount invalid")
    return raw


def validate_envelope(raw: Any, expected_run_id: str) -> RawEventEnvelope:
    _require(isinstance(raw, dict), "envelope is not an object")
    _require(raw.get("schemaVersion") == SCHEMA_VERSION, "envelope.schemaVersion is not 2")
    _require(_is_str(raw.get("runId")), "envelope.runId is not a string")
    _require(
        raw["runId"] == expected_run_id,
        f"envelope.runId {raw.get('runId')!r} != {expected_run_id!r}",
    )
    _require(
        _is_int(raw.get("sequence")) and raw["sequence"] >= 1,
        "envelope.sequence is not a positive int",
    )
    _require(raw.get("source") in EVENT_SOURCES, f"envelope.source invalid: {raw.get('source')!r}")
    _require(_is_str(raw.get("eventType")), "envelope.eventType is not a string")
    _require(_is_opt_str(raw.get("marketId")), "envelope.marketId invalid")
    _require(_is_opt_str(raw.get("conditionId")), "envelope.conditionId invalid")
    _require(_is_opt_str(raw.get("sourceMarketId")), "envelope.sourceMarketId invalid")
    _require(_is_opt_str(raw.get("tokenId")), "envelope.tokenId invalid")
    oti = raw.get("observedTokenIds")
    _require(
        oti is None or (isinstance(oti, list) and all(_is_str(x) for x in oti)),
        "envelope.observedTokenIds invalid",
    )
    _require(_is_opt_int(raw.get("sourceTimestamp")), "envelope.sourceTimestamp invalid")
    _require(
        _is_opt_int(raw.get("topLevelSourceTimestamp")), "envelope.topLevelSourceTimestamp invalid"
    )
    _require(
        _is_opt_int(raw.get("payloadSourceTimestamp")), "envelope.payloadSourceTimestamp invalid"
    )
    _require(_is_int(raw.get("receivedAt")), "envelope.receivedAt is not an int")
    _require(_is_int(raw.get("processedAt")), "envelope.processedAt is not an int")
    _require(_is_opt_int(raw.get("connectionGeneration")), "envelope.connectionGeneration invalid")
    _require(_is_opt_str(raw.get("frameId")), "envelope.frameId invalid")
    _require(_is_opt_int(raw.get("messageIndex")), "envelope.messageIndex invalid")
    _require(_is_opt_int(raw.get("messageCount")), "envelope.messageCount invalid")
    _require(isinstance(raw.get("isDuplicate"), bool), "envelope.isDuplicate is not a boolean")
    _require(
        raw.get("dedupeState") in DEDUPE_STATES,
        f"envelope.dedupeState invalid: {raw.get('dedupeState')!r}",
    )
    _require(_is_opt_int(raw.get("duplicateOfSequence")), "envelope.duplicateOfSequence invalid")
    _require(_is_opt_str(raw.get("dedupeReason")), "envelope.dedupeReason invalid")
    _require(raw.get("payload") is not None, "envelope.payload is missing")
    _require(_is_opt_str(raw.get("rawText")), "envelope.rawText invalid")
    _require(_is_opt_str(raw.get("rawMessageText")), "envelope.rawMessageText invalid")
    return RawEventEnvelope(
        runId=raw["runId"],
        sequence=raw["sequence"],
        source=raw["source"],
        eventType=raw["eventType"],
        marketId=raw.get("marketId"),
        conditionId=raw.get("conditionId"),
        sourceMarketId=raw.get("sourceMarketId"),
        tokenId=raw.get("tokenId"),
        observedTokenIds=raw.get("observedTokenIds"),
        sourceTimestamp=raw.get("sourceTimestamp"),
        topLevelSourceTimestamp=raw.get("topLevelSourceTimestamp"),
        payloadSourceTimestamp=raw.get("payloadSourceTimestamp"),
        receivedAt=raw["receivedAt"],
        processedAt=raw["processedAt"],
        connectionGeneration=raw.get("connectionGeneration"),
        frameId=raw.get("frameId"),
        messageIndex=raw.get("messageIndex"),
        messageCount=raw.get("messageCount"),
        isDuplicate=raw["isDuplicate"],
        dedupeState=raw["dedupeState"],
        duplicateOfSequence=raw.get("duplicateOfSequence"),
        dedupeReason=raw.get("dedupeReason"),
        payload=raw["payload"],
        rawText=raw.get("rawText"),
        rawMessageText=raw.get("rawMessageText"),
    )
