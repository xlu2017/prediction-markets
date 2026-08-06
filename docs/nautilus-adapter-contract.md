# Nautilus adapter contract

Phase 2 deliverable. This document defines the versioned intermediate contract between schema-v2
recordings and NautilusTrader. The Nautilus Python classes are **not** the only persistent
representation: an internal normalized format carries every record and its source-envelope lineage,
so any Nautilus event can be mapped back to the exact schema-v2 source envelope.

Contract version: `1`.

## Goals

1. Preserve exact source-envelope traceability for every normalized record.
2. Preserve decimal values without binary floating-point distortion.
3. Define field-by-field mappings to Nautilus domain events.
4. Define event ordering, duplicate handling, gap handling, and unsupported-event handling.
5. Keep audit-only events (empty frames, pings, deduplicated copies, unsupported source events) in a
   sidecar audit stream rather than discarding them.

## Source-envelope lineage (required on every normalized record)

Every normalized record carries a `Lineage` block:

```text
runId              string         schema-v2 run id
sequence           int            one-based local order from events.jsonl
frameId            string|null    local per-frame id (e.g. frame-1)
messageIndex       int|null       zero-based index within a batched frame
messageCount       int|null       total messages in the batched frame
connectionGeneration int|null     one-based socket attempt number
marketId           string         local market identifier
conditionId        string|null    Polymarket CLOB condition id
tokenId            string|null    single token affected
rawEventType       string         schema-v2 envelope eventType
sourceTimestamp    int|null       upstream Unix ms when supplied
receivedAt         int            local wall-clock Unix ms at frame callback
processedAt        int            local wall-clock Unix ms before envelope build
```

`Lineage` is immutable and serialized with every normalized record and every Nautilus-derived
catalog row. A normalized record without complete lineage for a required field is a conversion
error, not a silent drop.

## Normalized intermediate format

The intermediate format is a set of Python dataclasses in `prediction_markets_nautilus.models`. They
are the persistent representation written to the catalog sidecar; Nautilus objects are constructed
from them at replay time.

```python
NormalizedInstrument       # one per outcome token
NormalizedBookSnapshot     # full CLOB book snapshot
NormalizedBookDelta        # incremental price_change update
NormalizedTrade            # valid trade / last-trade event
NormalizedReferencePrice   # Chainlink BTC/USD reference price
NormalizedLifecycleEvent   # connection-generation and lifecycle boundaries
NormalizedAuditEvent       # audit-only stream (empty frames, pings, dedup, unsupported)
```

Each carries a `Lineage` and a typed payload. Decimal price/quantity fields are kept as `str`
through the intermediate format and only converted to Nautilus `Price`/`Quantity` at the boundary,
using string parsing (`Price.from_str`), never via `float`.

### NormalizedInstrument

```text
instrumentId       str          "POLYMARKET-{conditionId}-{tokenId}" (Nautilus InstrumentId venue POLYMARKET)
conditionId        str
tokenId            str
outcome            "UP" | "DOWN"
slug               str
question           str
opensAt            ISO-8601 UTC
closesAt           ISO-8601 UTC
tickSize           str          decimal lexeme (may be null in source)
minOrderSize       str|null     not synthesized if absent
collateralCurrency "pUSD"
rawGammaText       str          verbatim Gamma response text
```

The Up and Down outcome tokens are modeled as **two distinct `BinaryOption` instruments**, each with
its own `InstrumentId` derived from `conditionId` + `tokenId`. This matches the Nautilus Polymarket
adapter convention and keeps each token's book independent. The shared `conditionId` links them as
components of one prediction market; the `outcome` field records which side each instrument
represents.

### NormalizedBookSnapshot

```text
instrumentId       str
bids               list[(price:str, size:str)]   price ascending toward best
asks               list[(price:str, size:str)]   price ascending away from best
sourceTimestamp    int|null
```

### NormalizedBookDelta

```text
instrumentId       str
side               "BUY" | "SELL"
price              str
size               str          "0" indicates a level to clear
sourceTimestamp    int|null
```

A `price_change` payload may carry multiple levels per token; each level becomes one
`NormalizedBookDelta` sharing the same lineage.

### NormalizedTrade

```text
instrumentId       str
price              str
size               str
aggressorSide      "BUY" | "SELL" | "UNKNOWN"
sourceTimestamp    int|null
```

### NormalizedReferencePrice

```text
symbol             "btc/usd"
topic              "crypto_prices_chainlink"
value              str          exact source decimal lexeme (e.g. "67234.5000000000000001")
topLevelSourceTimestamp  int|null
payloadSourceTimestamp    int|null
sourceTimestamp    int|null    canonical = payloadSourceTimestamp if present else topLevel
```

The Chainlink reference price is **never** represented as a Polymarket trade, an outcome-token
quote, an exchange mark price, or a fill. It is a custom Nautilus data type (see Phase 6).

### NormalizedLifecycleEvent

```text
kind               "CONNECTION_GENERATION" | "RESYNC_BOUNDARY" | "MARKET_DISCOVERED"
generation         int|null
detail             str|null
```

### NormalizedAuditEvent

```text
kind               "EMPTY_FRAME" | "IGNORED_FOREIGN_MARKET" | "PING" | "PONG"
                   | "DEDUPLICATED_COPY" | "UNSUPPORTED_SOURCE_EVENT" | "RAW_LIFECYCLE_DIAGNOSTIC"
originalEventType  str
originalPayload    JsonValue     verbatim normalized payload
rawText            str|null
rawMessageText     str|null
```

## Field-by-field mapping to Nautilus

| normalized field           | Nautilus target                         | rule                                                                                                                                                                                                 |
| -------------------------- | --------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `NormalizedInstrument`     | `BinaryOption`                          | `InstrumentId(Symbol(tokenId), Venue("POLYMARKET"))`; `Price.from_str(tickSize)`; collateral `pUSD`; `ts_init` from `opensAt`. `min_quantity` left unset when source has no minimum (do not invent). |
| `NormalizedBookSnapshot`   | `OrderBookSnapshot`                     | `InstrumentId` per token; `BookLevel` list from bids/asks; `Price.from_str`/`Quantity.from_str`; `ts_event` = replay clock (see below), `ts_init` = `processedAt`.                                   |
| `NormalizedBookDelta`      | `OrderBookDelta`                        | `OrderSide.BUY`/`SELL`; `BookOrder(Price.from_str, Quantity.from_str)`; size `"0"` → `OrderBookDeltaOperation.CLEAR`; `ts_event`/`ts_init` as above.                                                 |
| `NormalizedTrade`          | `TradeTick`                             | `Price.from_str`/`Quantity.from_str`; `AggregationSide` from `aggressorSide`; `ts_event`/`ts_init`.                                                                                                  |
| `NormalizedReferencePrice` | custom `ReferencePriceUpdate` data type | custom Nautilus `Data` object (Phase 6); not a `TradeTick`/`QuoteTick`.                                                                                                                              |
| `NormalizedLifecycleEvent` | custom `LifecycleEvent` data type       | audit/catalog sidecar; not fed to the trading engine.                                                                                                                                                |
| `NormalizedAuditEvent`     | audit sidecar catalog only              | never fed to the trading engine.                                                                                                                                                                     |

## Timestamp semantics

- `ts_event` (the event's view of when it occurred) = **monotonic replay clock** based on
  `receivedAt` (in ns), enforced as `max(receivedAt_ns + childIndex, previousReplayTime + 1)`. This
  preserves actual elapsed time between observations, feed-gap durations, staleness intervals, and
  time remaining until market close, while still enforcing strict monotonic ordering when wall-clock
  timestamps regress or tie. Child records within one envelope get nanosecond offsets. Source,
  receipt, and processing timestamps are retained separately in the `Lineage` sidecar.
- `ts_init` (when the system initialized processing) = same replay clock as `ts_event`, ensuring the
  Parquet catalog partitions and filters consistently. The original `processedAt` is preserved in
  the `Lineage` block.
- Source timestamps (`sourceTimestamp`, `topLevelSourceTimestamp`, `payloadSourceTimestamp`),
  `receivedAt`, and `processedAt` are all preserved in the `Lineage` block for audit and analysis.
- Negative source-to-receive latencies are preserved by storing both `sourceTimestamp` and
  `receivedAt` in lineage; they are not clamped.

## Decimal conversion rules

1. Decimal price/quantity fields are kept as `str` end-to-end in the intermediate format.
2. Conversion to Nautilus `Price`/`Quantity` uses `Price.from_str(s)` / `Quantity.from_str(s)`,
   which parse the string directly without passing through `float`.
3. Values that cannot be represented exactly as binary floats (e.g. `67234.5000000000000001`) are
   preserved as their original string lexeme in the audit sidecar and in the reference-price custom
   type's `value` field.
4. The reader validates decimals as strings before any Nautilus construction.
5. Nautilus `Price`/`Quantity` are internally fixed-point (integer mantissa + precision); the string
   parse preserves the source precision up to the instrument's `price_precision`/ `size_precision`.
   Precision loss, if any, is recorded in the conversion report.

## Event-ordering rules

1. Events are processed in strictly increasing `sequence` order from `events.jsonl`.
2. The reader rejects non-contiguous sequences (gap) before conversion.
3. Within a single batched frame (`messageCount > 1`), messages are processed in `messageIndex`
   order.
4. Snapshots reset the book atomically; subsequent deltas apply on top.
5. A snapshot for one token does not initialize the other token's book; each token book is
   independent (one outcome book may be initialized before the other).

## Duplicate handling

- `dedupeState == DUPLICATE_APPLIED`: the message was applied as a duplicate. It is converted to a
  `NormalizedAuditEvent` of kind `DEDUPLICATED_COPY` and **not** fed to the trading engine, because
  the unique copy already produced the book effect.
- `dedupeState == RECEIVED_NOT_APPLIED`: converted to `NormalizedAuditEvent` kind
  `DEDUPLICATED_COPY`; not fed to the engine.
- `dedupeState == UNIQUE_APPLIED`: converted normally.
- `duplicateOfSequence` is retained in lineage for traceability.

## Gap handling

- The reader requires `captureStatus == COMPLETE` and `usableForReplay == true`, which implies no
  open or closed gaps. A run with gaps is rejected before conversion.
- If a non-contiguous sequence is detected in `events.jsonl` despite a `COMPLETE` status, the reader
  raises a `GapError` identifying the missing sequence; conversion aborts.
- Resynchronization boundaries (`BookState == RESYNC_REQUIRED`, generation changes) are recorded as
  `NormalizedLifecycleEvent` of kind `RESYNC_BOUNDARY` and in the audit stream; they do not by
  themselves abort a `COMPLETE` run, but a snapshot must follow to reseed the book.

## Unsupported-event handling

- Any schema-v2 event whose `eventType`/`source` combination is not in the supported map is
  converted to a `NormalizedAuditEvent` of kind `UNSUPPORTED_SOURCE_EVENT`.
- It is counted in the conversion report under `ignoredEventCount` and identified by
  `source:eventType`.
- It is never silently dropped; its lineage and raw payload are retained in the audit sidecar.

## Audit lineage requirements

- Every Nautilus event constructed from a normalized record carries the `Lineage` fields in a
  sidecar table keyed by `(runId, sequence)`.
- The catalog stores a `lineage.parquet` sidecar alongside the data catalog, mapping each Nautilus
  event row to its source `sequence` and envelope fields.
- A round-trip test (Phase 10) proves every normalized event can be traced back to its source
  envelope `sequence`.
- The conversion report (Phase 7) emits `firstSourceSequence` and `lastSourceSequence` and requires
  them to bound every converted record.

## Supported event map

| source                       | eventType                    | normalized target                                                                          | engine-fed        |
| ---------------------------- | ---------------------------- | ------------------------------------------------------------------------------------------ | ----------------- |
| `POLYMARKET_MARKET`          | `MARKET_DISCOVERED`          | `NormalizedLifecycleEvent` (MARKET_DISCOVERED) + `NormalizedInstrument` from `market.json` | instrument only   |
| `POLYMARKET_CLOB`            | `book`                       | `NormalizedBookSnapshot`                                                                   | yes               |
| `POLYMARKET_CLOB`            | `price_change`               | `NormalizedBookDelta` (one per level)                                                      | yes               |
| `POLYMARKET_CLOB`            | `last_trade_price` / `trade` | `NormalizedTrade`                                                                          | yes               |
| `POLYMARKET_REFERENCE_PRICE` | `REFERENCE_PRICE`            | `NormalizedReferencePrice`                                                                 | yes (custom type) |
| `POLYMARKET_CLOB`            | `EMPTY_FRAME`                | `NormalizedAuditEvent`                                                                     | no                |
| `POLYMARKET_CLOB`            | `IGNORED_FOREIGN_MARKET`     | `NormalizedAuditEvent`                                                                     | no                |
| any                          | `PING` / `PONG`              | `NormalizedAuditEvent`                                                                     | no                |
| any                          | unsupported                  | `NormalizedAuditEvent`                                                                     | no                |

## Versioning

This contract is versioned independently of schema-v2 and Nautilus. Contract version `1` targets
schema-v2 and NautilusTrader `1.231.0`. Changes to field meaning require a new contract version.
