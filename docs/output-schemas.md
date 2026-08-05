# Output schemas and timing

All schemas are version `2`. Fields may be added compatibly, but changing field meaning requires a
new schema version. Schema v2 adds event identity (`conditionId`, `sourceMarketId`, `frameId`,
`messageIndex`, `messageCount`, `observedTokenIds`), message-level deduplication metadata
(`isDuplicate`, `dedupeState`, `duplicateOfSequence`, `dedupeReason`), transactional book state
(`BookState` with `RESYNC_REQUIRED`), capture-quality status (`captureStatus`, `usableForReplay`,
`qualityFailures`, `openGapsByFeed`), feed-specific staleness thresholds (`clobStaleAfterMs`,
`referenceStaleAfterMs`), Chainlink dual timestamps (`topLevelSourceTimestamp`,
`payloadSourceTimestamp`), per-message raw text (`rawMessageText`), and the raw Gamma response text
(`rawGammaText`).

## `manifest.json`

`RunManifest` contains the run ID, start/end ISO-8601 UTC timestamps, recorder version, Git commit,
validated `MarketSpecification`, safe CLI arguments, output filenames, completion status, final
event count, failure reason, capture status, replay usability flag, quality failures, and
feed-specific staleness thresholds.

The manifest is written as `RUNNING` before sockets start, then atomically replaced with `COMPLETED`
or `FAILED`. A remaining `RUNNING` status indicates interruption before finalization.

## `market.json`

Contains:

```json
{
  "schemaVersion": 2,
  "market": {
    "schemaVersion": 2,
    "eventId": "...",
    "marketId": "...",
    "conditionId": "...",
    "slug": "...",
    "upTokenId": "...",
    "downTokenId": "...",
    "referenceTopic": "crypto_prices_chainlink",
    "referenceSymbol": "btc/usd",
    "rawMetadata": {},
    "rawGammaText": "..."
  }
}
```

The full Gamma response is retained under `rawMetadata` after JSON-safe decimal normalization, and
the original raw response text is preserved verbatim under `rawGammaText` for byte-exact replay.

## `events.jsonl`

Every complete line is one `RawEventEnvelope`:

- `sequence`: local one-based order; strictly increasing without gaps in written output;
- `marketId`: stable local market identifier derived from the Gamma event id;
- `conditionId`: Polymarket CLOB condition id used as the on-wire market identity;
- `sourceMarketId`: the `market` field from a CLOB event, or null for non-CLOB sources;
- `tokenId`: single token affected, or null for metadata/lifecycle events;
- `observedTokenIds`: array of token IDs in a snapshot or touched by a price_change batch, or null;
- `sourceTimestamp`: upstream Unix time in milliseconds when supplied (prefers payload timestamp for
  Chainlink);
- `topLevelSourceTimestamp`: Chainlink top-level `timestamp` field, or null for non-Chainlink
  events;
- `payloadSourceTimestamp`: Chainlink `payload.timestamp` field, or null for non-Chainlink events;
- `receivedAt`: local wall-clock Unix time when the frame callback ran;
- `processedAt`: local wall-clock Unix time immediately before envelope construction;
- `connectionGeneration`: one-based socket attempt number, or null for market metadata;
- `frameId`: local per-frame identifier (e.g. `frame-3`), or null for lifecycle/metadata events;
- `messageIndex` / `messageCount`: zero-based index and total within a batched frame, or null;
- `isDuplicate`: true when a message was classified as a duplicate by the message-level
  deduplicator;
- `dedupeState`: `UNIQUE_APPLIED`, `DUPLICATE_APPLIED`, or `RECEIVED_NOT_APPLIED`;
- `duplicateOfSequence`: the sequence number of the first occurrence, or null;
- `dedupeReason`: human-readable reason when `isDuplicate` is true, or null;
- `payload`: JSON-safe normalized payload with decimal price/quantity fields as strings;
- `rawText`: original source frame text when applicable;
- `rawMessageText`: exact wire bytes for the individual message within a batched frame, or null for
  non-batched frames.

The file is append-only and synced during controlled shutdown. A crash can leave at most an
incomplete final line; consumers should reject or explicitly quarantine incomplete lines.

## `health.json`

Includes total and grouped event counts, malformed/duplicate counts, reconnects, maximum measured
latencies, stale incidents, both-token/reference observation flags, final health-only book state for
each token (including `BookState`, `synchronizationEpoch`, `snapshotConnectionGeneration`), open
gaps by feed, capture status, replay usability, quality failures, and feed-specific staleness
thresholds.

Latency calculations use a signed clock offset (local minus source) so that negative source-to-
receive latencies are preserved for diagnosis rather than clamped to zero.

## Capture status

`captureStatus` is one of `COMPLETE`, `PARTIAL`, or `UNUSABLE`:

- `COMPLETE`: both token streams observed, reference price stream observed and fresh, no open or
  closed gaps, no malformed events, no stale incidents, all books `SYNCHRONIZED` with
  current-generation snapshots and non-crossed integrity, no fatal errors, process completed
  successfully.
- `PARTIAL`: at least one quality issue detected (malformed events, stale/invalid/resync-required
  books, stale-generation snapshots, crossed books, missing or stale reference stream, open or
  closed gaps, stale incidents).
- `UNUSABLE`: fatal processor error, process failure, or no events captured at all.

`usableForReplay` is true only when `captureStatus` is `COMPLETE`. `qualityFailures` lists
`QualityFailure` objects with machine-readable `code` and human-readable `message` fields. Codes
include `MALFORMED_EVENTS_PRESENT`, `BOOK_STALE`, `BOOK_INVALID`, `BOOK_RESYNC_REQUIRED`,
`BOOK_SNAPSHOT_STALE_GENERATION`, `BOOK_CROSSED`, `REFERENCE_STREAM_MISSING`,
`REFERENCE_STREAM_STALE`, `REFERENCE_CLOCK_SKEW`, `REFERENCE_TIMESTAMP_MISSING`,
`REFERENCE_LOCAL_CLOCK_SKEW`, `OPEN_DATA_GAP`, `CLOSED_DATA_GAP`, `FATAL_PROCESSOR_ERROR`, and
`PROCESS_FAILURE`.

Each book health entry includes an `integrity` field of `NORMAL` (best bid < best ask), `LOCKED`
(best bid == best ask), or `CROSSED` (best bid > best ask), and a `crossedIncidentCount` tracking
cumulative crossed incidents during the run. Crossed books (current or historical) produce a
`BOOK_CROSSED` quality failure that prevents `usableForReplay`. `healthy` is true only when the book
is `SYNCHRONIZED` and integrity is not `CROSSED`.

The health summary also includes gap history fields: `dataGapCount` (total gaps started),
`closedGapCount` (gaps that ended), `totalGapDurationMs` (sum of gap durations), and
`maximumGapDurationMs` (longest gap). Any closed gap produces a `CLOSED_DATA_GAP` quality failure.

The health summary also includes reference freshness fields: `lastReferenceReceivedAt` (wall-clock
time of the last valid non-duplicate Chainlink observation, or null),
`lastReferenceConnectionGeneration` (connection generation of that observation, or null),
`lastReferenceSourceTimestamp` (Chainlink payload timestamp of that observation, or null),
`finalReferenceAgeMs` (age of the last observation at finalization by receipt time, or null),
`finalReferenceSourceAgeMs` (age by source timestamp, or null), `referenceFresh` (true only if the
reference was observed, both ages are within the staleness threshold, and the source timestamp is
not beyond the clock skew tolerance), `referenceClockSkewMs` (tolerance for future source
timestamps, default 5000ms), and `referenceTimestampAnomalyCount` (cumulative count of reference
observations with missing or extreme source timestamps). A single shared `evaluateReferenceQuality`
predicate is used for both `computeCaptureStatus` and `referenceFresh`, ensuring they can never
contradict each other. Missing source timestamps produce `REFERENCE_TIMESTAMP_MISSING`, stale source
timestamps produce `REFERENCE_STREAM_STALE`, future source timestamps beyond the skew tolerance
produce `REFERENCE_CLOCK_SKEW`, and negative receipt ages produce `REFERENCE_LOCAL_CLOCK_SKEW`.

## Clock semantics

- ISO timestamps and event wall times are UTC.
- Recording duration uses `performance.now()` so wall-clock adjustments do not shorten or extend a
  run.
- Reconnect gaps use local wall time and are always explicit events.
- Source-to-receive latency is computed with a signed clock offset and may be negative.
- Chainlink events prefer `payload.timestamp` as the canonical `sourceTimestamp` but persist both
  the top-level and payload timestamps for diagnosis.

## Offline use

All market identity, raw metadata, raw Gamma text, source frames, per-message text, and local
ordering needed for later replay live inside one directory. Reading or replaying a saved run must
not call Polymarket. The committed `fixtures/sample-run` directory is exercised by CI to enforce
this property.
