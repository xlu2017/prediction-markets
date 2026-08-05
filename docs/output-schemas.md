# Output schemas and timing

All schemas are version `2`. Fields may be added compatibly, but changing field meaning requires a
new schema version. Schema v2 adds event identity (`conditionId`, `sourceMarketId`, `frameId`,
`messageIndex`, `messageCount`), conservative deduplication metadata (`isDuplicate`,
`duplicateOfSequence`, `dedupeReason`), transactional book state (`BookState`), capture-quality
status (`captureStatus`, `usableForReplay`, `qualityFailures`, `openGapsByFeed`), and the raw Gamma
response text (`rawGammaText`).

## `manifest.json`

`RunManifest` contains the run ID, start/end ISO-8601 UTC timestamps, recorder version, Git commit,
validated `MarketSpecification`, safe CLI arguments, output filenames, completion status, final
event count, failure reason, capture status, replay usability flag, and quality failures.

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
- `sourceTimestamp`: upstream Unix time in milliseconds when supplied;
- `receivedAt`: local wall-clock Unix time when the frame callback ran;
- `processedAt`: local wall-clock Unix time immediately before envelope construction;
- `connectionGeneration`: one-based socket attempt number, or null for market metadata;
- `frameId`: local per-frame identifier (e.g. `frame-3`), or null for lifecycle/metadata events;
- `messageIndex` / `messageCount`: zero-based index and total within a batched frame, or null;
- `isDuplicate`: true when a frame was classified as a duplicate by the conservative deduplicator;
- `duplicateOfSequence`: the sequence number of the first occurrence, or null;
- `dedupeReason`: human-readable reason when `isDuplicate` is true, or null;
- `payload`: JSON-safe normalized payload with decimal price/quantity fields as strings;
- `rawText`: original source frame text when applicable.

The file is append-only and synced during controlled shutdown. A crash can leave at most an
incomplete final line; consumers should reject or explicitly quarantine incomplete lines.

## `health.json`

Includes total and grouped event counts, malformed/duplicate counts, reconnects, maximum measured
latencies, stale incidents, both-token/reference observation flags, final health-only book state for
each token (including `BookState`), open gaps by feed, capture status, replay usability, and quality
failures.

Latency calculations use a signed clock offset (local minus source) so that negative source-to-
receive latencies are preserved for diagnosis rather than clamped to zero.

## Capture status

`captureStatus` is one of `COMPLETE`, `PARTIAL`, or `FAILED`:

- `COMPLETE`: both token streams observed, reference price stream observed, no open gaps, no
  malformed events, and all books synchronized.
- `PARTIAL`: at least one stream observed but one or more quality checks failed.
- `FAILED`: no usable data captured.

`usableForReplay` is true only when `captureStatus` is `COMPLETE`. `qualityFailures` lists the
specific reasons the run is not complete.

## Clock semantics

- ISO timestamps and event wall times are UTC.
- Recording duration uses `performance.now()` so wall-clock adjustments do not shorten or extend a
  run.
- Reconnect gaps use local wall time and are always explicit events.
- Source-to-receive latency is computed with a signed clock offset and may be negative.

## Offline use

All market identity, raw metadata, raw Gamma text, source frames, and local ordering needed for
later replay live inside one directory. Reading or replaying a saved run must not call Polymarket.
The committed `fixtures/sample-run` directory is exercised by CI to enforce this property.
