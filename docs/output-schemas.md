# Output schemas and timing

All schemas are version `1`. Fields may be added compatibly, but changing field meaning requires a
new schema version.

## `manifest.json`

`RunManifest` contains the run ID, start/end ISO-8601 UTC timestamps, recorder version, Git commit,
validated `MarketSpecification`, safe CLI arguments, output filenames, completion status, final
event count, and failure reason.

The manifest is written as `RUNNING` before sockets start, then atomically replaced with `COMPLETED`
or `FAILED`. A remaining `RUNNING` status indicates interruption before finalization.

## `market.json`

Contains:

```json
{
  "schemaVersion": 1,
  "market": {
    "schemaVersion": 1,
    "eventId": "...",
    "marketId": "...",
    "conditionId": "...",
    "slug": "...",
    "upTokenId": "...",
    "downTokenId": "...",
    "referenceTopic": "crypto_prices_chainlink",
    "referenceSymbol": "btc/usd",
    "rawMetadata": {}
  }
}
```

The full Gamma response is retained under `rawMetadata` after JSON-safe decimal normalization.

## `events.jsonl`

Every complete line is one `RawEventEnvelope`:

- `sequence`: local one-based order; strictly increasing without gaps in written output;
- `sourceTimestamp`: upstream Unix time in milliseconds when supplied;
- `receivedAt`: local wall-clock Unix time when the frame callback ran;
- `processedAt`: local wall-clock Unix time immediately before envelope construction;
- `connectionGeneration`: one-based socket attempt number, or null for market metadata;
- `payload`: JSON-safe normalized payload with decimal price/quantity fields as strings;
- `rawText`: original source frame text when applicable.

The file is append-only and synced during controlled shutdown. A crash can leave at most an
incomplete final line; consumers should reject or explicitly quarantine incomplete lines.

## `health.json`

Includes total and grouped event counts, malformed/duplicate counts, reconnects, maximum measured
latencies, stale incidents, both-token/reference observation flags, and final health-only book state
for each token.

Latency calculations are clamped at zero because upstream and local wall clocks can differ.

## Clock semantics

- ISO timestamps and event wall times are UTC.
- Recording duration uses `performance.now()` so wall-clock adjustments do not shorten or extend a
  run.
- Reconnect gaps use local wall time and are always explicit events.

## Offline use

All market identity, raw metadata, source frames, and local ordering needed for later replay live
inside one directory. Reading or replaying a saved run must not call Polymarket. The committed
`fixtures/sample-run` directory is exercised by CI to enforce this property.
