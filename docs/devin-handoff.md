# Devin handoff

## What is implemented

- Public Gamma discovery with current-boundary slug candidates, official keyset fallback, and
  bounded retry on 429/5xx responses.
- Fail-closed validation for the active BTC five-minute Up/Down contract, with raw Gamma response
  text retained verbatim for byte-exact replay.
- Public CLOB subscription for both outcome token IDs, with `conditionId` validation on every
  inbound CLOB event.
- Public Chainlink RTDS subscription for settlement-aligned `btc/usd` prices.
- Versioned (schema v2) market, event, manifest, health, and book-health contracts.
- Event identity contract: `marketId`, `conditionId`, `sourceMarketId`, `frameId`, `messageIndex`,
  `messageCount` on every envelope.
- Conservative CLOB deduplication keyed on connection generation + source timestamp + raw-frame
  SHA-256 hash, with `isDuplicate`, `duplicateOfSequence`, and `dedupeReason` metadata.
- Transactional order-book updates with `BookState` machine, validate-before-mutate, crossed-book
  detection, and rollback on partial batch failures.
- Gap semantics: `DATA_GAP_ENDED` only on first valid subscribed data message; stale streams close
  the socket and reconnect; open gaps tracked in `health.json`.
- Capture status (`COMPLETE`/`PARTIAL`/`FAILED`), `usableForReplay` flag, and `qualityFailures` list
  in manifest and health.
- Fatal processing error propagation: a processing failure aborts the run and finalizes as `FAILED`.
- Exact decimal-string boundary handling plus original wire-frame retention.
- Serialized JSONL writes, exclusive run directories, controlled shutdown, and atomic final files.
- Capped reconnect backoff, resubscription, stale detection, explicit gaps, and structured logs.
- Unit tests, mocked HTTP/WebSocket integration tests, offline fixture test, real-Gamma
  compatibility test, CI, and manual public smoke workflow.

## Important decisions

1. This repository is one focused Node/TypeScript package. No database or service fleet is needed.
2. Only three public Polymarket interfaces are allowed; no general-purpose trading client is
   installed.
3. CLOB decimal fields must arrive as strings. Numeric variants become malformed events rather than
   silently losing precision.
4. Raw wire text is canonical evidence; normalized payloads are ergonomic views.
5. The order book is a transactional health projection with state machine, not a fill or execution
   model.
6. Deduplication is conservative: only exact raw-frame matches within the same connection generation
   and source timestamp are suppressed. CLOB `hash` fields are not used as dedup keys.
7. A run is only `usableForReplay` when `captureStatus` is `COMPLETE`.

## Output contract

See [`output-schemas.md`](output-schemas.md). Treat `schemaVersion`, `sequence`, `conditionId`,
`rawText`, `rawGammaText`, and all three timestamps as durable replay inputs. Treat any run with
`captureStatus` not equal to `COMPLETE`, open gaps, or `RUNNING`/`FAILED` status as unsuitable for
model research until explicitly reviewed.

## Obtain a replay fixture

1. Run `corepack pnpm market:record --duration-seconds 120` from a network that can reach the public
   endpoints.
2. Inspect `health.json`; require `captureStatus: "COMPLETE"` and `usableForReplay: true`.
3. Confirm `manifest.json` is `COMPLETED` and `finalEventCount` equals the number of JSONL lines.
4. Copy the complete run directory into a new fixture path. Never copy credentials; the recorder
   does not use any.
5. Add a fixture test before changing schemas.

`fixtures/sample-run` is synthetic and suitable for parser/replay tests, not market conclusions.

## Incomplete areas

- Deterministic replay engine.
- Full level-by-level order-book reconstruction with invariant checks at every sequence.
- Gap-aware replay policy and explicit unusable intervals.
- Snapshot/checkpoint support for fast replay seeking.
- Fixture migration tooling for future schema versions.
- A successfully captured live smoke artifact from the current official interfaces.

## Recommended next task

Implement deterministic offline replay and local order-book reconstruction only.

Acceptance criteria for that phase:

1. Input is a saved run directory; the replay command makes no network calls.
2. Validate manifest status, file presence, schema versions, line validity, and strictly increasing
   sequences before replay.
3. Reject runs where `captureStatus` is not `COMPLETE` or `usableForReplay` is false.
4. Rebuild independent Up and Down books from `book` snapshots and `price_change` frames using
   decimal strings, respecting the transactional `BookState` machine.
5. Emit deterministic best bid/ask, spread, depth, and Chainlink reference-price state after every
   relevant sequence.
6. Represent reconnect gaps and stale periods explicitly; never interpolate through them silently.
7. Detect changes before an initial snapshot, negative/invalid sizes, crossed books, unknown token
   IDs, and sequence discontinuities.
8. Given identical input bytes, produce byte-identical replay output.
9. Test against `fixtures/sample-run` plus malformed, duplicate, reconnect, and gap fixtures.

Do not add simulated orders, live execution, wallet support, a prediction model, expected-value
logic, position sizing, or P&L in the replay phase.
