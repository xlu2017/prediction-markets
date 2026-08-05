# Devin handoff

## What is implemented

- Public Gamma discovery with current-boundary slug candidates and official keyset fallback.
- Fail-closed validation for the active BTC five-minute Up/Down contract.
- Public CLOB subscription for both outcome token IDs.
- Public Chainlink RTDS subscription for settlement-aligned `btc/usd` prices.
- Versioned market, event, manifest, health, and book-health contracts.
- Exact decimal-string boundary handling plus original wire-frame retention.
- Serialized JSONL writes, exclusive run directories, controlled shutdown, and atomic final files.
- Capped reconnect backoff, resubscription, stale detection, explicit gaps, bounded dedupe, and
  structured logs.
- Unit tests, mocked HTTP/WebSocket integration tests, offline fixture test, CI, and manual public
  smoke workflow.

## Important decisions

1. This repository is one focused Node/TypeScript package. No database or service fleet is needed.
2. Only three public Polymarket interfaces are allowed; no general-purpose trading client is
   installed.
3. CLOB decimal fields must arrive as strings. Numeric variants become malformed events rather than
   silently losing precision.
4. Raw wire text is canonical evidence; normalized payloads are ergonomic views.
5. The current order book is a health projection, not a fill or execution model.

## Output contract

See [`output-schemas.md`](output-schemas.md). Treat `schemaVersion`, `sequence`, `rawText`, and all
three timestamps as durable replay inputs. Treat any run with missing feed flags, malformed events,
an unclosed gap, or `RUNNING`/`FAILED` status as unsuitable for model research until explicitly
reviewed.

## Obtain a replay fixture

1. Run `corepack pnpm market:record --duration-seconds 120` from a network that can reach the public
   endpoints.
2. Inspect `health.json`; require both token streams and the Chainlink reference stream.
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
3. Rebuild independent Up and Down books from `book` snapshots and `price_change` frames using
   decimal strings.
4. Emit deterministic best bid/ask, spread, depth, and Chainlink reference-price state after every
   relevant sequence.
5. Represent reconnect gaps and stale periods explicitly; never interpolate through them silently.
6. Detect changes before an initial snapshot, negative/invalid sizes, crossed books, unknown token
   IDs, and sequence discontinuities.
7. Given identical input bytes, produce byte-identical replay output.
8. Test against `fixtures/sample-run` plus malformed, duplicate, reconnect, and gap fixtures.

Do not add simulated orders, live execution, wallet support, a prediction model, expected-value
logic, position sizing, or P&L in the replay phase.
