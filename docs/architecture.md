# Architecture

## Run lifecycle

1. Parse and validate CLI arguments.
2. Discover an event through public Gamma endpoints with bounded retry on 429/5xx.
3. Normalize and validate one unambiguous active BTC five-minute Up/Down market.
4. Create an exclusive run directory and write `RUNNING` manifest and market files.
5. Open the public CLOB market channel for both tokens and the Chainlink RTDS `btc/usd` channel.
6. Serialize all lifecycle and source frames through one processing queue.
7. On duration expiry, `SIGINT`, `SIGTERM`, or fatal processing error, close sockets, drain the
   queue, sync JSONL, and atomically replace final health and manifest files.
8. On unrecoverable processing/storage failure, make a best effort to finalize the run as `FAILED`
   and return a nonzero process status.

## Contract validation

`validateBtcFiveMinuteEvent` fails closed unless all of the following hold:

- the event and selected market are explicitly active and not closed;
- title/slug/question identify Bitcoin or BTC and Up/Down;
- opening and closing timestamps are parseable and exactly 300,000 ms apart;
- current time falls inside the interval, with only a 15-second pre-open tolerance for publication
  timing;
- there are exactly two outcomes named `Up` and `Down`;
- each outcome has a nonempty CLOB token ID, mapped by label rather than position assumption;
- a condition ID is present;
- the rules identify Chainlink and BTC/USD or Bitcoin.

An explicit slug bypasses discovery only. It never bypasses validation.

## Event identity

Every envelope carries:

- `marketId`: stable local market identifier derived from the Gamma event id;
- `conditionId`: the Polymarket CLOB condition id used as the on-wire market identity;
- `sourceMarketId`: the `market` field from a CLOB event, validated against `conditionId`;
- `frameId`: local per-frame identifier for correlating batched messages;
- `messageIndex` / `messageCount`: position within a batched frame array.

CLOB events whose `market` field does not match the discovered `conditionId` are rejected as
malformed, preventing cross-market contamination.

## Event processing

Both WebSockets emit immutable frame objects into one promise queue. The queue guarantees that local
sequence numbers and file append order agree. Each envelope includes:

- source and event type;
- market, condition, and token identity when available;
- source, receive, and processing timestamps;
- reconnect generation;
- frame and message identity;
- normalized payload;
- untouched wire-frame text.

## Conservative deduplication

Duplicate suppression uses a composite key of connection generation, source timestamp, and a SHA-256
hash of the raw frame text. This avoids the unsafe previous approach of keying on CLOB `hash` fields
alone, which could suppress legitimate cancellations that share a hash but have different content.
The first occurrence's sequence number is retained in `duplicateOfSequence` for traceability.

Malformed source frames are not discarded silently. They increment the malformed count and append a
`MALFORMED_EVENT` envelope containing the error and original text.

## Transactional order-book projection

The in-memory tracker implements a `BookState` machine: `UNINITIALIZED` -> `SYNCHRONIZED` ->
`INVALID`. Updates are transactional: all changes in a batch are validated before any mutation. If
any change fails validation (unknown token, invalid side, negative size, crossed book), the entire
batch is rolled back and the book transitions to `INVALID`. Recovery requires a new full `book`
snapshot.

The projection stores price and size strings exactly, removes zero-size levels, and computes best
prices with decimal-string comparison rather than floating point. It reports best bid, best ask,
bid/ask level counts, book state, last successful update time, and freshness. It is not an execution
simulator and does not infer fills, queue position, or profitability.

## Reconnection, gaps, and stale streams

Each feed has capped exponential backoff with 20% jitter, automatic subscription restoration, and a
monotonic reconnect generation. Unexpected closure appends `DATA_GAP_STARTED`; the gap ends only
when the first valid subscribed data message arrives, appending `DATA_GAP_ENDED` with duration. An
open-but-silent socket does not close a gap.

If no data arrives within `staleAfterMs`, the feed emits a `STALE_STREAM` incident, closes the
socket, and schedules a reconnect. Open gaps are tracked in `health.json` under `openGapsByFeed`.

Reconnect scheduling, errors, opens, closes, subscription messages, and stale incidents are all
recorded.

## Capture status and replay usability

`health.json` reports a `captureStatus` of `COMPLETE`, `PARTIAL`, or `FAILED` based on whether both
token streams were observed, the reference price stream was observed, no gaps remain open, no
malformed events occurred, and all books are synchronized. `usableForReplay` is true only when
`captureStatus` is `COMPLETE`. `qualityFailures` lists the specific reasons.

## Secret boundary

No runtime configuration is read from environment credentials. Remote URLs are hard-coded to a small
public allowlist and internal overrides exist only for local tests. CI scans `src` for signing
libraries, credential access, mutating HTTP methods, authenticated clients, and order/relayer path
strings.

## Known limitations

- Live public interface drift can still require schema or validation updates. The recorder fails
  closed rather than guessing.
- The fast path uses the observed interval slug convention, with official event-list discovery as a
  fallback.
- A successful timed run can still report `PARTIAL` capture status if a public feed is quiet or
  unavailable. Consumers must inspect `captureStatus` and `usableForReplay` before using a run.
- Only the current two-token BTC five-minute Up/Down contract format is supported.
- The public smoke test was not run in the build environment because direct Polymarket egress was
  unavailable. A manual, read-only GitHub Actions workflow is included.
