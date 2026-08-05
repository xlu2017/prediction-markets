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
- `messageIndex` / `messageCount`: position within a batched frame array;
- `observedTokenIds`: token IDs present in a snapshot or touched by a price_change batch;
- `topLevelSourceTimestamp` / `payloadSourceTimestamp`: Chainlink top-level and payload timestamps;
- `rawMessageText`: exact wire bytes for the individual message within a batched frame.

CLOB events whose `market` field does not match the discovered `conditionId` are rejected as
malformed, preventing cross-market contamination. A missing `market` field on a CLOB event is also
rejected.

## Event processing

Both WebSockets emit immutable frame objects into one promise queue. The queue guarantees that local
sequence numbers and file append order agree. Each envelope includes:

- source and event type;
- market, condition, and token identity when available;
- source, receive, and processing timestamps;
- reconnect generation;
- frame and message identity;
- normalized payload;
- untouched wire-frame text (`rawText`) and per-message text (`rawMessageText`).

## Message-level deduplication

Duplicate suppression operates at the message level, not the frame level. The composite key is
`(connectionGeneration, frameHash, messageIndex, messageHash, sourceTimestamp)`, where `frameHash`
is a SHA-256 of the full frame text and `messageHash` is a SHA-256 of the individual message text.
This ensures that two distinct messages within the same array frame are never classified as
duplicates of each other, and that the same message retransmitted on a different connection
generation is not suppressed.

A message is registered as applied only after validation and book mutation succeed. If processing
throws, the message is not registered, and a future retransmission will be re-processed. The
`dedupeState` field on each envelope records the outcome: `UNIQUE_APPLIED`, `DUPLICATE_APPLIED`, or
`RECEIVED_NOT_APPLIED`.

Malformed source frames are not discarded silently. They increment the malformed count and append a
`MALFORMED_EVENT` envelope containing the error, original text, and any extractable source identity.

## Transactional order-book projection

The in-memory tracker implements a `BookState` machine:

- `UNINITIALIZED` -> `SYNCHRONIZED` (via a `book` snapshot)
- `SYNCHRONIZED` -> `STALE` (when no update arrives within the CLOB staleness threshold)
- `SYNCHRONIZED` -> `INVALID` (when a `price_change` batch fails validation)
- `SYNCHRONIZED` -> `RESYNC_REQUIRED` (when a gap occurs or a new connection generation arrives)
- Any state -> `SYNCHRONIZED` (via a fresh `book` snapshot from the current generation)

Updates are transactional: all changes in a batch are validated before any mutation. If any change
fails validation (unknown token, invalid side, negative size, crossed book, price out of bounds,
tick-size violation, duplicate price level, crossed book), the entire batch is rolled back and the
book transitions to `INVALID`. Recovery requires a new full `book` snapshot from the current
connection generation. Locked markets (best bid == best ask) are accepted; only truly crossed books
(best bid > best ask) are rejected.

Pre-mutation rejections (unknown token, invalid side, missing fields, tick-size violation, price out
of bounds) throw before any book backup is created. In this case the prior book state is preserved
but the event is recorded as malformed, making the overall capture unusable through
`MALFORMED_EVENTS_PRESENT`. Only failures during mutation or crossed-book validation (which occur
after backups are created) transition the book to `INVALID`. This policy ensures that a single
malformed batch never silently corrupts a previously valid book.

After a reconnection gap, all books transition to `RESYNC_REQUIRED`. A `book` snapshot from the new
connection generation is required to restore `SYNCHRONIZED` state. Snapshots from a stale generation
are rejected. The `synchronizationEpoch` field tracks how many times the book has been
re-synchronized, and `snapshotConnectionGeneration` records which generation produced the current
snapshot.

The projection stores price and size strings exactly, removes zero-size levels, and computes best
prices with decimal-string comparison rather than floating point. It validates price bounds (0 <
price < 1), tick-size alignment, and duplicate price levels. It reports best bid, best ask, bid/ask
level counts, book state, synchronization epoch, snapshot generation, last successful update time,
and freshness. It is not an execution simulator and does not infer fills, queue position, or
profitability.

## Reconnection, gaps, and stale streams

Each feed has capped exponential backoff with 20% jitter, automatic subscription restoration, and a
monotonic reconnect generation. Unexpected closure appends `DATA_GAP_STARTED`; the gap ends only
when the first valid subscribed data message arrives on the current generation, appending
`DATA_GAP_ENDED` with duration. An open-but-silent socket does not close a gap. Valid data messages
from an old connection generation are ignored for gap-ending purposes.

Staleness checking uses feed-specific thresholds: `clobStaleAfterMs` for the CLOB feed and
`referenceStaleAfterMs` for the RTDS feed. Freshness is refreshed only by `notifyValidDataMessage`,
which is called after successful processing, not by raw WebSocket message arrival. This prevents
non-data frames (e.g., PING/PONG) from masking a silent data stream.

If no valid data arrives within the feed-specific threshold, the feed emits a `STALE_STREAM`
incident, closes the socket, and schedules a reconnect. Open gaps are tracked in `health.json` under
`openGapsByFeed`.

Reconnect scheduling, errors, opens, closes, subscription messages, and stale incidents are all
recorded.

## Capture status and replay usability

`health.json` reports a `captureStatus` of `COMPLETE`, `PARTIAL`, or `UNUSABLE` based on:

- `COMPLETE`: both token streams observed, reference stream observed, no open gaps, no malformed
  events, no stale incidents, all books `SYNCHRONIZED` with current-generation snapshots, no fatal
  errors.
- `PARTIAL`: at least one quality issue detected (malformed events, stale books, invalid books,
  resync-required books, stale-generation snapshots, missing reference stream, open gaps).
- `UNUSABLE`: fatal processor error, or no events captured at all.

`usableForReplay` is true only when `captureStatus` is `COMPLETE`. `qualityFailures` lists specific
`QualityFailure` objects with machine-readable `code` and human-readable `message` fields. Codes
include `MALFORMED_EVENTS_PRESENT`, `BOOK_STALE`, `BOOK_INVALID`, `BOOK_RESYNC_REQUIRED`,
`BOOK_SNAPSHOT_STALE_GENERATION`, `REFERENCE_STREAM_MISSING`, `OPEN_GAPS`, `STALE_INCIDENTS`,
`FATAL_PROCESSOR_ERROR`, and `NO_EVENTS_CAPTURED`.

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
