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
fails validation (unknown token, invalid side, negative size, price out of bounds, duplicate price
level), the entire batch is rolled back and the book transitions to `INVALID`. Recovery requires a
new full `book` snapshot from the current connection generation. Source prices are always recorded
exactly as received — the recorder faithfully captures what the feed sends. Tick size is recorded in
the manifest for reference but not enforced, as the live feed may send sub-tick prices; tick
enforcement is deferred to later execution logic.

The projection classifies each book's integrity as `NORMAL` (best bid < best ask), `LOCKED` (best
bid == best ask), or `CROSSED` (best bid > best ask). Locked and crossed states are preserved as
valid market data, but a `CROSSED` projection triggers a `BOOK_CROSSED` quality failure that
prevents `usableForReplay` for execution-quality replay. This separates raw event capture (which
must be faithful) from executable book health (which must be sound). Crossed incidents are tracked
cumulatively per token via `crossedIncidentCount`: a book that becomes crossed during recording and
later returns to normal still produces a `BOOK_CROSSED` quality failure. `BookHealth.healthy` is
true only when the book is `SYNCHRONIZED` and integrity is not `CROSSED`.

Pre-mutation rejections (unknown token, invalid side, missing fields, price out of bounds) throw
before any book backup is created. In this case the prior book state is preserved but the event is
recorded as malformed, making the overall capture unusable through `MALFORMED_EVENTS_PRESENT`. Only
failures during mutation (which occur after backups are created) transition the book to `INVALID`.
This policy ensures that a single malformed batch never silently corrupts a previously valid book.

After a reconnection gap, all books transition to `RESYNC_REQUIRED`. A `book` snapshot from the new
connection generation is required to restore `SYNCHRONIZED` state. Snapshots from a stale generation
are rejected. The `synchronizationEpoch` field tracks how many times the book has been
re-synchronized, and `snapshotConnectionGeneration` records which generation produced the current
snapshot.

The projection stores price and size strings exactly, removes zero-size levels, and computes best
prices with decimal-string comparison rather than floating point. It validates price bounds (0 <
price < 1) and duplicate price levels. It reports best bid, best ask, bid/ask level counts, book
state, integrity, synchronization epoch, snapshot generation, last successful update time, and
freshness. It is not an execution simulator and does not infer fills, queue position, or
profitability.

## Reconnection, gaps, and stale streams

Each feed has capped exponential backoff with 20% jitter, automatic subscription restoration, and a
monotonic reconnect generation. Unexpected closure appends `DATA_GAP_STARTED`; the gap ends only
when the first valid subscribed data message arrives on the current generation, appending
`DATA_GAP_ENDED` with duration. An open-but-silent socket does not close a gap. Valid data messages
from an old connection generation are ignored for gap-ending purposes.

Gap history is tracked cumulatively: `dataGapCount` counts all gaps that started during the run,
`closedGapCount` counts gaps that ended, `totalGapDurationMs` sums their durations, and
`maximumGapDurationMs` tracks the longest gap. Any closed gap produces a `CLOSED_DATA_GAP` quality
failure, preventing `COMPLETE` status even if the run later resynchronizes. This ensures a run that
missed events cannot silently become `COMPLETE` after reconnecting.

Staleness checking uses feed-specific thresholds: `clobStaleAfterMs` for the CLOB feed and
`referenceStaleAfterMs` for the RTDS feed. Freshness is refreshed only by `commitValidDataMessage`,
which is called after successful processing, not by raw WebSocket message arrival. This prevents
non-data frames (e.g., PING/PONG) from masking a silent data stream. The active-generation check
(`isCurrentGeneration`) and pre-gap check (`isPreGapObservation`) run before book mutation; the
commit (`commitValidDataMessage`) runs only after book validation, mutation, envelope persistence,
and dedupe registration succeed. The recorder only updates its current CLOB generation inside the
commit callback, preventing stale callbacks from overwriting the active generation.

Reference (Chainlink) freshness requires both a recent receipt time and a recent canonical source
timestamp. `lastReferenceReceivedAt` tracks when the last non-duplicate observation was received,
and `lastReferenceSourceTimestamp` tracks the Chainlink payload timestamp. `referenceFresh` is true
only when both ages are within `referenceStaleAfterMs` and the source timestamp is not beyond
`referenceClockSkewMs` in the future. Duplicate observations do not update freshness, preventing an
old value retransmitted on a new connection from appearing fresh. A single shared
`evaluateReferenceQuality` predicate is used for both `computeCaptureStatus` and `referenceFresh`,
ensuring they can never contradict each other. Missing source timestamps produce
`REFERENCE_TIMESTAMP_MISSING`, stale source timestamps produce `REFERENCE_STREAM_STALE`, future
source timestamps beyond the skew tolerance produce `REFERENCE_CLOCK_SKEW`, and negative receipt
ages (local clock skew) produce `REFERENCE_LOCAL_CLOCK_SKEW`. Reference timestamp anomalies are
tracked cumulatively via `referenceTimestampAnomalyCount` with a breakdown into
`referenceTimestampMissingCount`, `referenceStaleAtReceiptCount`, and
`referenceFutureSourceSkewCount`. Each historical reference observation is evaluated using the same
configured `referenceStaleAfterMs` and `referenceClockSkewMs` thresholds as finalization, so a bad
observation followed by a valid one still produces a quality failure.

If no valid data arrives within the feed-specific threshold, the feed emits a `STALE_STREAM`
incident, closes the socket, and schedules a reconnect. Open gaps are tracked in `health.json` under
`openGapsByFeed`. A stale CLOB socket that closes and emits `DATA_GAP_STARTED` does not increment
the synchronization epoch twice: the processor uses a `clobResyncPending` flag bound to the outage
generation so that both the `STALE_STREAM` and the subsequent `DATA_GAP_STARTED` from the same
outage count as one resync. The flag is cleared only when a `book` event from a strictly newer
connection generation is accepted by the active-generation check. `price_change` events no longer
clear the resync guard, and late books from an inactive generation are rejected before mutating the
live order book (marked `RECEIVED_NOT_APPLIED` with a stale-generation reason).

The active-generation check is performed by calling `isCurrentGeneration` and `isPreGapObservation`
before book mutation. If the feed has advanced to a newer generation, or if the frame's `receivedAt`
precedes the current gap start time (`gapStartedAt`), the frame is rejected without refreshing the
stale detector or emitting `DATA_GAP_ENDED`. The `commitValidDataMessage` call runs only after book
validation, mutation, envelope persistence, and dedupe registration succeed, ensuring that a failed
`books.apply()` cannot close a gap or refresh staleness. This prevents pre-gap observations from
retroactively closing a gap that started after the frame was received.

Frames that are whitespace-only or belong to a different market condition are recorded as
`EMPTY_FRAME` and `IGNORED_FOREIGN_MARKET` events respectively, preserving their exact wire content
for audit purposes. They do not count as malformed and do not refresh feed freshness.

Reconnect scheduling, errors, opens, closes, subscription messages, and stale incidents are all
recorded.

## Capture status and replay usability

`health.json` reports a `captureStatus` of `COMPLETE`, `PARTIAL`, or `UNUSABLE` based on:

- `COMPLETE`: both token streams observed, reference stream observed and fresh, no open or closed
  gaps, no malformed events, no stale incidents, all books `SYNCHRONIZED` with current-generation
  snapshots and non-crossed integrity, no fatal errors.
- `PARTIAL`: at least one quality issue detected (malformed events, stale books, invalid books,
  resync-required books, stale-generation snapshots, missing or stale reference stream, open or
  closed gaps, crossed books).
- `UNUSABLE`: fatal processor error, process failure, or no events captured at all.

`usableForReplay` is true only when `captureStatus` is `COMPLETE`. `qualityFailures` lists specific
`QualityFailure` objects with machine-readable `code` and human-readable `message` fields. Codes
include `MALFORMED_EVENTS_PRESENT`, `BOOK_STALE`, `BOOK_INVALID`, `BOOK_RESYNC_REQUIRED`,
`BOOK_SNAPSHOT_STALE_GENERATION`, `BOOK_CROSSED`, `REFERENCE_STREAM_MISSING`,
`REFERENCE_STREAM_STALE`, `REFERENCE_CLOCK_SKEW`, `REFERENCE_TIMESTAMP_MISSING`,
`REFERENCE_LOCAL_CLOCK_SKEW`, `OPEN_DATA_GAP`, `CLOSED_DATA_GAP`, `FATAL_PROCESSOR_ERROR`, and
`PROCESS_FAILURE`.

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
