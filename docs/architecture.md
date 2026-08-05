# Architecture

## Run lifecycle

1. Parse and validate CLI arguments.
2. Discover an event through public Gamma endpoints.
3. Normalize and validate one unambiguous active BTC five-minute Up/Down market.
4. Create an exclusive run directory and write `RUNNING` manifest and market files.
5. Open the public CLOB market channel for both tokens and the Chainlink RTDS `btc/usd` channel.
6. Serialize all lifecycle and source frames through one processing queue.
7. On duration expiry, `SIGINT`, or `SIGTERM`, close sockets, drain the queue, sync JSONL, and
   atomically replace final health and manifest files.
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

## Event processing

Both WebSockets emit immutable frame objects into one promise queue. The queue guarantees that local
sequence numbers and file append order agree. Each envelope includes:

- source and event type;
- market and token identity when available;
- source, receive, and processing timestamps;
- reconnect generation;
- normalized payload;
- untouched wire-frame text.

Stable CLOB hashes and Chainlink symbol/timestamp/value tuples are used for bounded duplicate
suppression when present. Frames without stable IDs are retained.

Malformed source frames are not discarded silently. They increment the malformed count and append a
`MALFORMED_EVENT` envelope containing the error and original text.

## Minimal order-book projection

The in-memory tracker handles full `book` snapshots and incremental `price_change` messages for the
two expected token IDs. It stores price and size strings exactly, removes zero-size levels, and
computes best prices with decimal-string comparison rather than floating point.

The projection reports best bid, best ask, bid/ask level counts, last update time, and freshness. It
is not an execution simulator and does not infer fills, queue position, or profitability.

## Reconnection and gaps

Each feed has capped exponential backoff with 20% jitter, automatic subscription restoration, and a
monotonic reconnect generation. Unexpected closure appends `DATA_GAP_STARTED`; the next successful
open appends `DATA_GAP_ENDED` with duration. Reconnect scheduling, errors, opens, closes,
subscription messages, and stale incidents are all recorded.

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
- A successful timed run can still report missing token or reference observations in `health.json`
  if a public feed is quiet or unavailable. Consumers must inspect health before using a run.
- Only the current two-token BTC five-minute Up/Down contract format is supported.
- The current book exists only for health. Deterministic full reconstruction and replay are the next
  phase.
- The public smoke test was not run in the build environment because direct Polymarket egress was
  unavailable. A manual, read-only GitHub Actions workflow is included.
