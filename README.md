# Polymarket BTC Five-Minute Recorder

A strictly read-only TypeScript recorder for the active Polymarket BTC five-minute Up/Down market.
It discovers and validates the contract, subscribes to both public outcome-token market streams,
records the settlement-aligned Chainlink `btc/usd` reference stream, and writes an append-only run
that can be inspected offline.

This is research data infrastructure. It contains no wallet integration, authentication, order
construction, execution, signals, strategy, position tracking, or P&L logic.

## Requirements

- Node.js 22 or newer
- Corepack
- Network access to the three public endpoints listed in
  [`docs/official-interfaces.md`](docs/official-interfaces.md)

## Setup

```bash
corepack pnpm install --frozen-lockfile
corepack pnpm ci
```

Dependencies and the package manager are pinned exactly in `package.json`; `pnpm-lock.yaml` is
committed.

## Record a market

Automatically discover the active interval and record for two minutes:

```bash
corepack pnpm market:record --duration-seconds 120
```

Validate and record a particular event slug:

```bash
corepack pnpm market:record \
  --duration-seconds 120 \
  --market-slug btc-updown-5m-1785909600 \
  --output-dir data/polymarket-runs \
  --log-level info
```

The explicit slug is never trusted blindly. It passes the same asset, interval, outcome, token,
time-window, status, and resolution-source checks as an automatically discovered market.

Press `Ctrl+C` to request controlled shutdown. The recorder closes both WebSockets, drains queued
events, syncs `events.jsonl`, and finalizes `health.json` and `manifest.json`.

## Output

Each invocation creates an exclusive directory:

```text
data/polymarket-runs/<run-id>/
  manifest.json
  market.json
  events.jsonl
  health.json
```

- `manifest.json`: run identity, arguments, Git commit, validated market, status, and final count.
- `market.json`: versioned normalized market specification plus full raw Gamma metadata.
- `events.jsonl`: one versioned envelope per complete line with a strictly increasing local sequence
  number.
- `health.json`: source/type counts, malformed and duplicate counts, reconnects, gaps, latency
  maxima, stream-observation flags, and final book-health projections.

The committed [`fixtures/sample-run`](fixtures/sample-run) directory is a synthetic, deterministic
saved run used for offline tests. It is not evidence of live market behaviour.

## Decimal and raw-data guarantees

- CLOB prices and quantities are accepted only when the public market channel supplies decimal
  strings. Numeric CLOB decimal fields are rejected as malformed rather than passed through a
  JavaScript floating-point conversion.
- The RTDS reference-price decimal lexeme is extracted from the original wire frame before the
  parsed payload is stored.
- Every source frame is retained in `rawText`; normalized payloads retain price and size values as
  strings.
- Source, receive, and processing timestamps are distinct. Elapsed-duration control uses a monotonic
  clock.

## Commands

```bash
corepack pnpm read-only:check
corepack pnpm format:check
corepack pnpm lint
corepack pnpm typecheck
corepack pnpm test
corepack pnpm build
```

`corepack pnpm ci` runs all non-build verification. Unit and integration tests use fixtures and
local mock servers; normal CI does not contact Polymarket. The separate GitHub Actions workflow
“Read-only public smoke test” is manual and has read-only repository permissions.

## Architecture

See:

- [`docs/implementation-plan.md`](docs/implementation-plan.md) for the pre-implementation layout
  decision.
- [`docs/architecture.md`](docs/architecture.md) for components, discovery, validation, connection
  lifecycle, and limitations.
- [`docs/output-schemas.md`](docs/output-schemas.md) for file and timestamp semantics.
- [`docs/devin-handoff.md`](docs/devin-handoff.md) for the next bounded implementation phase.

## Pinned versions

| Package           | Version |
| ----------------- | ------: |
| pnpm              | 10.17.1 |
| commander         |  15.0.0 |
| ws                |  8.21.2 |
| zod               |   4.4.3 |
| TypeScript        |   6.0.3 |
| tsx               |  4.23.7 |
| Vitest            |  4.1.10 |
| ESLint            |  10.8.0 |
| typescript-eslint |  8.66.0 |
| Prettier          |   3.9.6 |

## Safety boundary

Runtime code is limited to public Gamma `GET` discovery and two unauthenticated public WebSockets. A
source scan runs in CI to reject credential access, signing libraries, mutating HTTP methods,
authenticated clients, and order/relayer endpoint strings.

No authenticated endpoint is called and no order can be placed by this codebase.
