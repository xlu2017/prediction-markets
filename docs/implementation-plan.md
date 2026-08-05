# Implementation layout decision

Recorded before implementation on 2026-08-05.

The target repository was new and contained no reusable worker, service, package, lint, logging, or
test conventions. The smallest coherent structure was therefore one private TypeScript package, not
a monorepo or separate web/API application.

```text
src/
  cli.ts                         CLI and signal handling
  recorder.ts                    lifecycle orchestration
  gamma.ts                       public market discovery
  market.ts                      validation and normalized specification
  feeds/managed-websocket.ts     reconnecting public WebSocket transport
  processor.ts                   parsing, dedupe, sequencing, persistence queue
  book.ts                        minimal health-only book projection
  health.ts                      run health aggregation
  store.ts                       exclusive run directory and safe finalization
  decimal.ts                     exact decimal boundary helpers
  reliability.ts                 backoff and stale detection
  types.ts                       versioned domain contracts
test/
  fixtures/                      deterministic Gamma input
  integration/                   mocked HTTP/WebSocket end-to-end run
fixtures/sample-run/             committed offline replay input
docs/                            architecture, schemas, official interfaces, handoff
scripts/assert-read-only.mjs     CI safety-boundary check
```

Decision rationale:

1. Node 22 supplies public `fetch`, monotonic performance timing, abort signals, UUIDs, and modern
   file APIs, reducing dependencies.
2. `ws` is used only as a public WebSocket transport. No Polymarket trading client is installed.
3. `zod` validates the volatile Gamma boundary while normalized internal contracts remain explicit
   TypeScript types.
4. JSON/JSONL is intentionally used instead of a database or analytics format so each run is easy to
   inspect and replay.
5. Full local book reconstruction is deferred. The current projection exists only to expose best
   bid/ask, depth counts, freshness, and stream health.
