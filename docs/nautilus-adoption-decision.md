# Nautilus adoption decision

Phase 11 deliverable. This document records the decision on whether to adopt NautilusTrader for
deterministic replay, backtesting, paper trading, portfolio accounting, and eventual execution,
while keeping the TypeScript recorder as the authoritative lossless acquisition and audit layer.

## Conclusion

**`ADOPT_WITH_CUSTOM_ADAPTER`**

NautilusTrader provides a strong, deterministic, production-grade engine for replay, backtesting,
matching/fill simulation, portfolio accounting, and execution. It can represent both outcome-token
books correctly and preserve source-envelope lineage **when a custom adapter is built**. It cannot,
on its own, preserve the recorder's lossless audit semantics or model Chainlink reference prices —
those require custom work that this spike has prototyped.

The existing Nautilus Polymarket adapter is reusable for **live execution and live market data** but
is **not directly reusable for replaying schema-v2 recordings**: it expects a live CLOB/Gamma client
and discards raw frame text, dedup metadata, and connection-generation lineage. Replay requires the
custom schema-v2 → Nautilus converter and catalog driver built in this spike.

## Answers to the required questions

1. **Can Nautilus replay the required Polymarket data deterministically?** Yes. The spike converts a
   verified schema-v2 run into a Parquet catalog and replays it twice in independent processes with
   identical hashes for the input stream, both books after each event, final books, the
   reference-price stream, the strategy-visible stream, and the simulated order-intent stream.

2. **Can it represent both outcome-token books correctly?** Yes. Each outcome token is a distinct
   `BinaryOption` instrument with its own `InstrumentId` (`{conditionId}-{tokenId}.POLYMARKET`). The
   Up and Down books are reconstructed independently and match the recorder's `finalBookHealth` for
   best bid/ask, depth counts, and integrity.

3. **Can Chainlink reference prices be represented without semantic distortion?** Yes, via a custom
   Nautilus `@customdataclass` `ReferencePriceUpdate` data type. It is stored as `CustomData` in the
   catalog, remains independently queryable, and is never misrepresented as a trade, quote, mark
   price, or fill. The exact source decimal (e.g. `67234.5000000000000001`) survives the catalog
   round trip.

4. **Can every Nautilus event retain source-envelope lineage?** Yes, via the `lineage.parquet`
   sidecar written during conversion. Every normalized record carries an immutable `Lineage`
   (`runId`, `sequence`, `frameId`, `messageIndex`, `connectionGeneration`, `marketId`,
   `conditionId`, `tokenId`, `rawEventType`, timestamps). A round-trip test proves every normalized
   event traces back to its source `sequence`.

5. **Does catalog serialization preserve required decimal precision?** Yes. Decimal price/quantity
   fields stay `str` through the normalized format and are converted to Nautilus `Price`/`Quantity`
   via `from_str` (string parse, never `float`). The reference decimal is preserved verbatim as a
   string in the custom data type and in the audit sidecar.

6. **Does the reconstructed final book match the recorder?** Yes. The Phase 9 comparison verifies
   best bid, best ask, bid/ask depth counts, integrity classification, and token identity against
   `health.finalBookHealth`. `allMatch` is true for the sample fixture. Full level-by-level depth is
   compared in the replay hash (the `levels` field), not only best bid/ask.

7. **Which planned roadmap components become unnecessary?** Replay engine, backtester, matching/fill
   simulation, portfolio/account state, and the data catalog can be served by Nautilus rather than
   built from scratch. The custom deterministic book-reconstruction and hashing harness from this
   spike can be retained as the verification layer.

8. **Which custom components must remain?**
   - The TypeScript lossless recorder (unchanged, authoritative audit layer).
   - The schema-v2 → Nautilus converter (`prediction_markets_nautilus.convert`).
   - The `Lineage` sidecar and audit sidecar (`lineage.parquet`, `audit.parquet`).
   - The custom `ReferencePriceUpdate` and `LifecycleEvent` Nautilus data types.
   - The deterministic replay/hash verification harness (`prediction_markets_nautilus.replay`).
   - Schema verification on the Python side (mirrors the TypeScript Zod contract).

9. **What new operational risks are introduced?**
   - **LGPL-3.0-or-later** dependency (weak copyleft, library-level). Using Nautilus as an
     unmodified dependency does not force the project's code to LGPL, but modifying the Nautilus
     core/adapter would. Confirm against the project's distribution model.
   - **Version churn**: Nautilus is transitioning from v1 (Cython) to v2 (Rust + PyO3); v1 is
     entering maintenance-only. Pinning (`1.231.0`) mitigates this; a v2 upgrade path must be
     planned.
   - **Heavy dependency surface** (pyarrow, pandas, numpy, msgspec) for the Python spike.
   - **Catalog filter quirk**: the Parquet catalog's `instrument_id` filter was unreliable in
     `1.231.0`; the spike filters manually. This must be re-checked on upgrade.
   - **No native Chainlink model**: the custom reference-price type is project-specific and must be
     maintained.

10. **Estimated maintenance burden of the adapter?** Low-to-moderate. The adapter is a thin,
    well-bounded converter plus two custom data types and a deterministic replay harness. The
    schema-v2 contract is stable (versioned). The main ongoing cost is re-validating against new
    Nautilus releases and re-running the 21 edge-case tests. No trading/execution code is maintained
    by the project — that is Nautilus's responsibility.

11. **Is the current Nautilus Polymarket adapter reusable directly?** Not for replay. It is reusable
    directly for **live execution and live market data** (`PolymarketExecutionClient`,
    `PolymarketDataClient`, `PolymarketInstrumentProvider`). For replay of schema-v2 recordings, the
    custom converter + catalog + backtest-node driver built here is required because the official
    adapter consumes a live CLOB/Gamma client and does not preserve recorder lineage or raw frames.

12. **Should the official Polymarket SDK replace any TypeScript transport code?** Not in this spike
    and not for the recorder. The TypeScript recorder is the lossless audit layer and must remain
    unchanged. The official SDKs (TS/Python/Rust) are transport/SDK layers that decode to typed
    events and discard raw frames, dedup metadata, and connection-generation identity — exactly what
    the recorder must preserve. They are candidates for a future **live execution** path (Rust SDK
    with `gamma`/`ws`/`rtds`/`data` features is the most complete), but that is out of scope for
    this read-only spike and would require authentication surfaces that are explicitly excluded
    here.

## Build-versus-adopt matrix

| Component            |   Keep custom |        Nautilus |                 Official SDK | Reason                                                                               |
| -------------------- | ------------: | --------------: | ---------------------------: | ------------------------------------------------------------------------------------ |
| Lossless recorder    |            ✅ |               — |                            — | Schema-v2 audit semantics (raw frames, dedup, lineage) no component preserves.       |
| Gamma discovery      | ✅ (recorder) | optional (live) |              optional (live) | Recorder retains raw Gamma text; SDKs usable only for live path.                     |
| CLOB transport       | ✅ (recorder) | optional (live) |              optional (live) | Recorder retains raw WS frames; SDKs usable only for live path.                      |
| RTDS transport       | ✅ (recorder) |               — | optional (live, Rust `rtds`) | Recorder retains Chainlink raw frames; only Rust SDK models RTDS, as live transport. |
| Schema verification  |   ✅ (mirror) |               — |                            — | TypeScript Zod is source of truth; Python mirrors it for replay validation.          |
| Normalized catalog   |  ✅ (adapter) |    ✅ (Parquet) |                            — | Custom normalized format + lineage sidecar over Nautilus Parquet catalog.            |
| Replay engine        |             — |              ✅ |                            — | Nautilus deterministic backtest node + custom hash harness.                          |
| Backtester           |             — |              ✅ |                            — | Nautilus `BacktestNode`.                                                             |
| Matching/fill model  |             — |              ✅ |                            — | Nautilus simulated execution.                                                        |
| Portfolio accounting |             — |              ✅ |                            — | Nautilus portfolio/account state.                                                    |
| Paper trading        |             — |              ✅ |                            — | Nautilus sandbox/simulated execution.                                                |
| Live execution       |             — |    ✅ (adapter) |                  ✅ (future) | Nautilus `PolymarketExecutionClient` now; official SDK a future alternative.         |

## Spike evidence

- Sample fixture `fixtures/sample-run` converts: 2 snapshots, 1 reference, 1 lifecycle, 0 audit.
- Two independent process replays produce identical hashes (see CI).
- Final-book comparison `allMatch: true` against `health.finalBookHealth`.
- 21 edge-case tests pass (20 required cases + reference-price queryability).
- No authenticated endpoint called; no order placed.

## Recommendation

Proceed with `ADOPT_WITH_CUSTOM_ADAPTER`: keep the TypeScript recorder as the lossless audit layer,
build the schema-v2 → Nautilus converter and lineage/audit sidecars as the durable boundary, and use
Nautilus for replay, backtesting, matching/fill simulation, portfolio accounting, and (later) live
execution. Defer any SDK-based transport replacement to a separate, authenticated live-execution
workstream outside this read-only spike.
