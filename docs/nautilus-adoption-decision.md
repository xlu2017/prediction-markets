# Nautilus adoption decision

Phase 11 deliverable. This document records the decision on whether to adopt NautilusTrader for
deterministic replay, backtesting, paper trading, portfolio accounting, and eventual execution,
while keeping the TypeScript recorder as the authoritative lossless acquisition and audit layer.

## Conclusion

**`EVALUATION_INCOMPLETE`**

The initial spike demonstrated that NautilusTrader can replay schema-v2 recordings deterministically
and that a custom adapter can preserve source-envelope lineage. However, a subsequent review
identified critical shortcomings (P0) and important gaps (P1) that have been addressed in this
revision but require further validation before a final adoption decision:

### P0 findings (addressed)

1. **Replay sequence preservation**: Replay orders by `(sourceSequence, childIndex)` embedded in
   each delta's `sequence` field (`sourceSequence * 1_000_000 + childIndex`). The `ts_event` field
   uses a **monotonic replay clock** derived from `(sourceSequence, childIndex)` — not wall-clock
   time — so the BacktestNode delivers events in recorder order even when `sourceTimestamp`
   regresses or ties. Source, receipt, and processing timestamps are preserved separately in the
   `Lineage` sidecar. A dedicated test with regressing source timestamps proves this.
2. **Multi-token price_change**: The converter resolves the instrument per-change using
   `price_changes[i].asset_id` when the envelope `tokenId` is null. The lineage sidecar records
   `resolvedTokenId` for each row.
3. **BacktestNode proof**: A real `BacktestNode` runs against the converted catalog with a strategy
   that receives book events, submits a crossing market order at the best ask, and registers a fill
   (`OrderFilled` event). The strategy records the `sequence` field of each delta to prove recorder
   ordering. **Chainlink reference prices are delivered through the Nautilus event engine** via
   `BacktestDataConfig` with `client_id="CHAINLINK"` and `subscribe_data()` — not an out-of-band
   catalog query. The proof asserts: 3 book events, 1 reference event, 1 order submitted, 1 fill,
   cash debited to `999_994.800000 pUSD` (10 × 0.52), and a `LONG 10.000000` position.
4. **Reader invariant cross-checks**: The reader independently recomputes `eventCountsBySource`,
   `eventCountsByType` (only `UNIQUE_APPLIED` events), reference anomalies, and reconstructs final
   book state from the event stream to compare with `finalBookHealth`. Unknown book sides are
   rejected, `Decimal` ordering is used for price comparison, and event-count maps require exact
   equality (all keys checked, not just recomputed ones).
5. **Lineage one-to-one with catalog rows**: Each expanded catalog row has its own lineage row with
   a distinct `childIndex` and a **genuinely unique** `catalogRecordId`
   (`runId:sourceSequence:childIndex:tokenId`). UP and DOWN instruments from the same
   market-discovery event get distinct IDs via the token ID. `TradeId` includes `childIndex` to
   avoid duplicate IDs for batched trades. `ReferencePriceUpdate` and `LifecycleEvent` carry
   `sourceSequence` and `childIndex`.
6. **Canonical input hash**: The input stream hash includes book deltas, trades, reference
   observations, lifecycle events (with full typed payload), **and audit events** (duplicates, empty
   frames, unsupported events) — all ordered by `(sourceSequence, childIndex)`.
7. **Real recorder lifecycle names**: The adapter recognizes `CONNECTION_ATTEMPT`,
   `CONNECTION_OPEN`, `SUBSCRIPTION_SENT`, `CONNECTION_ERROR`, `CONNECTION_CLOSED`,
   `DATA_GAP_STARTED`, `DATA_GAP_ENDED`, `RECONNECT_SCHEDULED`, `STALE_STREAM`, and
   `BOOK_RESYNC_REQUIRED` — the actual event names emitted by `managed-websocket.ts`.

### P1 findings (addressed)

1. **Python CI**: `uv sync --frozen --extra dev` installs dev dependencies. The CLI entry point is
   in a separate `__main_backtest.py` module to avoid the `__main__` / importable module
   dual-instance problem with `ProofStrategy` class variables.
2. **Decimal guarantees**: Comparisons use `Decimal`, JSON float fallback uses
   `parse_float=Decimal`, and precision loss aborts conversion (sub-tick prices are not silently
   quantized).
3. **Audit classification**: `RECEIVED_NOT_APPLIED` is no longer classified as a duplicate — only
   `DUPLICATE_APPLIED` is.
4. **Missing reference value**: The converter **aborts with a `ValueError`** instead of routing to
   audit or substituting `"0"` when a reference value is missing.
5. **Unknown book sides**: Invalid sides are rejected with a `ValueError` instead of defaulting to
   `SELL`.
6. **sourceEventCount**: Counts unique source envelopes, not expanded normalized records.
7. **Realistic fixture**: A 10-event fixture with snapshots, deltas, trades, multi-token
   price_changes, lifecycle events, and same-time conflicts. A separate regressing-timestamp fixture
   proves recorder-order delivery.
8. **Typed lifecycle payload**: `LifecycleEvent` preserves the full typed payload (feed name, close
   code, gap duration, etc.) as `payloadJson`, not just `dedupeReason`.

### Remaining work before final decision

- Validate the adapter against real (non-synthetic) schema-v2 recordings.
- Performance test with larger event streams.

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
    Nautilus releases and re-running the 32 edge-case tests. No trading/execution code is maintained
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
- 32 edge-case tests pass (20 required cases + reference-price queryability + realistic fixture
  suite + BacktestNode proof + regressing-timestamp proof + missing-reference-value abort + reader
  invariant rejection tests).
- BacktestNode proof: 3 book events, 1 Chainlink reference event (via event engine), 1 market order,
  1 fill, cash debited to `999_994.800000 pUSD`, `LONG 10.000000` position, sequences in recorder
  order.
- No authenticated endpoint called; no order placed to a live exchange.

## Recommendation

The evaluation remains **incomplete**. Nautilus demonstrates the required catalog, custom-data,
backtest, matching, fill, and accounting primitives for the synthetic proof, but several blockers
must be resolved before a final adoption decision:

- The adapter currently routes `RECEIVED_NOT_APPLIED` envelopes to the Nautilus engine; they must
  remain audit-only.
- The Python contract mirror is behind the latest recorder PR (#2); conversion must run the
  authoritative TypeScript verifier before consuming a run.
- The adapter has not yet been validated against a real (non-synthetic) verified recording.
- The remaining P1 findings (input-hash completeness, catalog-record universality) need resolution.

Keep the TypeScript recorder as the authoritative lossless audit layer. Continue the Nautilus
adapter work only after rebasing onto the current recorder contract and validating one real
recording end-to-end.
