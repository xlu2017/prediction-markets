# Prediction Markets: Multi-Phase Delivery Roadmap

Status: active planning document

Last updated: 2026-08-05

Planning branch: `agent/multi-phase-concurrent-plan`

Recorder implementation branch under review: `devin/recorder-schema-v2`

## 1. Executive decision

This project will keep a custom component only where it provides a clear capability that is not available as a coherent open-source Polymarket package: a lossless, auditable market-data recorder that certifies whether a capture is suitable for downstream replay.

The project will not assume that every later subsystem should be implemented from scratch. Deterministic event processing, catalogs, backtesting, fill simulation, portfolio accounting, reconciliation, paper trading, and live execution already exist in mature open-source systems. Those capabilities must be evaluated and adopted where they satisfy the project's contracts.

The target architecture is therefore:

```text
Gamma + Polymarket CLOB V2 + Chainlink RTDS
                       │
                       ▼
       custom lossless recorder and verifier
               recorder schema v2
                       │
                       ▼
      versioned normalization and lineage adapter
                       │
                       ▼
       selected catalog/replay/trading engine
          preferably NautilusTrader if proven
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
       research     paper/shadow   live later
```

The custom recorder is an evidence boundary, not a replacement for a mature trading platform.

No custom replay engine, matching engine, portfolio ledger, or live order-management system should be approved until the corresponding build-versus-adopt gate has been completed.

## 2. Project purpose

The project aims to produce a research-grade and eventually production-capable environment for Polymarket BTC five-minute Up/Down markets.

The required progression is:

1. trustworthy public-data capture;
2. verified schema and evidence lineage;
3. an explicit build-versus-adopt decision for downstream infrastructure;
4. deterministic offline replay;
5. leakage-safe datasets and settlement labels;
6. calibrated probability forecasts;
7. explicit decision policies with abstention;
8. conservative paper execution and accounting;
9. continuous shadow operation;
10. optional authenticated execution behind independent safety gates.

Infrastructure completion does not establish predictive edge or profitability. Any positive-alpha claim must survive untouched chronological testing, calibration analysis, conservative execution assumptions, operational failure testing, and independent reconciliation.

## 3. Current state

### 3.1 Implemented foundation

The repository contains a read-only TypeScript recorder that uses the current Polymarket architecture:

- Gamma for event discovery, rules, timing, condition identity, outcome labels, and token IDs;
- the public Polymarket CLOB V2 market WebSocket for executable order-book and trade data;
- Polymarket Chainlink RTDS for settlement-aligned BTC/USD reference observations.

The schema-v2 work under review adds or substantially improves:

- consistent Gamma market ID, CLOB condition ID, and outcome-token identity;
- exact raw-frame and raw-message retention;
- array-message indexing and lineage;
- exact decimal lexeme preservation;
- evidence-preserving duplicate classification;
- connection-generation identity;
- explicit gap and stale-feed lifecycle events;
- mandatory post-gap book resynchronization;
- transactional order-book updates;
- final and historical crossed-book detection;
- Chainlink receipt-time and source-time quality checks;
- cumulative capture-quality failures;
- `COMPLETE`, `PARTIAL`, and `UNUSABLE` capture classifications;
- an independent run verifier over `manifest.json`, `health.json`, and `events.jsonl`;
- a read-only public smoke workflow.

### 3.2 Phase-0 status

Recorder schema v2 is close to a freeze but remains under review. It must not be treated as frozen merely because CI and the public smoke workflow pass.

The final schema-freeze review must confirm that every historical Chainlink observation is evaluated using the configured reference-staleness and clock-skew policies, not an unrelated hard-coded threshold. The persisted health summary and the independent verifier must derive the same anomaly result from the underlying events.

Until all Phase-0 blockers are closed:

- PR #2 remains draft;
- downstream work may prototype against the branch;
- downstream contracts must not be declared stable;
- replay or catalog output must be disposable and regenerable.

### 3.3 Meaningful differentiation

The custom implementation is differentiated at the pre-normalization evidence boundary. It combines capabilities that are usually separate or absent in open-source Polymarket projects:

- exact source-frame retention;
- exact per-message text and decimal representations;
- frame, message, sequence, connection, market, condition, and token lineage;
- duplicate evidence retained without duplicate state mutation;
- explicit gap history and resynchronization requirements;
- cumulative book and reference-feed integrity incidents;
- fail-closed capture certification;
- verification of persisted output independently from the recorder process.

This is valuable for corpus integrity, adapter debugging, reproducible microstructure research, and proving that normalized downstream events can be traced to source evidence.

It is not a reason to rebuild the mature replay, backtesting, accounting, risk, and execution functionality available elsewhere.

## 4. Data-source and identity boundaries

### 4.1 Public data roles

The sources have distinct semantics:

| Source | Role | Must not be treated as |
| --- | --- | --- |
| Gamma API | Discovery, contract metadata, timing, rules, market ID, condition ID, token IDs | Executable price feed |
| CLOB V2 market channel | Up/Down snapshots, deltas, trades, liquidity and market events | BTC settlement source |
| Chainlink RTDS | Settlement-aligned BTC/USD reference observations | Executable outcome-token price |
| Polygon/on-chain fills, optional later | Authoritative executed fills and settlement-side evidence | Complete resting CLOB history |

In a CLOB event, the source field named `market` is the condition ID. It must be validated against the discovered condition ID and must not overwrite the normalized Gamma market ID.

### 4.2 Canonical identity

Every downstream record must be able to retain or recover:

```text
run_id
sequence
frame_id
message_index
connection_generation
gamma_market_id
condition_id
outcome_token_id
source_timestamp
received_at
processed_at
raw_event_type
schema_version
code_commit
```

No external SDK object is a stable project contract.

## 5. Operating modes

| Mode | Network input | Authenticated trading | Purpose |
| --- | --- | --- | --- |
| `DATA_ONLY` | Public live data | Forbidden | Lossless capture and validation |
| `REPLAY` | Saved local data only | Forbidden | Deterministic reconstruction and research |
| `PAPER` | Saved or public live data | Forbidden | Decisions and simulated execution |
| `SHADOW` | Public live data plus account-independent simulation | Forbidden | Continuous operational validation |
| `LIVE` | Public and authenticated APIs | Allowed only behind all gates | Optional production execution |

Missing or invalid configuration must never cause an implicit transition to a more permissive mode.

Unit tests, CI, fixtures, documentation examples, and public smoke workflows must remain unable to submit orders.

## 6. Build-versus-adopt allocation

The default allocation is:

| Component | Default decision | Candidate implementation | Reason |
| --- | --- | --- | --- |
| Lossless raw recorder | Keep custom | TypeScript schema-v2 recorder | Clear differentiated capability |
| Capture certification | Keep custom | Health summary plus independent verifier | Required evidence contract |
| Gamma discovery | Wrap official/current interfaces | Local gateway or current official SDK | Avoid volatile API types in domain code |
| CLOB public transport | Keep focused wrapper or official SDK adapter | Current CLOB V2 protocol | Recorder requires raw-wire access |
| Chainlink RTDS transport | Keep reliability wrapper | Official RTDS client may be used internally | Raw retention and stale detection remain local |
| Versioned normalized adapter | Keep custom | Schema-v2 adapter | Required lineage boundary |
| Normalized data catalog | Prefer adoption | Nautilus catalog or equivalent | Mature implementation already exists |
| Deterministic event engine | Prefer adoption | NautilusTrader | Avoid rebuilding clock and event infrastructure |
| Backtester | Prefer adoption | NautilusTrader or proven Polymarket framework | Existing event-driven simulation |
| Matching/fill models | Prefer adoption with project-specific calibration | Nautilus plus conservative local models | Generic engine plus venue assumptions |
| Portfolio accounting | Prefer adoption | NautilusTrader | Mature cash, inventory, order and reconciliation state |
| Paper trading | Prefer adoption | NautilusTrader | Research-to-live parity |
| Live execution | Official SDK or proven adapter behind gateway | Current official unified Polymarket SDK; Nautilus only after comparison | Protocol correctness and isolation |
| Research methodology | Reuse selectively | OpenMarket and other credible research projects | Avoid repeating established synchronization and walk-forward work |
| On-chain executed-fill truth | Optional adoption | Substreams/indexing project | Complements, but does not replace, CLOB capture |

Any deviation requires a written decision record with evidence that the adopted option cannot satisfy the contract.

## 7. Core engineering principles

### 7.1 Raw evidence before derived state

Every external frame must be preserved before normalization, filtering, duplicate classification, or state application. Derived books, normalized events, features, forecasts, decisions, simulated fills, and reports must remain traceable to source evidence.

### 7.2 Stable internal contracts

External API and SDK objects must be converted into versioned project types. Models, policies, reports, and risk code must not depend directly on transport payloads or SDK-native structures.

### 7.3 Determinism

Given identical input bytes, configuration, code versions, dependency versions, and random seed, independent replay processes must produce identical canonical hashes.

### 7.4 Fail closed

Unknown identity, malformed decimals, stale reference data, missing snapshots, unresolved or historical gaps, invalid state transitions, schema disagreement, and reconciliation mismatch must disable downstream use.

### 7.5 Prediction, policy, and execution are separate

A model estimates probabilities. A policy maps probabilities and observable state into an intended action or abstention. The execution layer decides whether and how the action can be simulated or submitted.

No layer may silently absorb the responsibilities of another.

### 7.6 Abstention is first-class

The policy must be able to abstain because of insufficient edge, uncertainty, spread, depth, time remaining, data quality, operational health, or risk limits.

### 7.7 Adopt before rebuilding

Before implementing a generic subsystem, the owner must identify credible existing projects, test them against the required contract, and document the adoption decision.

### 7.8 Independent verification

The component that creates an artifact must not be the only component that declares the artifact valid. Run verification, replay hash comparison, book comparison, and accounting reconciliation must be independently executable.

## 8. Concurrent-session operating model

### 8.1 Unit of work

Each session owns one bounded work package with:

- a unique ID;
- one objective;
- dependencies and base commit;
- allowed paths;
- prohibited scope;
- contracts consumed and changed;
- acceptance criteria;
- tests and evidence;
- a handoff note.

### 8.2 Branch convention

Use:

```text
agent/<phase>-<work-package>-<short-description>
devin/<phase>-<work-package>-<short-description>
```

A branch starts from the latest approved integration point for its phase. It must not assume unmerged sibling changes unless explicitly based on that branch.

### 8.3 Pull-request convention

Every PR must state:

- work-package ID and phase;
- base and head commits;
- dependency list;
- exact scope;
- schema and contract impact;
- fixtures changed;
- commands and test output;
- current limitations;
- authenticated capabilities added or explicitly excluded;
- whether an external project or SDK was evaluated or adopted.

Agent-created PRs remain draft until their dependencies and phase gates pass.

### 8.4 Shared-contract ownership

Changes to the following are serialized:

- recorder envelopes;
- health and manifest schemas;
- normalized adapter schema;
- replay state schema;
- dataset schema;
- forecast schema;
- policy action schema;
- execution and order lifecycle schema.

Downstream work may prototype against a proposed contract, but it must rebase onto the final freeze commit before approval.

### 8.5 Default file ownership

| Area | Paths | Ownership rule |
| --- | --- | --- |
| Recorder contracts | `src/types.ts`, schema docs, fixtures | One contract owner |
| Recorder transport | `src/feeds/**`, `src/gamma.ts` | One feed owner per source |
| Recorder processing | `src/processor.ts`, `src/store.ts`, `src/health.ts`, `src/book.ts` | Coordinate with contract owner |
| Platform evaluation | `docs/open-source-platform-evaluation.md` | Independent research owner |
| Adapter/replay spike | `python/**` or isolated adapter package | No recorder semantic changes |
| Datasets/features | `src/dataset/**`, `python/**/dataset/**` | Consume frozen replay contract |
| Models | isolated model package | No transport or execution calls |
| Policy | policy package | No direct SDK calls |
| Paper/live integration | isolated execution package/service | No model internals |
| Documentation | `docs/**` | Avoid overlapping document edits |

### 8.6 Freeze points

1. recorder schema v2;
2. normalized adapter contract v1;
3. replay/catalog state contract v1;
4. dataset and label schema v1;
5. probability forecast schema v1;
6. policy action schema v1;
7. execution event and accounting schema v1;
8. authenticated order lifecycle schema v1.

## 9. Phase map

| Phase | Outcome | Primary build/adopt decision | Exit gate |
| --- | --- | --- | --- |
| 0 | Lossless recorder and schema-v2 freeze | Keep custom | No known corruption or false-complete path |
| 1 | Continuous public capture operations | Keep custom recorder; reuse transport where safe | Verified rollover and supervised capture |
| 2 | Open-source platform and adapter spike | Decide Nautilus and SDK adoption | Executable `ADOPT`, `ADOPT_WITH_CUSTOM_ADAPTER`, or `DO_NOT_ADOPT` decision |
| 3 | Deterministic normalization, catalog and replay | Prefer selected open-source engine | Independent replay hashes and book comparison |
| 4 | Settlement labels and research dataset | Custom transformations over adopted storage tools | Leakage-safe dataset v1 |
| 5 | Probability models and calibration | Reuse standard ML libraries | Untouched chronological evaluation |
| 6 | Policy and risk decision layer | Keep project-specific | Reproducible actions and abstentions |
| 7 | Paper execution and accounting | Prefer adopted trading engine | Conservative simulator and ledger reconciliation |
| 8 | Continuous shadow operation | Adopt mature operational primitives | Stable multi-day run without authenticated trading |
| 9 | Dormant authenticated adapter | Official SDK or proven engine adapter | All live gates proven; disabled by default |
| 10 | Limited live pilot and hardening | Explicit launch decision | Documented go, revise, or stop decision |

# Phase 0: Freeze the lossless recorder contract

## Objective

Complete and independently verify the narrow component that is meaningfully differentiated: lossless Polymarket capture plus fail-closed run certification.

No production replay, modelling, paper execution, wallet support, or authenticated client work may merge before the recorder contract freezes.

## P0-WP01: Identity and envelope contract

Required outcomes:

- stable Gamma market ID across all sources;
- explicit condition ID;
- separate source-native identifiers;
- condition validation on all supported CLOB events;
- outcome-token identity preservation;
- frame and message indexing;
- exact raw frame and raw message text;
- schema-versioned fixtures and documentation.

## P0-WP02: Message identity and duplicate semantics

Required outcomes:

- every external message is retained;
- duplicate classification is message-level rather than only frame-level;
- a rejected first occurrence cannot poison a later valid retransmission;
- duplicates identify the prior successfully applied sequence;
- confirmed duplicates do not mutate derived state twice;
- duplicate behavior is reproducible from recorded evidence.

## P0-WP03: Feed lifecycle, gaps and resynchronization

Required outcomes:

- socket-open state is distinct from usable-data state;
- stale but open sockets reconnect;
- a gap ends only after validated current-generation data;
- closed and open gaps remain in cumulative health output;
- a CLOB interruption requires new snapshots for both outcome books;
- stale-generation callbacks cannot close a newer gap or overwrite active generation state.

## P0-WP04: Book-health projection

Required outcomes:

- explicit `UNINITIALIZED`, `SYNCHRONIZED`, `STALE`, `INVALID`, and `RESYNC_REQUIRED` states;
- complete snapshot required before deltas;
- validate-before-mutate transactionality;
- exact decimal price and quantity handling;
- unknown token, invalid side, negative size, duplicate level, and invalid price rejection;
- `NORMAL`, `LOCKED`, and `CROSSED` integrity classifications;
- cumulative transient crossed-book incidents;
- current-generation snapshot requirements.

This projection is a recorder-health mechanism. It is not the long-term full trading engine.

## P0-WP05: Chainlink quality semantics

Required outcomes:

- payload timestamp preferred when available;
- top-level and payload timestamps both retained;
- receipt time and source time preserved;
- duplicate reference observations do not refresh final quality;
- missing timestamp, stale source, future source skew, and local receipt-time skew fail closed;
- historical anomalies are cumulative;
- every historical observation uses the configured staleness and skew policy;
- recorder health and independent verification produce the same anomaly result.

## P0-WP06: Capture assessment and independent verifier

Required outcomes:

- process completion is separate from replay usability;
- machine-readable quality-failure codes;
- failed processes cannot be replay-usable;
- malformed events, gaps, stale state, invalid books, crossed incidents, missing snapshots, and reference anomalies prevent `COMPLETE`;
- `events.jsonl` sequence, identity, counts, timestamps, and schema versions are validated;
- manifest and health fields agree;
- final book keys match the discovered Up and Down token IDs;
- reference quality is recomputed from events rather than trusted from self-reported health.

## P0-WP07: Public smoke and freeze evidence

Required evidence:

- current Gamma response shape;
- current CLOB V2 snapshots and updates;
- multiple valid Chainlink observations;
- no authenticated calls;
- no order placement;
- `captureStatus: COMPLETE`;
- `usableForReplay: true`;
- zero quality failures;
- zero malformed events;
- zero open or closed gaps;
- current-generation synchronized books;
- independent verification success;
- retained workflow artifact.

## Phase-0 exit gate

All of the following must be true:

- every open P0 review finding is closed;
- schema v2 and quality-failure meanings are documented;
- fixtures are current and versioned;
- the TypeScript CI and build are green;
- the independent verifier passes a retained live smoke artifact;
- no known path can report `COMPLETE` while a published health field reports invalid or stale data;
- no authenticated dependency, endpoint, credential, or order capability is required;
- a freeze commit is recorded in this roadmap.

# Phase 1: Continuous public capture operations

## Objective

Turn the frozen recorder into a reliable read-only data service without expanding it into a trading engine.

## Work packages

### P1-WP01: Market rollover

- discover the next BTC five-minute market before the current market closes;
- preserve strict run and market boundaries;
- never mix condition or token identities across markets;
- record explicit rollover lifecycle events;
- handle unavailable or ambiguous next markets fail closed.

### P1-WP02: Capture supervisor

- restart policy;
- process health and readiness;
- storage-capacity checks;
- preservation of incomplete runs;
- recovery without overwriting evidence;
- bounded failure escalation.

### P1-WP03: Immutable archival and indexing

- canonical JSONL remains source of truth;
- optional compressed immutable archive;
- checksums in the manifest;
- searchable run index;
- extraction and checksum reproduction tests.

### P1-WP04: Coverage and operations report

- captured markets versus expected markets;
- complete versus partial versus unusable counts;
- gaps, stale incidents, malformed events, and crossed incidents;
- Chainlink observation coverage;
- storage growth and retention projections.

## Phase-1 exit gate

- supervised multi-market capture succeeds;
- rollover identity remains correct;
- incomplete captures remain preserved and classified;
- archives reproduce canonical checksums;
- no authenticated interface is used.

# Phase 2: Open-source platform and adapter spike

## Objective

Prove which downstream capabilities should be adopted rather than built.

This phase is mandatory. A generic custom replay, matching, accounting, or live engine may not proceed around it.

## P2-WP01: Platform evaluation

Evaluate current maintained versions and licenses of:

- NautilusTrader and its Polymarket integration;
- current official unified Polymarket SDKs;
- official RTDS tooling;
- OpenMarket as a research and data-methodology reference;
- credible Polymarket backtesting projects;
- optional on-chain fill indexing projects.

Record versions, commits, licenses, maintenance status, supported APIs, raw-data access, catalog support, replay support, execution support, limitations, and operational risks.

Deliver:

```text
docs/open-source-platform-evaluation.md
```

## P2-WP02: Versioned adapter contract

Define normalized records independent of any engine-specific classes:

```text
NormalizedInstrument
NormalizedBookSnapshot
NormalizedBookDelta
NormalizedTrade
NormalizedReferencePrice
NormalizedLifecycleEvent
NormalizedAuditEvent
```

Every record retains source-envelope lineage.

Deliver:

```text
docs/nautilus-adapter-contract.md
```

The file name may be generalized later if Nautilus is rejected, but the contract must remain engine-independent.

## P2-WP03: Catalog conversion proof

- read only verified schema-v2 runs;
- convert snapshots, deltas, trades, metadata, Chainlink reference prices, and lifecycle boundaries;
- retain unsupported and audit-only events in a sidecar stream;
- preserve decimal precision;
- create a local catalog using the candidate engine's standard format;
- report every converted, audit-only, ignored, and failed event.

## P2-WP04: Deterministic replay proof

Replay the same catalog twice in independent processes and compare hashes for:

- normalized inputs;
- Up book evolution;
- Down book evolution;
- final books;
- reference-price stream;
- strategy-visible stream;
- deterministic no-order or intent-only strategy output.

## P2-WP05: Cross-engine and recorder comparison

Compare the selected engine's reconstructed final state against an independent reconstruction from schema-v2 evidence.

At minimum compare:

- token identity;
- best bid and ask;
- bid and ask depth counts;
- final integrity;
- last applicable source sequence;
- complete depth when exposed by both sides.

## P2-WP06: Adoption decision

Deliver:

```text
docs/nautilus-adoption-decision.md
```

Use exactly one conclusion:

```text
ADOPT
ADOPT_WITH_CUSTOM_ADAPTER
DO_NOT_ADOPT
```

A `DO_NOT_ADOPT` conclusion must identify the concrete contract failures that justify custom implementation.

## Phase-2 exit gate

- an executable conversion exists;
- source lineage is preserved;
- Chainlink remains semantically distinct from trades and quotes;
- two independent replays produce identical hashes;
- final-book differences are resolved or documented as blockers;
- CI runs the spike;
- a formal adoption decision is approved.

# Phase 3: Deterministic normalization, catalog and replay

## Objective

Productionize the selected architecture from Phase 2.

## Rules

- Keep schema-v2 run directories immutable.
- Persist an engine-independent normalized contract before engine-native serialization.
- Do not make Nautilus or another framework's Python classes the only durable representation.
- Do not perform network I/O during replay.
- Do not discard unsupported source events silently.
- Maintain exact lineage from normalized records to recorder sequence and message identity.

## Work packages

### P3-WP01: Cross-language schema governance

Choose and implement a single versioned validation mechanism, such as JSON Schema generated from source definitions, that can be consumed by TypeScript and Python without divergent handwritten semantics.

### P3-WP02: Production converter

- streaming input;
- bounded memory;
- deterministic sorting and tie rules;
- explicit duplicate handling;
- lifecycle and gap boundaries;
- conversion reports and checksums;
- catalog round-trip tests.

### P3-WP03: Reference-price custom data

Represent Chainlink observations as a dedicated data type. Do not misrepresent them as Polymarket trades, token quotes, marks, or fills.

### P3-WP04: Canonical market-state output

Expose a versioned engine-independent state containing:

- run and source sequence;
- market and condition identity;
- event and market-relative time;
- opening and current reference price;
- Up and Down book state;
- gap and data-quality state;
- lineage pointers.

### P3-WP05: Replay diagnostics

- input and output hashes;
- unavailable periods;
- gaps and resynchronizations;
- crossed and locked states;
- reference age and anomalies;
- unsupported events;
- catalog coverage;
- final-book comparison.

## Phase-3 exit gate

- no replay command uses external network access;
- independent processes produce identical hashes;
- every normalized event has source lineage;
- catalog serialization preserves required precision;
- final state agrees with the recorder or an approved independent reconstruction;
- unsupported events are explicit.

# Phase 4: Settlement labels and research dataset

## Objective

Convert deterministic replay state into auditable, leakage-safe research data.

## Work packages

### P4-WP01: Opening and closing reference selection

- formalize contract boundary rules;
- preserve surrounding Chainlink observations;
- distinguish observed values from approximations;
- quarantine markets without defensible boundary values.

### P4-WP02: Official outcome and resolution

- persist official resolution evidence;
- define Up and Down exactly as the contract specifies;
- reconcile official outcome with reconstructed reference boundaries;
- quarantine disagreements.

### P4-WP03: Feature windows

- use only information available at the decision timestamp;
- support multiple decision horizons;
- preserve latest source sequence and source timestamp;
- prevent post-decision and settlement leakage.

### P4-WP04: Dataset storage

- Parquet for bulk research data;
- DuckDB or equivalent for local analysis;
- schema and transformation versions;
- links back to run, event, frame, and raw evidence.

### P4-WP05: Optional on-chain fill evidence

Use an on-chain indexing source to validate executed-trade history where useful. Treat it as complementary to, not a replacement for, off-chain CLOB evidence.

### P4-WP06: Dataset audit

- missingness;
- feed age;
- spread and depth distributions;
- volatility regimes;
- class balance;
- unusable-market reasons;
- split overlap;
- leakage tests.

## Phase-4 exit gate

- every row traces to replay and raw evidence;
- chronological splits are explicit;
- outcomes reconcile with official resolution;
- no future feature is present;
- unusable markets remain visible in audit output and excluded by predefined rules.

# Phase 5: Probability models and calibration

## Objective

Estimate calibrated outcome probabilities rather than only classifications.

## Forecast contract

A versioned forecast must include:

```text
market_id
decision_sequence
generated_at
feature_timestamp
model_name
model_version
p_up
uncertainty
calibration_version
dataset_version
code_commit
diagnostics
```

## Work packages

### P5-WP01: Baselines

- 50% baseline;
- clearly labelled market-price baseline;
- time-and-distance analytical baseline where justified;
- no treatment of market price as outcome ground truth.

### P5-WP02: Transparent statistical model

- logistic regression or similar interpretable baseline;
- regularization without test-set reuse;
- walk-forward estimation;
- coefficient and stability reports.

### P5-WP03: Bounded nonlinear model

- one justified nonlinear baseline;
- controlled hyperparameter search;
- chronological validation;
- feature-importance and regime stability.

### P5-WP04: Calibration

- reliability diagrams;
- Brier score;
- log loss;
- expected calibration error;
- calibration by time remaining and volatility regime;
- calibration fit only on designated periods.

### P5-WP05: Model registry

- model and dataset checksums;
- feature schema;
- training and evaluation periods;
- code commit;
- dependency versions;
- random seed;
- rejected versions and reasons.

## Phase-5 exit gate

- final chronological periods remain untouched until evaluation;
- forecasts remain bounded;
- calibration and directional discrimination are reported separately;
- market-based baselines are included;
- no profitability claim is made without execution simulation.

# Phase 6: Policy and risk decision layer

## Objective

Convert forecasts and observable state into auditable intended actions under explicit objectives and constraints.

## Required action types

```text
ABSTAIN
PLACE_ORDER_INTENT
CANCEL_ORDER_INTENT
CANCEL_ALL_INTENT
```

The policy does not call a Polymarket SDK or execution client.

## Work packages

### P6-WP01: Expected-value primitives

- executable depth-weighted price;
- fees;
- spread and slippage buffer;
- uncertainty-adjusted probability;
- all-in break-even probability;
- exact decimal arithmetic.

### P6-WP02: Reference abstaining policy

- buy only above conservative all-in break-even;
- abstain otherwise;
- reject stale, gapped, invalid, incomplete, or low-depth state;
- enforce time-remaining restrictions.

### P6-WP03: Sizing and portfolio constraints

- no-position baseline;
- fixed-notional baseline;
- optional capped fractional Kelly for research only;
- maximum order, market, session, and total exposure;
- drawdown and loss limits.

### P6-WP04: Explanation record

Persist the forecast, uncertainty, book levels, fees, slippage, expected value, risk checks, final action, and reason codes.

### P6-WP05: Policy evaluation

- action and abstention rates;
- edge distribution;
- parameter sensitivity;
- regime stability;
- data-quality exclusion rates.

## Phase-6 exit gate

- every action is reproducible;
- abstention is tested;
- no future state is consumed;
- executable depth is used;
- the policy has no network or SDK dependency;
- risk checks cannot be bypassed by order splitting or repeated intents.

# Phase 7: Paper execution and accounting

## Objective

Use the selected mature trading engine where possible to simulate orders, fills, positions, fees, and settlement under conservative Polymarket-specific assumptions.

## Work packages

### P7-WP01: Engine integration

- map policy intents into engine-native orders;
- preserve intent and source lineage;
- prohibit authenticated clients;
- maintain deterministic clocks and seeds.

### P7-WP02: Aggressive-order simulation

- walk contemporaneous executable depth;
- implement FAK partial fills;
- enforce FOK full-or-zero;
- apply fee configuration;
- model latency and unavailable depth conservatively.

### P7-WP03: Passive-order assumption bands

Because exact queue position is generally unavailable:

- conservative lower-bound model;
- evidence-based intermediate model;
- optimistic upper bound;
- explicit labels on every report.

### P7-WP04: Portfolio and settlement

- cash;
- inventory;
- reserved capital;
- average cost;
- realized and unrealized P&L;
- fees;
- settlement and void handling;
- accounting identities.

### P7-WP05: Independent comparison

Compare selected fixtures against a second implementation or a narrow independent checker. Discrepancies in fills, cash, inventory, fees, or settlement block conclusions.

### P7-WP06: Paper report

- gross and net P&L;
- fees and slippage;
- exposure and drawdown;
- capital at risk;
- results by time remaining, spread, depth, and regime;
- maker-assumption sensitivity;
- forecast metrics separated from trading metrics.

## Phase-7 exit gate

- deterministic fill fixtures pass;
- FOK behavior is correct;
- accounting reconciles;
- settlement is correct;
- conservative assumptions are primary;
- independent discrepancies are resolved;
- no authenticated endpoint is required.

# Phase 8: Continuous shadow operation

## Objective

Run capture, normalization, forecasting, policy, and paper execution continuously without authenticated trading.

## Work packages

### P8-WP01: Orchestration

- market rollover;
- restart recovery;
- one active decision stream;
- idempotent decisions;
- graceful shutdown;
- process supervision.

### P8-WP02: Operational storage

- run and incident metadata;
- forecasts and policy decisions;
- simulated orders and fills;
- positions and reports;
- bulk event/catalog retention;
- migration, backup, and restore tests.

### P8-WP03: Observability

- feed age and event lag;
- reconnects and gaps;
- book and reference anomalies;
- forecast and decision latency;
- simulated order state;
- current risk state;
- capture and replay coverage.

### P8-WP04: Long-running failure tests

- forced disconnects;
- slow or failed storage;
- malformed frames;
- unavailable Gamma;
- stale Chainlink;
- process restart during a market;
- rollover races;
- disk exhaustion;
- catalog corruption.

### P8-WP05: Daily reports

- market coverage and exclusions;
- calibration;
- decisions and abstentions;
- simulated fills;
- P&L and drawdown;
- assumption sensitivity;
- operational incidents.

## Phase-8 exit gate

- stable multi-day operation;
- no silent gaps;
- restart recovery demonstrated;
- reports reconcile to stored artifacts;
- kill and shutdown procedures work;
- no wallet or authenticated endpoint is needed.

# Phase 9: Dormant authenticated adapter

## Objective

Implement an authenticated boundary using the current official Polymarket SDK or a proven adopted engine adapter, but keep it disabled by default.

## Work packages

### P9-WP01: Gateway and dependency pinning

- pin exact versions;
- isolate SDK-native types;
- define supported wallet and order configurations;
- add contract tests and upgrade procedure;
- compare official SDK and engine-adapter behavior.

### P9-WP02: Secrets and identity

- isolated secret namespace;
- no secrets in logs, fixtures, reports, or CI;
- explicit wallet identity;
- fail closed on mismatch.

### P9-WP03: Order lifecycle and reconciliation

- submit, acknowledge, reject, partially fill, fill, cancel, and expire;
- unique local correlation IDs;
- ambiguous-submission reconciliation before retry;
- user-stream and REST reconciliation;
- stop on mismatch.

### P9-WP04: Heartbeat and emergency controls

- required heartbeat mechanism;
- cancel-all;
- cancel on shutdown, stale data, auth failure, or reconciliation failure;
- local and remote kill switches.

### P9-WP05: Compliance and availability gate

- current official availability or geoblock check;
- no evasion of restrictions;
- disable submission when unavailable;
- retain a non-sensitive gate result.

## Mandatory live gates

```text
MODE=LIVE
LIVE_TRADING_ENABLED=true
explicit command-line confirmation
valid credentials and expected wallet identity
successful compliance/availability check
healthy CLOB stream
healthy Chainlink stream
healthy local clock
validated market specification
successful account reconciliation
nonzero configured limits
kill switch inactive
```

No test, CI job, fixture, migration, startup probe, or documentation example may satisfy all gates.

## Phase-9 exit gate

- authenticated behavior is verified with mocks or approved controlled environments;
- reconciliation is deterministic under fixtures;
- live mode remains disabled by default;
- CI cannot access live secrets;
- no live order is required.

# Phase 10: Limited live pilot and hardening

## Objective

Only after a separate explicit decision, run a tightly limited pilot and determine whether the system is economically, operationally, and legally viable.

## Prerequisites

- legal, compliance, and geographic eligibility reviewed;
- continuous paper results approved;
- conservative assumptions remain acceptable;
- capital and loss limits approved;
- incident owner and shutdown procedure assigned;
- live adapter independently reviewed;
- official SDK and selected engine agree on controlled fixtures.

## Pilot sequence

1. read-only account reconciliation;
2. cancellation and heartbeat validation without exposure where possible;
3. smallest permitted order notional;
4. one market at a time;
5. taker orders before maker assumptions;
6. manual observation of every lifecycle event;
7. automatic stop after predefined count, loss, or inconsistency;
8. post-pilot reconciliation against platform records.

## Production hardening

- deployment isolation;
- secret rotation;
- backup and disaster recovery;
- incident drills;
- dependency and SDK upgrade tests;
- audit logs and dashboards;
- release and rollback procedures;
- capital scaling based on realized slippage and drawdown, not win rate.

## Phase-10 exit gate

There is no automatic promotion. Produce a documented `GO`, `REVISE`, or `STOP` decision based on realized net results, execution quality, calibration stability, drawdown, operational incidents, compliance status, and evidence that results are not caused by optimistic simulation assumptions.

# 10. Shared testing strategy

## Unit and contract tests

- schema validation and migration;
- identity mapping;
- raw-frame and array-message splitting;
- exact decimal handling;
- duplicate registration timing;
- feed lifecycle and gap state;
- book transactionality and integrity;
- reference timestamp policies;
- run verification;
- adapter lineage;
- catalog serialization;
- replay ordering and hashing;
- feature timestamp boundaries;
- forecast bounds;
- expected-value calculations;
- policy abstention;
- risk checks;
- fill mechanics;
- accounting and settlement.

## Property-based and invariant tests

Where practical:

- rejected events leave no partial mutation;
- duplicate processing creates no duplicate state mutation or fill;
- valid book changes never create negative depth;
- replay is deterministic;
- source lineage is never lost;
- FOK is complete or zero;
- cash and inventory balance;
- probabilities remain bounded;
- risk limits cannot be bypassed by splitting actions.

## Integration tests

- current Gamma and CLOB fixture shapes;
- multiple messages per frame;
- disconnect and reconnect;
- open-but-silent sockets;
- old-generation callbacks;
- stale and malformed reference events;
- market rollover;
- interrupted storage;
- catalog conversion and round trip;
- two-process replay;
- engine-versus-recorder book comparison;
- settlement;
- ambiguous authenticated submission only in Phase 9 fixtures.

## Golden fixtures

Maintain immutable versioned fixtures for:

- valid public capture;
- duplicate transport delivery;
- failed first occurrence and valid retransmission;
- delta before snapshot;
- CLOB resynchronization;
- crossed and locked books;
- malformed decimal;
- stale, missing, or future-skewed reference timestamp;
- open and closed gaps;
- market rollover;
- official resolution disagreement;
- aggressive full and partial fills;
- FOK rejection;
- passive-fill sensitivity.

Fixtures carry schema versions and checksums.

# 11. Data and schema governance

A field-meaning change requires a new schema version. An optional addition is compatible only when existing consumers have defined behavior for its absence.

Every schema change requires:

- migration or explicit non-migratable declaration;
- fixture updates;
- reader compatibility tests;
- documentation;
- downstream impact list;
- handoff note.

Derived records should retain:

- run, market, condition, token, frame, and source sequence identity;
- replay and normalized sequence;
- dataset and feature versions;
- model and calibration versions;
- policy version;
- fee and simulator versions;
- dependency versions;
- code commit.

Use UTC for recorded wall time, monotonic clocks for elapsed local durations, exact source timestamps as provided, signed clock differences, and explicit market-relative time. Local sequence remains the canonical recorder receipt order.

# 12. Dependency policy

- Use only current CLOB V2-compatible interfaces.
- Do not adopt archived CLOB V1 clients.
- Pin exact SDK and engine versions and commit lock files.
- Record licenses and distribution implications.
- Isolate external types behind local interfaces.
- Add upgrade contract tests.
- Prefer official SDKs for authenticated protocol behavior.
- Retain local reliability, raw-evidence, and verification controls even when using official clients.
- Treat OpenMarket and similar projects as methodology and reusable-code references, not unquestioned production executors.
- Treat NautilusTrader as the leading downstream candidate, subject to the Phase-2 proof.
- Do not adopt projects whose central claim is guaranteed profit, whose order model ignores executable prices and fees, or whose credentials and safety boundaries are inadequate.

# 13. Security and safety boundaries

Before Phase 9:

- no private keys;
- no API credentials capable of trading;
- no mutating Polymarket requests;
- no authenticated user channel;
- no order-signing dependency required by normal CI or research commands;
- no CI secret capable of placing an order.

The read-only source scan is a regression guard, not a complete proof. Runtime network boundaries, dependency review, isolated credentials, and explicit mode gates must enforce safety.

# 14. Research validity checklist

No performance result is promotable unless:

- evaluation is chronological;
- the final test period was untouched;
- every feature was available at decision time;
- executable bids, asks, and depth were used;
- fees, slippage, latency, partial fills, and missed fills were included;
- maker assumptions were labeled and stressed;
- invalid and incomplete captures were excluded by predefined rules;
- calibration was evaluated separately from accuracy;
- forecast quality was separated from P&L;
- parameter and regime sensitivity were tested;
- every simulated or live trade can be reproduced from source evidence;
- engine and independent accounting agree.

# 15. Immediate execution order

1. Close every remaining P0 review finding on `devin/recorder-schema-v2`.
2. Run CI, build, and a retained verified public smoke workflow.
3. Record and approve the recorder schema-v2 freeze commit.
4. Merge the recorder correction PR without adding replay or authenticated scope.
5. Start Phase-1 capture operations and the Phase-2 open-source platform spike from the freeze commit. They may proceed concurrently because the spike consumes but does not mutate recorder semantics.
6. Do not build a generic custom replay, backtester, matching engine, portfolio ledger, or live order manager before the Phase-2 adoption decision.
7. Productionize the selected adapter and replay architecture in Phase 3.
8. Begin dataset, model, policy, and execution phases only from their respective frozen contracts.

# 16. Definition of project success

The project is successful when it can make a defensible statement of this form:

> Given a verified versioned raw capture, the system can trace every normalized event to source evidence, deterministically reconstruct observable market and reference state, produce a timestamp-valid calibrated probability forecast, apply an explicit auditable policy, simulate or execute an order under documented assumptions and risk limits, and reconcile the resulting position and settlement.

This definition does not require profitability. A negative result produced from reliable evidence, mature reused infrastructure, realistic execution, and reproducible analysis is a valid research outcome.
