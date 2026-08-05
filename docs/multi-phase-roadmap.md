# Prediction Markets: Multi-Phase Delivery Roadmap

Status: proposed

Last updated: 2026-08-05

Repository baseline: `main` at `17b5453e12065be17e8c766434a88cf54211f371`

## 1. Purpose

This document defines the delivery plan for a research-grade and eventually production-capable environment for Polymarket BTC five-minute Up/Down markets.

The plan is designed for multiple ChatGPT, Devin, Codex, or human engineering sessions to work concurrently without silently changing shared contracts, duplicating work, or invalidating recorded data.

The target progression is:

1. trustworthy public data capture;
2. deterministic offline replay;
3. research-ready datasets and labels;
4. calibrated probability models;
5. explicit decision policies;
6. realistic paper execution;
7. continuous shadow operation;
8. optional live execution behind independent safety gates.

The project must not claim that a profitable or "winning" policy exists merely because the infrastructure is complete. Profitability is an empirical result that must survive untouched chronological testing and conservative execution assumptions.

## 2. Current baseline

The repository currently contains a read-only TypeScript recorder that:

- discovers an active BTC five-minute Up/Down market through public Gamma interfaces;
- validates the event and extracts Up and Down token IDs;
- subscribes to the public Polymarket CLOB market WebSocket;
- subscribes to the public Chainlink BTC/USD real-time stream;
- records normalized envelopes and original WebSocket text in JSONL;
- maintains a minimal health-only order-book projection;
- emits manifest and health files;
- includes mocked tests, a synthetic fixture, CI, and a manual public smoke workflow.

This is a useful foundation, but deterministic replay must not be built on top of schema version 1 until Phase 0 repairs the known identity, deduplication, gap, book-state, and capture-quality defects described below.

## 3. System boundaries

### 3.1 Data sources

The system uses three distinct public-data roles:

- **Gamma API:** event discovery, contract metadata, rules, timing, condition identity, outcome labels, and outcome-token IDs.
- **Polymarket CLOB V2 market channel:** executable bids and asks, level-two snapshots and updates, trades, tick-size changes, and market lifecycle events.
- **Polymarket Chainlink RTDS:** settlement-aligned BTC/USD observations when required by the contract rules.

These roles must remain separate. Chainlink prices must not be treated as executable contract prices, and CLOB token prices must not be treated as the BTC settlement source.

### 3.2 Operating modes

The long-term system will support four explicit modes:

| Mode | External data | Authenticated trading | Purpose |
| --- | --- | --- | --- |
| `DATA_ONLY` | Public live data | Forbidden | Capture and validation |
| `REPLAY` | Saved local data only | Forbidden | Deterministic research and simulation |
| `PAPER` | Public live data | Forbidden | Continuous shadow decisions and simulated execution |
| `LIVE` | Public and authenticated APIs | Allowed only behind gates | Optional production execution |

Missing or invalid configuration must never cause an implicit transition to a more permissive mode.

### 3.3 Explicit non-goals before the applicable phase

Until the roadmap reaches the corresponding phase, do not add:

- wallet or private-key handling;
- authenticated order submission;
- live positions or capital exposure;
- claims of predictive edge;
- claims of profitable execution;
- a strategy forced to trade every market;
- reinforcement learning merely because the word "policy" is used;
- maker fills that assume favorable queue priority without evidence;
- backtests using midpoint prices as executable prices;
- models trained with future information or settlement leakage.

## 4. Core engineering principles

### 4.1 Raw evidence before derived state

Every external frame must be preserved before normalization, deduplication, filtering, or state application. Derived order books, features, forecasts, decisions, fills, and reports must be reproducible from retained evidence.

### 4.2 Stable internal contracts

External API objects must be converted into versioned internal types. Model, policy, and execution code must not depend directly on volatile SDK or WebSocket payload shapes.

### 4.3 Determinism

Given identical input bytes, configuration, code version, and random seed, replay must produce byte-identical canonical output.

### 4.4 Fail closed

Ambiguous market rules, unknown token identities, stale feeds, uninitialized books, sequence defects, unresolved gaps, invalid decimals, and inconsistent settlement data must disable downstream use rather than trigger a guess.

### 4.5 Prediction is separate from policy

A probability model estimates an outcome distribution. A policy maps state, probabilities, executable prices, portfolio state, and risk constraints into actions such as buy, sell, cancel, or abstain.

### 4.6 Execution is separate from policy

The policy proposes an intended action. The execution layer decides whether and how that action can be represented, submitted, filled, reconciled, cancelled, or rejected under the selected operating mode.

### 4.7 Abstention is first-class

A policy must be allowed to do nothing when estimated edge is inadequate, data quality is insufficient, uncertainty is too high, or risk limits would be violated.

### 4.8 Research and live parity without shared failure domains

Replay, paper, and live modes should share domain interfaces and decision logic, but live credentials and authenticated adapters must remain isolated so tests and research commands cannot reach them.

## 5. Concurrent-session operating model

## 5.1 Unit of work

Each concurrent session must own one bounded work package with:

- a unique work-package ID;
- one objective;
- explicit dependencies;
- an allowed file set or subsystem boundary;
- acceptance criteria;
- tests it owns;
- contracts it consumes;
- contracts it proposes to change;
- a handoff note.

A session must not expand into a neighboring work package without updating the plan or recording the new dependency.

## 5.2 Branch convention

Use:

```text
agent/<phase>-<work-package>-<short-description>
```

Examples:

```text
agent/p0-wp01-envelope-identity
agent/p1-wp03-public-smoke-fixture
agent/p2-wp02-book-replay
agent/p4-wp04-calibration-report
```

Every branch starts from the latest approved integration point for its phase, not an arbitrary sibling branch.

## 5.3 Pull request convention

Each PR must include:

- work-package ID;
- phase and dependency list;
- exact scope;
- contracts changed;
- schema-version impact;
- fixtures added or migrated;
- commands and tests run;
- unresolved limitations;
- confirmation that forbidden capabilities were not added.

Default all agent-created PRs to draft until their dependencies are merged and their acceptance criteria pass.

## 5.4 File ownership

Concurrent sessions should avoid overlapping files. The following ownership model is the default:

| Area | Primary paths | Notes |
| --- | --- | --- |
| External discovery | `src/gamma.ts`, market discovery tests | One owner per phase |
| Domain contracts | `src/types.ts`, schema docs, fixture migrations | Serialized work only |
| Feed transport | `src/feeds/**`, reliability tests | May run parallel to replay after contracts freeze |
| Recording pipeline | `src/processor.ts`, `src/store.ts`, health | Coordinate with schema owner |
| Book state | `src/book.ts`, later `src/replay/book/**` | One state-machine owner |
| Replay | later `src/replay/**` | Must consume frozen recorder schema |
| Research data | later `src/dataset/**`, `src/features/**` | No recorder mutation |
| Models | later `src/models/**` or separate Python package | No execution mutation |
| Policy | later `src/policy/**` | No direct SDK calls |
| Paper execution | later `src/execution/paper/**` | No model internals |
| Live adapter | later `src/execution/live/**` | Isolated and gated |
| Reports | later `src/reporting/**` | Consume versioned outputs |
| Documentation | `docs/**` | Avoid editing the same document concurrently |

Changes to shared contract files are merge bottlenecks and must be serialized through a designated contract-owner PR.

## 5.5 Contract freeze points

Each phase has a contract freeze. Downstream sessions may begin from the freeze commit, but they must not merge until the corresponding phase gate passes.

Freeze points:

1. recorder schema v2;
2. replay state schema v1;
3. research dataset schema v1;
4. forecast schema v1;
5. policy action schema v1;
6. execution event schema v1;
7. live-order lifecycle schema v1.

## 5.6 Handoff template

Every session must add or update a handoff note containing:

```markdown
## Work package

- ID:
- Branch:
- Base commit:
- Objective:

## Completed

- ...

## Contracts consumed

- ...

## Contracts changed

- ...

## Validation

- command: result

## Known limitations

- ...

## Safe next work

- ...

## Work that must wait

- ...
```

## 5.7 Merge order

Within a phase, merge in this order unless the phase specifies otherwise:

1. contract and schema changes;
2. fixture migrations;
3. core implementation;
4. integration adapters;
5. reporting and documentation;
6. acceptance and smoke evidence.

A downstream implementation must rebase or merge the final contract-owner commit before approval.

## 6. Phase map

| Phase | Outcome | Parallelism | Exit gate |
| --- | --- | --- | --- |
| 0 | Recorder correctness and schema v2 | Moderate | No known corruption paths; tests pass |
| 1 | Real public compatibility and capture operations | High after v2 freeze | Valid real capture artifact |
| 2 | Deterministic replay and full book reconstruction | High by subsystem | Byte-deterministic replay |
| 3 | Research dataset and settlement labels | High | Leakage-safe dataset v1 |
| 4 | Probability baselines and calibration | High by model | Untouched chronological evaluation |
| 5 | Policy engine and risk-neutral reference policy | Moderate | Reproducible action decisions |
| 6 | Paper execution and accounting | High by order type/reporting | Conservative simulator reconciliation |
| 7 | Continuous shadow operation | High by operations/reporting | Stable multi-day paper run |
| 8 | Dormant authenticated live adapter | Low to moderate | All gates proven; no live order required |
| 9 | Limited live pilot and production hardening | Serialized | Explicit launch decision |

# Phase 0: Stabilize the recorder

## Objective

Repair defects that could make saved evidence internally inconsistent, silently incomplete, or unsuitable for replay.

No replay, modelling, paper execution, wallet support, or authenticated client work should merge before the Phase 0 gate.

## Phase 0 work packages

### P0-WP01: Envelope identity and schema v2

Scope:

- keep the normalized Gamma market ID consistent across all event sources;
- add the condition ID explicitly to each envelope;
- preserve source-native identifiers separately;
- validate that CLOB `market` identity matches the discovered condition ID;
- add raw-frame identity fields;
- increment schema versions;
- migrate synthetic fixtures and schema documentation.

Required fields should include, at minimum:

```ts
type RawEventEnvelopeV2 = {
  schemaVersion: 2;
  runId: string;
  sequence: number;
  frameId: string;
  messageIndex: number;
  messageCount: number;
  source: EventSource;
  eventType: string;
  marketId: string;
  conditionId: string;
  sourceMarketId: string | null;
  tokenId: string | null;
  sourceTimestamp: number | null;
  receivedAt: number;
  processedAt: number;
  connectionGeneration: number | null;
  payload: JsonValue;
  rawText: string | null;
  rawFrameSha256: string | null;
};
```

Acceptance:

- all event sources share the same normalized `marketId` and `conditionId`;
- condition mismatch fails closed;
- arrays have stable frame/message indexing;
- fixture migration is explicit and tested.

### P0-WP02: Evidence-preserving duplicate classification

Scope:

- remove hash-only event dropping;
- persist every external frame;
- classify suspected duplicates without deleting evidence;
- prevent confirmed duplicates from mutating derived book state twice;
- distinguish repeated transport delivery from legitimate state changes.

Acceptance:

- two changes with the same order hash but different sizes are both retained and applied in order;
- an exact repeated frame can be identified through conservative raw-frame identity;
- duplicate classification is reproducible in replay;
- health reports count observed and classified events separately.

### P0-WP03: Connection, gap, and stale-state semantics

Scope:

- separate socket-open state from usable-data state;
- end a data gap only after a valid subscribed message;
- reconnect stale but open sockets;
- preserve open gaps through repeated reconnect failures;
- include unresolved gaps in final health output.

Acceptance:

- open-but-silent sockets cause stale handling and reconnect;
- `DATA_GAP_ENDED` cannot occur before relevant data resumes;
- controlled shutdown records an unresolved gap when applicable;
- reconnection restores subscriptions.

### P0-WP04: Transactional book-health state machine

Scope:

- introduce `UNINITIALIZED`, `SYNCHRONIZED`, `STALE`, and `INVALID` states;
- require a complete snapshot before deltas;
- validate entire events before mutation;
- reject invalid sides, negative quantities, invalid prices, unknown tokens, and crossed books;
- ensure trade-only events do not refresh book freshness;
- require a new snapshot after invalidation.

Acceptance:

- malformed multi-change frames leave no partial mutation;
- deltas before initial snapshots are rejected or quarantined;
- only complete validated state transitions update freshness;
- invariant failures are explicit in the event and health records.

### P0-WP05: Capture-status and fatal-error propagation

Scope:

- separate process termination from capture usability;
- stop feeds promptly after persistent storage or processor failure;
- report quality failures explicitly;
- use the CLOB-specific threshold for book health;
- record signed clock differences rather than clamping them away.

Add:

```ts
type CaptureAssessment = {
  captureStatus: "COMPLETE" | "PARTIAL" | "UNUSABLE";
  usableForReplay: boolean;
  qualityFailures: string[];
};
```

Acceptance:

- `COMPLETED` process status does not imply replay usability;
- missing snapshots, missing Chainlink data, unresolved gaps, fatal processing errors, and sequence defects produce explicit failures;
- fatal write failures terminate the run promptly.

### P0-WP06: Gamma resilience and exact HTTP evidence

Scope:

- retain original Gamma response text and normalized data;
- add bounded retry for 429 and transient 5xx responses;
- validate realistic timing fields including `eventStartTime`;
- distinguish HTTP transport failure from contract-validation failure.

Acceptance:

- raw response bytes or text are retained;
- retry behavior is deterministic under tests;
- invalid or ambiguous contracts still fail closed.

## Phase 0 concurrency

Safe parallel groups after P0-WP01 publishes the proposed v2 contract:

- Group A: P0-WP02 duplicate classification;
- Group B: P0-WP03 feed lifecycle;
- Group C: P0-WP04 book state machine;
- Group D: P0-WP06 Gamma resilience.

P0-WP05 integrates outputs from Groups A-C and should merge after them.

## Phase 0 exit gate

All of the following must pass:

- schema v2 committed and documented;
- all fixtures migrated;
- no hash-only frame dropping;
- transactional book updates;
- gap semantics corrected;
- capture assessment implemented;
- lint, typecheck, unit, integration, and read-only boundary checks pass;
- no authenticated dependency or endpoint added.

# Phase 1: Public compatibility and capture operations

## Objective

Prove that the corrected recorder works against current public interfaces and can generate operationally useful capture artifacts.

## Phase 1 work packages

### P1-WP01: Real public compatibility fixture

- run the manual read-only smoke workflow;
- capture a current Gamma response and live WebSocket shapes;
- sanitize only identifiers that are genuinely sensitive or excessive;
- retain realistic timing, field, array, and numeric representations;
- add contract tests against the real shape.

### P1-WP02: Market rollover recorder

- discover the next five-minute market before the current market closes;
- preserve separate run/market boundaries;
- never mix token IDs across markets;
- support a controlled multi-market capture session;
- emit explicit rollover lifecycle events.

### P1-WP03: Capture supervisor

- add restart policy and process-level health checks;
- preserve incomplete runs rather than overwriting them;
- support clean recovery after process restart;
- report storage capacity and write failures;
- keep all functionality read-only.

### P1-WP04: Capture validation command

Add a local command such as:

```bash
pnpm market:validate-run --run <path>
```

It must validate:

- file presence;
- schema versions;
- JSONL integrity;
- event-count agreement;
- sequence continuity;
- frame/message indexing;
- identity consistency;
- initial snapshots;
- reference observations;
- closed and unresolved gaps;
- final capture assessment.

### P1-WP05: Compact archival format

- add an optional immutable compressed archive of a completed run;
- preserve canonical JSONL as the source of truth;
- include checksums in the manifest;
- verify archive extraction and checksum reproduction.

## Phase 1 concurrency

After recorder schema v2 freezes:

- P1-WP01, P1-WP02, P1-WP03, and P1-WP04 may proceed concurrently;
- P1-WP05 depends on P1-WP04's validation contract.

## Phase 1 exit gate

- one real public capture is retained as evidence;
- both token snapshots and multiple Chainlink observations are present;
- the capture validator marks it replay-usable;
- market rollover works without identity mixing;
- no authenticated interface is used.

# Phase 2: Deterministic replay and canonical market state

## Objective

Reconstruct the exact observable state sequence from a saved run without external network calls.

## Phase 2 work packages

### P2-WP01: Replay input validator and deterministic clock

- accept only local saved runs;
- reject unsupported schema versions or provide explicit migration;
- validate all run invariants before replay;
- implement a deterministic clock using recorded timestamps;
- expose controlled ordering rules for ties.

### P2-WP02: Full order-book reconstruction

- reconstruct independent Up and Down books;
- require valid snapshots before deltas;
- maintain sorted levels using decimal-safe representations;
- check nonnegative depth, valid prices, and uncrossed books;
- mark state unavailable through gaps and invalid periods.

### P2-WP03: Reference-price reconstruction

- reconstruct the latest valid Chainlink state;
- preserve source/receive/process timestamps;
- expose data age and latency fields;
- do not invent observations within gaps.

### P2-WP04: Canonical `MarketState` schema

Define and freeze a versioned state such as:

```ts
type MarketStateV1 = {
  schemaVersion: 1;
  runId: string;
  sequence: number;
  marketId: string;
  conditionId: string;
  observedAt: number;
  marketOpenAt: number;
  marketCloseAt: number;
  millisecondsRemaining: number;
  openingReferencePrice: string | null;
  currentReferencePrice: string | null;
  distanceFromOpenBps: string | null;
  upBook: TokenBookState;
  downBook: TokenBookState;
  dataQuality: MarketStateQuality;
};
```

### P2-WP05: Replay checkpoints and canonical output

- output deterministic state-change JSONL;
- provide periodic checkpoints for fast seeking;
- include code, schema, configuration, and input checksums;
- produce byte-identical output for identical inputs.

### P2-WP06: Replay diagnostics report

- state counts;
- unavailable periods;
- gaps;
- book invalidations;
- crossed-book attempts;
- reference-feed age;
- time coverage;
- checksum results.

## Phase 2 concurrency

- P2-WP01 must establish the replay runner and clock first.
- P2-WP02 and P2-WP03 may then proceed concurrently.
- P2-WP04 is contract-owner work and integrates both state streams.
- P2-WP05 and P2-WP06 proceed after the state contract freezes.

## Phase 2 exit gate

- no replay command performs network I/O;
- order books and Chainlink state reconstruct deterministically;
- identical inputs produce byte-identical canonical output;
- gaps and unavailable periods remain explicit;
- malformed fixtures fail with stable reason codes.

# Phase 3: Research dataset and settlement labels

## Objective

Convert replayed state into leakage-safe, auditable training and evaluation data.

## Phase 3 work packages

### P3-WP01: Opening and closing reference determination

- formalize how the contract's opening and ending Chainlink values are selected;
- preserve surrounding observations and selection evidence;
- distinguish observed boundary values from inferred approximations;
- mark markets unusable when settlement-aligned values cannot be established reliably.

### P3-WP02: Resolution and label ingestion

- obtain and persist official market resolution;
- define Up and Down exactly as the contract specifies;
- reconcile official outcome with reconstructed boundary values;
- quarantine disagreements.

### P3-WP03: Feature-window builder

- generate features only from information available at the forecast timestamp;
- support multiple decision horizons within each five-minute interval;
- preserve feature timestamp, source timestamp, and latest allowed source event;
- prevent settlement and post-decision leakage.

### P3-WP04: Dataset storage

- add Parquet for high-volume research data;
- optionally use DuckDB for local analysis;
- retain links back to run, sequence, frame, market, and raw evidence;
- version dataset schemas and transformation code.

### P3-WP05: Dataset audit

- missingness;
- feed age;
- spread and depth distributions;
- volatility regimes;
- class balance;
- invalid or unresolved markets;
- overlap between train, validation, and test periods;
- leakage checks.

## Phase 3 concurrency

P3-WP01 and P3-WP02 may run concurrently but must agree before labels are accepted. P3-WP03 and P3-WP04 may proceed against provisional schemas, but final merge waits for the label contract. P3-WP05 merges last.

## Phase 3 exit gate

- every row traces to a replay sequence and raw run;
- chronological splits are explicit;
- outcome labels reconcile with official resolution;
- no post-decision feature is included;
- unusable markets are retained in audit output but excluded from modelling.

# Phase 4: Probability baselines and calibration

## Objective

Estimate calibrated probabilities, not merely Up/Down classifications.

## Forecast contract

Freeze a contract similar to:

```ts
type ProbabilityForecastV1 = {
  schemaVersion: 1;
  marketId: string;
  decisionSequence: number;
  generatedAt: number;
  featureTimestamp: number;
  modelName: string;
  modelVersion: string;
  pUp: string;
  uncertainty: string | null;
  calibrationVersion: string | null;
  diagnostics: Record<string, JsonValue>;
};
```

## Phase 4 work packages

### P4-WP01: No-information and market baselines

- 50% baseline;
- current executable-price or midpoint baseline clearly labeled;
- time-and-distance analytical baseline where justified;
- no claim that market price is ground truth.

### P4-WP02: Transparent statistical baseline

- logistic regression or similarly interpretable model;
- walk-forward fitting;
- regularization selected without test-set reuse;
- coefficient and stability reports.

### P4-WP03: Tree or nonlinear baseline

- one bounded nonlinear model;
- controlled hyperparameter search;
- chronological validation only;
- feature-importance stability checks.

### P4-WP04: Calibration

- reliability diagrams;
- Brier score;
- log loss;
- expected calibration error;
- calibration by time remaining and volatility regime;
- post-hoc calibration fitted only on designated calibration periods.

### P4-WP05: Model registry and reproducibility

- model artifact checksums;
- training dataset version;
- feature schema version;
- code commit;
- random seed;
- evaluation period;
- rejected or unsupported model versions.

## Phase 4 concurrency

Baseline model work packages may proceed concurrently against the frozen dataset schema. P4-WP04 owns shared calibration utilities. P4-WP05 owns the registry contract and merges before final model reports.

## Phase 4 exit gate

- untouched chronological test periods remain untouched until final evaluation;
- forecasts remain within `[0,1]`;
- calibration is evaluated separately from directional accuracy;
- performance is compared with market-based baselines;
- no profitability claim is made without execution simulation.

# Phase 5: Policy engine

## Objective

Convert forecasts and observable state into auditable actions under explicit objectives and constraints.

## Policy action contract

```ts
type PolicyActionV1 =
  | { type: "ABSTAIN"; reasonCodes: string[] }
  | {
      type: "PLACE_ORDER";
      token: "UP" | "DOWN";
      side: "BUY" | "SELL";
      orderStyle: "TAKER_FAK" | "TAKER_FOK" | "MAKER_GTC" | "MAKER_GTD";
      limitPrice: string;
      quantity: string;
      expiresAt: number | null;
      reasonCodes: string[];
    }
  | { type: "CANCEL_ORDER"; orderId: string; reasonCodes: string[] }
  | { type: "CANCEL_ALL"; reasonCodes: string[] };
```

## Phase 5 work packages

### P5-WP01: Expected-value primitives

- executable depth-weighted acquisition cost;
- fee schedule input;
- spread and slippage estimates;
- conservative probability bound;
- all-in break-even probability;
- decimal-safe arithmetic.

### P5-WP02: Reference abstaining policy

- buy Up only above conservative all-in break-even;
- buy Down only above conservative all-in break-even;
- abstain otherwise;
- reject stale, gapped, invalid, or incomplete state;
- enforce pre-close safety window.

### P5-WP03: Portfolio and sizing interface

- no-position baseline;
- fixed-notional sizing;
- capped fractional Kelly as an optional research policy;
- strict maximum exposure and drawdown inputs;
- no sizing method may bypass risk checks.

### P5-WP04: Policy explanation record

Persist every input and intermediate calculation:

- forecast and uncertainty;
- executable book levels used;
- fee version;
- slippage buffer;
- expected gross and net value;
- risk checks;
- final action and reason codes.

### P5-WP05: Policy evaluation

- action rate and abstention rate;
- edge distribution;
- sensitivity to uncertainty margin;
- sensitivity to fee and slippage assumptions;
- stability across regimes.

## Phase 5 concurrency

P5-WP01 and the action contract merge first. P5-WP02 and P5-WP03 may then proceed concurrently. P5-WP04 is shared audit infrastructure. P5-WP05 merges after all policy variants stabilize.

## Phase 5 exit gate

- every action is reproducible;
- abstention works and is tested;
- policy never consumes future state;
- policy uses executable depth rather than only midpoint;
- policy has no network or SDK dependency.

# Phase 6: Paper execution and accounting

## Objective

Simulate orders, fills, positions, fees, and settlement under explicit conservative assumptions.

## Phase 6 work packages

### P6-WP01: Aggressive FAK/FOK simulator

- walk contemporaneous asks or bids;
- respect limits and available depth;
- implement partial fills for FAK;
- require complete fills for FOK;
- apply current fee configuration;
- produce deterministic fills.

### P6-WP02: Passive-order simulation bands

Because exact queue position is generally unavailable:

- conservative fill model;
- neutral or evidence-based intermediate model;
- optimistic upper bound;
- clear labels on every result;
- no optimistic result may be described as realized performance.

### P6-WP03: Portfolio accounting

- cash;
- inventory by token and market;
- reserved capital;
- average acquisition cost;
- realized and unrealized P&L;
- fees;
- settlement;
- void or cancellation handling.

### P6-WP04: Risk engine

- maximum order notional;
- maximum market exposure;
- maximum total exposure;
- maximum daily and session loss;
- minimum expected net edge;
- maximum data age;
- minimum time remaining;
- maximum spread and slippage;
- circuit breakers;
- duplicate-order protection.

### P6-WP05: Independent simulator comparison

Use an independent implementation or framework, potentially NautilusTrader, as a benchmark for selected fixtures. The project remains authoritative for its own domain contracts, but discrepancies in fills, fees, cash, inventory, or settlement must block conclusions until resolved.

### P6-WP06: Paper report

- gross and net P&L;
- fees and slippage;
- exposure through time;
- maximum drawdown;
- return on capital at risk;
- results by time remaining, spread, depth, and volatility;
- maker-fill assumption sensitivity;
- forecast metrics separately from trading metrics.

## Phase 6 concurrency

P6-WP01, P6-WP02, and P6-WP03 can begin concurrently against a frozen execution schema, but final accounting tests must integrate all order types. P6-WP04 may proceed independently. P6-WP05 and P6-WP06 merge after the core simulator stabilizes.

## Phase 6 exit gate

- deterministic fill fixtures pass;
- FOK is full-or-zero;
- accounting identities reconcile;
- settlement is correct;
- conservative results remain positive before any edge claim;
- independent comparison discrepancies are resolved or documented as blockers.

# Phase 7: Continuous paper and shadow operation

## Objective

Run the complete public-data, forecast, policy, and paper-execution pipeline continuously without authenticated trading.

## Phase 7 work packages

### P7-WP01: Runtime orchestration

- automatic market rollover;
- restart recovery;
- single active policy decision stream;
- idempotent decisions;
- process supervision;
- graceful shutdown.

### P7-WP02: Operational storage

- PostgreSQL for run metadata, forecasts, decisions, simulated orders, fills, positions, and incidents;
- Parquet for bulk ticks and research states;
- schema migrations;
- retention policy;
- backup and restore test.

### P7-WP03: Observability

- structured logs;
- health and readiness endpoints;
- event lag;
- feed age;
- reconnect counts;
- book invalidation counts;
- forecast and decision latency;
- paper order and fill metrics;
- current risk state.

### P7-WP04: Daily and session reports

- market coverage;
- data-quality exclusions;
- forecast calibration;
- policy actions;
- simulated fills;
- P&L and drawdown;
- assumption sensitivity;
- operational incidents.

### P7-WP05: Long-running reliability tests

- forced disconnects;
- slow storage;
- malformed frames;
- unavailable Gamma;
- stale Chainlink;
- process restart during an open market;
- rollover race conditions;
- disk exhaustion simulation.

## Phase 7 exit gate

- stable multi-day paper operation;
- no silent gaps;
- restart recovery is demonstrated;
- reports reconcile with stored events;
- kill and shutdown procedures work;
- no wallet or authenticated endpoint is required.

# Phase 8: Dormant authenticated live adapter

## Objective

Implement, but do not enable by default, the authenticated execution boundary using the current official Polymarket SDK and CLOB V2 order workflows.

## Phase 8 work packages

### P8-WP01: Official SDK gateway

- pin the official unified SDK exactly;
- wrap it behind a local gateway interface;
- isolate SDK-native types;
- add contract tests for supported wallet and order configurations;
- document upgrade procedure.

### P8-WP02: Secret and identity boundary

- separate live configuration and secret namespace;
- prevent secrets from entering logs, fixtures, reports, or CI;
- define supported wallet type explicitly;
- fail closed on identity mismatch.

### P8-WP03: Order lifecycle adapter

- submit;
- acknowledge;
- reject;
- partially fill;
- fill;
- cancel;
- expire;
- reconcile ambiguous submissions before retry;
- maintain unique local correlation IDs.

### P8-WP04: Authenticated user stream and reconciliation

- process order and trade updates;
- periodically reconcile against REST state;
- reconcile after reconnect;
- detect duplicate or missing fills;
- stop trading on mismatch.

### P8-WP05: Heartbeat and emergency cancellation

- official heartbeat mechanism when required;
- explicit cancel-all;
- cancel on controlled shutdown;
- cancel on stale market data;
- cancel on auth failure;
- local and remote kill-switch pathways.

### P8-WP06: Compliance and geoblock gate

- perform the current official availability/geoblock check;
- never bypass or evade geographic restrictions;
- disable live submission when unavailable;
- record gate result without exposing sensitive data.

## Mandatory live gates

Live order placement must require all of the following independently:

```text
MODE=LIVE
LIVE_TRADING_ENABLED=true
explicit command-line confirmation
valid authenticated credentials
successful compliance/geoblock check
healthy CLOB stream
healthy Chainlink stream
healthy clock
validated market specification
successful account reconciliation
nonzero configured limits
kill switch inactive
```

No unit test, integration test, CI job, migration, startup probe, or documentation example may satisfy all gates.

## Phase 8 exit gate

- authenticated adapter tests pass with mocks or approved sandbox-like controls;
- order reconciliation is deterministic under fixtures;
- live mode remains disabled by default;
- CI cannot access live secrets;
- no live order is required to complete this phase.

# Phase 9: Limited live pilot and production hardening

## Objective

Only after a separate explicit decision, operate a tightly limited live pilot and determine whether the system is economically and operationally viable.

## Prerequisites

- legal and compliance review completed;
- account and geographic eligibility confirmed;
- continuous paper results reviewed;
- conservative execution assumptions remain acceptable;
- explicit capital and loss limits approved;
- incident owner and shutdown procedure assigned;
- live adapter independently reviewed.

## Pilot sequence

1. read-only account reconciliation;
2. authenticated cancel test with no open exposure where possible;
3. smallest permitted order notional;
4. one market at a time;
5. taker order types before maker assumptions;
6. manual observation of every order and reconciliation event;
7. automatic stop after predefined count, loss, or inconsistency;
8. post-pilot reconciliation against platform records.

## Production-hardening work

- deployment isolation;
- secret rotation;
- backup and disaster recovery;
- incident response drills;
- dependency and SDK upgrade testing;
- audit logs;
- operational dashboards;
- explicit release and rollback procedures;
- capital scaling rules based on measured slippage and drawdown, not win rate alone.

## Phase 9 exit gate

There is no automatic promotion. The project requires a documented go, revise, or stop decision based on:

- net realized results;
- execution quality;
- calibration stability;
- drawdown;
- operational incidents;
- compliance status;
- evidence that results are not explained by optimistic simulation assumptions.

# 7. Shared testing strategy

## 7.1 Unit tests

- decimal parsing and arithmetic;
- schema validation and migration;
- identity mapping;
- frame splitting and hashing;
- gap state machine;
- order-book transactionality;
- replay ordering;
- feature timestamp boundaries;
- forecast bounds;
- expected-value calculations;
- policy abstention;
- risk checks;
- fill mechanics;
- accounting identities;
- settlement.

## 7.2 Property-based tests

Where practical:

- valid deltas never create negative depth;
- failed frame validation leaves state unchanged;
- replay output is deterministic;
- FOK fills are complete or zero;
- cash and inventory accounting balances;
- probabilities remain bounded;
- duplicate processing does not create duplicate state mutation or fills;
- risk limits cannot be bypassed through order splitting.

## 7.3 Integration tests

- public-market discovery fixtures;
- WebSocket snapshots and changes;
- array frames;
- disconnect and reconnect;
- open-but-silent socket;
- stale reference stream;
- market rollover;
- interrupted storage;
- process restart;
- settlement;
- ambiguous order submission and reconciliation in the dormant live phase.

## 7.4 Golden fixtures

Maintain immutable fixtures for:

- valid real public capture;
- duplicate transport delivery;
- same hash with changed size;
- delta before snapshot;
- crossed book;
- malformed decimal;
- stale stream;
- unresolved gap;
- market rollover;
- official resolution disagreement;
- aggressive full fill;
- aggressive partial fill;
- FOK rejection;
- maker-fill sensitivity.

Golden fixtures must carry schema versions and checksums.

## 7.5 Acceptance evidence

A phase is not complete because code exists. Its PR or release note must include:

- exact commands;
- exact test results;
- fixture names and checksums;
- smoke or long-run evidence where applicable;
- known limitations;
- confirmation of excluded capabilities.

# 8. Data and schema governance

## 8.1 Schema changes

Any change to field meaning requires a new schema version. Adding an optional field may remain compatible only when old consumers have defined behavior for its absence.

Every schema change requires:

- migration or explicit non-migratable declaration;
- fixture updates;
- reader compatibility tests;
- documentation;
- handoff note;
- downstream impact list.

## 8.2 Evidence lineage

Derived records should eventually retain:

- run ID;
- market ID;
- condition ID;
- source frame ID;
- source sequence;
- replay sequence;
- feature schema version;
- model version;
- policy version;
- fee version;
- simulator version;
- code commit.

## 8.3 Time semantics

Use:

- UTC wall time for recorded timestamps;
- monotonic time for local elapsed durations;
- source timestamps exactly as provided;
- signed source-to-receive clock difference;
- explicit event age;
- explicit market-relative time.

Do not infer chronological causality solely from wall-clock equality. Local sequence remains the recorder's canonical receipt order.

# 9. Dependency policy

## Preferred use

- direct public WebSocket handling may remain appropriate for the focused recorder;
- the official unified Polymarket SDK should be introduced only behind a gateway when authenticated execution work begins;
- the official RTDS client may be evaluated, but it must remain behind a local reliability adapter;
- OpenMarket may be used as a research and methodology reference, not copied as an assumed production executor;
- NautilusTrader may be used as an independent paper/accounting benchmark, not as unquestioned ground truth.

## Dependency requirements

- pin exact versions;
- commit the lockfile;
- record licenses;
- add upgrade contract tests;
- isolate external types;
- do not adopt archived CLOB V1 clients;
- do not adopt repositories advertising guaranteed profit as core dependencies.

# 10. Security and safety boundaries

Before the live phase:

- no private keys;
- no API credentials;
- no mutating Polymarket requests;
- no authenticated user channel;
- no order-signing dependency;
- no CI secret capable of placing an order.

The read-only source scan is a useful regression guard, not a formal proof of safety. Runtime architecture, dependency review, network policy, and tests must enforce the boundary as the system grows.

# 11. Research validity checklist

No performance result should be promoted unless it answers all of the following:

- Was the evaluation chronological?
- Was the final test period untouched during model and policy development?
- Were features available at the decision timestamp?
- Were contract prices executable bid/ask levels rather than midpoints?
- Were depth, fees, slippage, latency, partial fills, and missed fills included?
- Were maker queue assumptions labeled and stress-tested?
- Were overlapping observations handled appropriately?
- Were invalid, stale, or unresolved markets excluded through predefined rules?
- Was calibration evaluated, not only accuracy or win rate?
- Was P&L separated from forecast quality?
- Did the result survive reasonable parameter and regime sensitivity checks?
- Can every trade be reproduced from raw evidence?

# 12. Suggested issue structure

Create one tracking issue per phase and one issue per work package.

Suggested labels:

```text
phase:0 through phase:9
area:recorder
area:replay
area:data
area:model
area:policy
area:execution
area:risk
area:operations
contract-change
schema-migration
blocked
parallel-safe
acceptance
```

Each issue should include:

- objective;
- dependencies;
- allowed paths;
- prohibited scope;
- acceptance criteria;
- test requirements;
- phase gate impacted.

# 13. Immediate execution order

The next work should proceed in this order:

1. P0-WP01 envelope identity and schema v2;
2. launch P0-WP02, P0-WP03, P0-WP04, and P0-WP06 concurrently from the v2 contract branch or freeze commit;
3. merge those corrections through small PRs;
4. complete P0-WP05 integration and capture assessment;
5. run the complete Phase 0 test matrix;
6. begin P1 public compatibility and capture operations;
7. do not start deterministic replay implementation until the Phase 0 schema and semantics are accepted.

# 14. Definition of project success

The project is successful when it can make a defensible statement of the following form:

> Given a versioned raw capture, this system can deterministically reconstruct the observable market state, produce a timestamp-valid calibrated probability forecast, apply an explicit auditable policy, simulate or execute an order under documented assumptions and risk limits, and reconcile the resulting position and settlement.

This definition does not require the resulting policy to be profitable. A negative result produced through reliable data, realistic execution, and reproducible analysis is still a valid research outcome.
