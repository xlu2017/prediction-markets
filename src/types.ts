export type JsonPrimitive = boolean | null | number | string;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

export type EventSource = "POLYMARKET_CLOB" | "POLYMARKET_MARKET" | "POLYMARKET_REFERENCE_PRICE";

export type MarketSpecification = {
  schemaVersion: 2;
  eventId: string;
  marketId: string;
  conditionId: string;
  slug: string;
  question: string;
  opensAt: string;
  closesAt: string;
  resolutionSource: string;
  referenceSymbol: "btc/usd";
  referenceTopic: "crypto_prices_chainlink";
  upTokenId: string;
  downTokenId: string;
  tickSize: string | null;
  active: boolean;
  closed: boolean;
  rawMetadata: JsonValue;
  rawGammaText: string;
};

export type RawEventEnvelope = {
  schemaVersion: 2;
  runId: string;
  sequence: number;
  source: EventSource;
  eventType: string;
  marketId: string | null;
  conditionId: string | null;
  sourceMarketId: string | null;
  tokenId: string | null;
  sourceTimestamp: number | null;
  receivedAt: number;
  processedAt: number;
  connectionGeneration: number | null;
  frameId: string | null;
  messageIndex: number | null;
  messageCount: number | null;
  isDuplicate: boolean;
  duplicateOfSequence: number | null;
  dedupeReason: string | null;
  payload: JsonValue;
  rawText: string | null;
};

export type CompletionStatus = "COMPLETED" | "FAILED" | "RUNNING";
export type CaptureStatus = "COMPLETE" | "PARTIAL" | "UNUSABLE";
export type BookState = "UNINITIALIZED" | "SYNCHRONIZED" | "STALE" | "INVALID";

export type RunManifest = {
  schemaVersion: 2;
  runId: string;
  startedAt: string;
  endedAt: string | null;
  recorderVersion: string;
  gitCommit: string | null;
  market: MarketSpecification;
  commandArguments: Record<string, JsonValue>;
  outputFiles: string[];
  completionStatus: CompletionStatus;
  finalEventCount: number | null;
  failureReason: string | null;
  captureStatus: CaptureStatus;
  usableForReplay: boolean;
  qualityFailures: string[];
};

export type BookHealth = {
  tokenId: string;
  state: BookState;
  bestBid: string | null;
  bestAsk: string | null;
  bidDepthCount: number;
  askDepthCount: number;
  lastSuccessfulUpdateTime: number | null;
  healthy: boolean;
};

export type OpenGap = {
  feed: string;
  startedAt: number;
};

export type HealthSummary = {
  schemaVersion: 2;
  runId: string;
  startedAt: string;
  endedAt: string | null;
  totalEventsReceived: number;
  eventCountsBySource: Record<string, number>;
  eventCountsByType: Record<string, number>;
  malformedEventCount: number;
  duplicateEventCount: number;
  reconnectCount: number;
  maximumSourceToReceiveLatencyMs: number | null;
  maximumReceiveToProcessLatencyMs: number | null;
  staleFeedIncidents: number;
  bothTokenStreamsObserved: boolean;
  referencePriceStreamObserved: boolean;
  finalBookHealth: Record<string, BookHealth>;
  openGapsByFeed: Record<string, OpenGap>;
  captureStatus: CaptureStatus;
  usableForReplay: boolean;
  qualityFailures: string[];
};

export type LogLevel = "debug" | "error" | "info" | "warn";

export type Logger = {
  debug: (message: string, context?: Record<string, JsonValue>) => void;
  error: (message: string, context?: Record<string, JsonValue>) => void;
  info: (message: string, context?: Record<string, JsonValue>) => void;
  warn: (message: string, context?: Record<string, JsonValue>) => void;
};
