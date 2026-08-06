#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import path from "node:path";

import { Command } from "commander";

import type {
  BookHealth,
  HealthSummary,
  QualityFailureCode,
  RawEventEnvelope,
  RunManifest,
} from "./types.js";
import { OrderBookTracker } from "./book.js";

const EVENT_SOURCES = [
  "POLYMARKET_CLOB",
  "POLYMARKET_MARKET",
  "POLYMARKET_REFERENCE_PRICE",
] as const;
const BOOK_STATES = [
  "UNINITIALIZED",
  "SYNCHRONIZED",
  "STALE",
  "RESYNC_REQUIRED",
  "INVALID",
] as const;
const BOOK_INTEGRITIES = ["NORMAL", "LOCKED", "CROSSED"] as const;
const DEDUPE_STATES = ["UNIQUE_APPLIED", "DUPLICATE_APPLIED", "RECEIVED_NOT_APPLIED"] as const;
const COMPLETION_STATUSES = ["COMPLETED", "FAILED", "RUNNING"] as const;
const CAPTURE_STATUSES = ["COMPLETE", "PARTIAL", "UNUSABLE"] as const;
const QUALITY_FAILURE_CODES = [
  "MISSING_UP_SNAPSHOT",
  "MISSING_DOWN_SNAPSHOT",
  "REFERENCE_STREAM_MISSING",
  "REFERENCE_STREAM_STALE",
  "REFERENCE_CLOCK_SKEW",
  "REFERENCE_TIMESTAMP_MISSING",
  "REFERENCE_LOCAL_CLOCK_SKEW",
  "OPEN_DATA_GAP",
  "CLOSED_DATA_GAP",
  "MALFORMED_EVENTS_PRESENT",
  "BOOK_STALE",
  "BOOK_INVALID",
  "BOOK_RESYNC_REQUIRED",
  "BOOK_SNAPSHOT_STALE_GENERATION",
  "BOOK_CROSSED",
  "FATAL_PROCESSOR_ERROR",
  "PROCESS_FAILURE",
] as const;

function assertString(value: unknown, label: string): string {
  if (typeof value !== "string") throw new Error(`${label} is not a string`);
  return value;
}

function assertNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`${label} is not a finite number`);
  }
  return value;
}

function assertPositiveNumber(value: unknown, label: string): number {
  const n = assertNumber(value, label);
  if (n <= 0) throw new Error(`${label} is not a positive number`);
  return n;
}

function assertBoolean(value: unknown, label: string): boolean {
  if (typeof value !== "boolean") throw new Error(`${label} is not a boolean`);
  return value;
}

function assertIsoTimestamp(value: unknown, label: string): string {
  const s = assertString(value, label);
  if (Number.isNaN(Date.parse(s))) throw new Error(`${label} is not a valid ISO timestamp: ${s}`);
  return s;
}

function assertOptionalNumber(value: unknown, label: string): number | null {
  if (value === null) return null;
  return assertNumber(value, label);
}

function assertOptionalString(value: unknown, label: string): string | null {
  if (value === null) return null;
  return assertString(value, label);
}

function assertNonNegativeSafeInteger(value: unknown, label: string): number {
  const n = assertNumber(value, label);
  if (!Number.isSafeInteger(n) || n < 0) {
    throw new Error(`${label} is not a non-negative safe integer`);
  }
  return n;
}

function assertOptionalNonNegativeSafeInteger(value: unknown, label: string): number | null {
  if (value === null) return null;
  return assertNonNegativeSafeInteger(value, label);
}

function assertOptionalFiniteNumber(value: unknown, label: string): number | null {
  if (value === null) return null;
  if (typeof value !== "number" || !Number.isFinite(value)) {
    const type = typeof value;
    throw new Error(`${label} has type ${type}, expected a finite number or null`);
  }
  return value;
}

function assertEnum<T extends string>(value: unknown, allowed: readonly T[], label: string): T {
  const s = assertString(value, label);
  if (!allowed.includes(s as T)) {
    throw new Error(`${label} is ${s}, expected one of ${allowed.join(", ")}`);
  }
  return s as T;
}

function assertStringArray(value: unknown, label: string): string[] {
  if (!Array.isArray(value)) throw new Error(`${label} is not an array`);
  for (let i = 0; i < value.length; i += 1) {
    if (typeof value[i] !== "string") throw new Error(`${label}[${i}] is not a string`);
  }
  return value as string[];
}

function assertOptionalStringArray(value: unknown, label: string): string[] | null {
  if (value === null) return null;
  return assertStringArray(value, label);
}

function assertRecord(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${label} is not an object`);
  }
  return value as Record<string, unknown>;
}

function assertQualityFailure(value: unknown, label: string): { code: string; message: string } {
  const record = assertRecord(value, label);
  assertEnum(record.code, QUALITY_FAILURE_CODES, `${label}.code`);
  assertString(record.message, `${label}.message`);
  if (record.feed !== undefined && record.feed !== null) assertString(record.feed, `${label}.feed`);
  if (record.tokenId !== undefined && record.tokenId !== null)
    assertString(record.tokenId, `${label}.tokenId`);
  return record as unknown as { code: string; message: string };
}

function assertQualityFailureArray(
  value: unknown,
  label: string,
): { code: string; message: string }[] {
  if (!Array.isArray(value)) throw new Error(`${label} is not an array`);
  return value.map((item, i) => assertQualityFailure(item, `${label}[${i}]`));
}

function assertCountRecord(value: unknown, label: string): Record<string, number> {
  const record = assertRecord(value, label);
  for (const [key, val] of Object.entries(record)) {
    assertNonNegativeSafeInteger(val, `${label}.${key}`);
  }
  return record as Record<string, number>;
}

export function runtimeValidateManifest(raw: unknown): RunManifest {
  const record = assertRecord(raw, "manifest");
  if (record.schemaVersion !== 2)
    throw new Error(`manifest schemaVersion is ${String(record.schemaVersion)}, expected 2`);
  assertString(record.runId, "manifest.runId");
  assertIsoTimestamp(record.startedAt, "manifest.startedAt");
  if (record.endedAt !== null) assertIsoTimestamp(record.endedAt, "manifest.endedAt");
  assertString(record.recorderVersion, "manifest.recorderVersion");
  if (record.gitCommit !== null) assertString(record.gitCommit, "manifest.gitCommit");
  const market = assertRecord(record.market, "manifest.market");
  if (market.schemaVersion !== 2)
    throw new Error(`manifest.market.schemaVersion is ${String(market.schemaVersion)}, expected 2`);
  assertString(market.eventId, "manifest.market.eventId");
  assertString(market.marketId, "manifest.market.marketId");
  assertString(market.conditionId, "manifest.market.conditionId");
  assertString(market.slug, "manifest.market.slug");
  assertString(market.question, "manifest.market.question");
  assertIsoTimestamp(market.opensAt, "manifest.market.opensAt");
  assertIsoTimestamp(market.closesAt, "manifest.market.closesAt");
  assertString(market.resolutionSource, "manifest.market.resolutionSource");
  const referenceSymbol = assertString(market.referenceSymbol, "manifest.market.referenceSymbol");
  if (referenceSymbol !== "btc/usd") {
    throw new Error(`manifest.market.referenceSymbol is ${referenceSymbol}, expected btc/usd`);
  }
  const referenceTopic = assertString(market.referenceTopic, "manifest.market.referenceTopic");
  if (referenceTopic !== "crypto_prices_chainlink") {
    throw new Error(
      `manifest.market.referenceTopic is ${referenceTopic}, expected crypto_prices_chainlink`,
    );
  }
  assertString(market.upTokenId, "manifest.market.upTokenId");
  assertString(market.downTokenId, "manifest.market.downTokenId");
  if (market.tickSize !== null) assertString(market.tickSize, "manifest.market.tickSize");
  assertBoolean(market.active, "manifest.market.active");
  assertBoolean(market.closed, "manifest.market.closed");
  validateJsonValue(market.rawMetadata, "manifest.market.rawMetadata");
  assertString(market.rawGammaText, "manifest.market.rawGammaText");
  const cmdArgs = assertRecord(record.commandArguments, "manifest.commandArguments");
  validateJsonValue(cmdArgs, "manifest.commandArguments");
  if (!Array.isArray(record.outputFiles)) throw new Error("manifest.outputFiles is not an array");
  for (let i = 0; i < record.outputFiles.length; i += 1) {
    if (typeof record.outputFiles[i] !== "string")
      throw new Error(`manifest.outputFiles[${i}] is not a string`);
  }
  assertEnum(record.completionStatus, COMPLETION_STATUSES, "manifest.completionStatus");
  assertOptionalNonNegativeSafeInteger(record.finalEventCount, "manifest.finalEventCount");
  if (record.failureReason !== null) assertString(record.failureReason, "manifest.failureReason");
  assertEnum(record.captureStatus, CAPTURE_STATUSES, "manifest.captureStatus");
  assertBoolean(record.usableForReplay, "manifest.usableForReplay");
  assertQualityFailureArray(record.qualityFailures, "manifest.qualityFailures");
  assertPositiveNumber(record.clobStaleAfterMs, "manifest.clobStaleAfterMs");
  assertPositiveNumber(record.referenceStaleAfterMs, "manifest.referenceStaleAfterMs");
  return raw as RunManifest;
}

export function runtimeValidateBookHealth(raw: unknown, tokenId: string): BookHealth {
  const record = assertRecord(raw, `book health for ${tokenId}`);
  assertString(record.tokenId, `book.${tokenId}.tokenId`);
  assertEnum(record.state, BOOK_STATES, `book.${tokenId}.state`);
  assertEnum(record.integrity, BOOK_INTEGRITIES, `book.${tokenId}.integrity`);
  assertNonNegativeSafeInteger(record.crossedIncidentCount, `book.${tokenId}.crossedIncidentCount`);
  assertNonNegativeSafeInteger(record.synchronizationEpoch, `book.${tokenId}.synchronizationEpoch`);
  assertOptionalNonNegativeSafeInteger(
    record.snapshotConnectionGeneration,
    `book.${tokenId}.snapshotConnectionGeneration`,
  );
  assertOptionalString(record.bestBid, `book.${tokenId}.bestBid`);
  assertOptionalString(record.bestAsk, `book.${tokenId}.bestAsk`);
  assertNonNegativeSafeInteger(record.bidDepthCount, `book.${tokenId}.bidDepthCount`);
  assertNonNegativeSafeInteger(record.askDepthCount, `book.${tokenId}.askDepthCount`);
  assertOptionalNonNegativeSafeInteger(
    record.lastSuccessfulUpdateTime,
    `book.${tokenId}.lastSuccessfulUpdateTime`,
  );
  assertBoolean(record.healthy, `book.${tokenId}.healthy`);
  const state = record.state as BookHealth["state"];
  const snapshotGen = record.snapshotConnectionGeneration as number | null;
  const lastUpdate = record.lastSuccessfulUpdateTime as number | null;
  const bestBid = record.bestBid as string | null;
  const bestAsk = record.bestAsk as string | null;
  const bidDepth = record.bidDepthCount as number;
  const askDepth = record.askDepthCount as number;
  if (state === "SYNCHRONIZED") {
    if (snapshotGen === null || snapshotGen <= 0) {
      throw new Error(
        `book.${tokenId}.snapshotConnectionGeneration must be > 0 for SYNCHRONIZED, got ${String(snapshotGen)}`,
      );
    }
    if (lastUpdate === null) {
      throw new Error(`book.${tokenId}.lastSuccessfulUpdateTime must be non-null for SYNCHRONIZED`);
    }
  }
  if (state === "UNINITIALIZED") {
    if (snapshotGen !== null) {
      throw new Error(
        `book.${tokenId}.snapshotConnectionGeneration must be null for UNINITIALIZED, got ${snapshotGen}`,
      );
    }
    if (lastUpdate !== null) {
      throw new Error(
        `book.${tokenId}.lastSuccessfulUpdateTime must be null for UNINITIALIZED, got ${lastUpdate}`,
      );
    }
    if (bestBid !== null || bestAsk !== null) {
      throw new Error(
        `book.${tokenId}.bestBid/bestAsk must be null for UNINITIALIZED, got bid=${String(bestBid)} ask=${String(bestAsk)}`,
      );
    }
    if (bidDepth !== 0 || askDepth !== 0) {
      throw new Error(
        `book.${tokenId}.bidDepthCount/askDepthCount must be 0 for UNINITIALIZED, got bid=${bidDepth} ask=${askDepth}`,
      );
    }
  }
  return raw as BookHealth;
}

export function runtimeValidateHealth(raw: unknown): HealthSummary {
  const record = assertRecord(raw, "health");
  if (record.schemaVersion !== 2)
    throw new Error(`health schemaVersion is ${String(record.schemaVersion)}, expected 2`);
  assertString(record.runId, "health.runId");
  assertIsoTimestamp(record.startedAt, "health.startedAt");
  if (record.endedAt !== null) assertIsoTimestamp(record.endedAt, "health.endedAt");
  assertNonNegativeSafeInteger(record.totalEventsReceived, "health.totalEventsReceived");
  assertCountRecord(record.eventCountsBySource, "health.eventCountsBySource");
  assertCountRecord(record.eventCountsByType, "health.eventCountsByType");
  assertNonNegativeSafeInteger(record.malformedEventCount, "health.malformedEventCount");
  assertNonNegativeSafeInteger(record.duplicateEventCount, "health.duplicateEventCount");
  assertNonNegativeSafeInteger(record.reconnectCount, "health.reconnectCount");
  assertOptionalFiniteNumber(
    record.maximumSourceToReceiveLatencyMs,
    "health.maximumSourceToReceiveLatencyMs",
  );
  assertOptionalNonNegativeSafeInteger(
    record.maximumReceiveToProcessLatencyMs,
    "health.maximumReceiveToProcessLatencyMs",
  );
  assertNonNegativeSafeInteger(record.staleFeedIncidents, "health.staleFeedIncidents");
  assertBoolean(record.bothTokenStreamsObserved, "health.bothTokenStreamsObserved");
  assertBoolean(record.referencePriceStreamObserved, "health.referencePriceStreamObserved");
  const bookHealth = assertRecord(record.finalBookHealth, "health.finalBookHealth");
  for (const [tokenId, book] of Object.entries(bookHealth)) {
    runtimeValidateBookHealth(book, tokenId);
  }
  const openGaps = assertRecord(record.openGapsByFeed, "health.openGapsByFeed");
  for (const [feed, gap] of Object.entries(openGaps)) {
    const gapRecord = assertRecord(gap, `health.openGapsByFeed.${feed}`);
    assertString(gapRecord.feed, `health.openGapsByFeed.${feed}.feed`);
    assertNonNegativeSafeInteger(gapRecord.startedAt, `health.openGapsByFeed.${feed}.startedAt`);
  }
  assertNonNegativeSafeInteger(record.dataGapCount, "health.dataGapCount");
  assertNonNegativeSafeInteger(record.closedGapCount, "health.closedGapCount");
  assertNonNegativeSafeInteger(record.totalGapDurationMs, "health.totalGapDurationMs");
  assertNonNegativeSafeInteger(record.maximumGapDurationMs, "health.maximumGapDurationMs");
  assertEnum(record.captureStatus, CAPTURE_STATUSES, "health.captureStatus");
  assertBoolean(record.usableForReplay, "health.usableForReplay");
  assertQualityFailureArray(record.qualityFailures, "health.qualityFailures");
  assertPositiveNumber(record.clobStaleAfterMs, "health.clobStaleAfterMs");
  assertPositiveNumber(record.referenceStaleAfterMs, "health.referenceStaleAfterMs");
  assertOptionalNonNegativeSafeInteger(
    record.lastReferenceReceivedAt,
    "health.lastReferenceReceivedAt",
  );
  assertOptionalNonNegativeSafeInteger(
    record.lastReferenceConnectionGeneration,
    "health.lastReferenceConnectionGeneration",
  );
  assertOptionalNonNegativeSafeInteger(
    record.lastReferenceSourceTimestamp,
    "health.lastReferenceSourceTimestamp",
  );
  assertOptionalNumber(record.finalReferenceAgeMs, "health.finalReferenceAgeMs");
  assertOptionalNumber(record.finalReferenceSourceAgeMs, "health.finalReferenceSourceAgeMs");
  assertBoolean(record.referenceFresh, "health.referenceFresh");
  const referenceClockSkewMs = assertPositiveNumber(
    record.referenceClockSkewMs,
    "health.referenceClockSkewMs",
  );
  const RECORDER_REFERENCE_CLOCK_SKEW_MS = 5_000;
  if (referenceClockSkewMs !== RECORDER_REFERENCE_CLOCK_SKEW_MS) {
    throw new Error(
      `health.referenceClockSkewMs is ${referenceClockSkewMs}, expected ${RECORDER_REFERENCE_CLOCK_SKEW_MS} (recorder policy is not configurable)`,
    );
  }
  assertNonNegativeSafeInteger(
    record.referenceTimestampAnomalyCount,
    "health.referenceTimestampAnomalyCount",
  );
  assertNonNegativeSafeInteger(
    record.referenceTimestampMissingCount,
    "health.referenceTimestampMissingCount",
  );
  assertNonNegativeSafeInteger(
    record.referenceStaleAtReceiptCount,
    "health.referenceStaleAtReceiptCount",
  );
  assertNonNegativeSafeInteger(
    record.referenceFutureSourceSkewCount,
    "health.referenceFutureSourceSkewCount",
  );
  return raw as HealthSummary;
}

export function runtimeValidateEvent(raw: unknown, index: number): RawEventEnvelope {
  const record = assertRecord(raw, `events.jsonl line ${index + 1}`);
  if (record.schemaVersion !== 2) {
    throw new Error(
      `events.jsonl line ${index + 1}: schemaVersion is ${String(record.schemaVersion)}, expected 2`,
    );
  }
  assertString(record.runId, `events.jsonl line ${index + 1}: runId`);
  assertNonNegativeSafeInteger(record.sequence, `events.jsonl line ${index + 1}: sequence`);
  assertEnum(record.source, EVENT_SOURCES, `events.jsonl line ${index + 1}: source`);
  assertString(record.eventType, `events.jsonl line ${index + 1}: eventType`);
  if (record.marketId !== null)
    assertString(record.marketId, `events.jsonl line ${index + 1}: marketId`);
  if (record.conditionId !== null)
    assertString(record.conditionId, `events.jsonl line ${index + 1}: conditionId`);
  if (record.sourceMarketId !== null)
    assertString(record.sourceMarketId, `events.jsonl line ${index + 1}: sourceMarketId`);
  if (record.tokenId !== null)
    assertString(record.tokenId, `events.jsonl line ${index + 1}: tokenId`);
  assertOptionalStringArray(
    record.observedTokenIds,
    `events.jsonl line ${index + 1}: observedTokenIds`,
  );
  assertOptionalNumber(record.sourceTimestamp, `events.jsonl line ${index + 1}: sourceTimestamp`);
  assertOptionalNumber(
    record.topLevelSourceTimestamp,
    `events.jsonl line ${index + 1}: topLevelSourceTimestamp`,
  );
  assertOptionalNumber(
    record.payloadSourceTimestamp,
    `events.jsonl line ${index + 1}: payloadSourceTimestamp`,
  );
  assertNumber(record.receivedAt, `events.jsonl line ${index + 1}: receivedAt`);
  assertNumber(record.processedAt, `events.jsonl line ${index + 1}: processedAt`);
  assertOptionalNonNegativeSafeInteger(
    record.connectionGeneration,
    `events.jsonl line ${index + 1}: connectionGeneration`,
  );
  if (record.frameId !== null)
    assertString(record.frameId, `events.jsonl line ${index + 1}: frameId`);
  assertOptionalNonNegativeSafeInteger(
    record.messageIndex,
    `events.jsonl line ${index + 1}: messageIndex`,
  );
  assertOptionalNonNegativeSafeInteger(
    record.messageCount,
    `events.jsonl line ${index + 1}: messageCount`,
  );
  assertBoolean(record.isDuplicate, `events.jsonl line ${index + 1}: isDuplicate`);
  assertEnum(record.dedupeState, DEDUPE_STATES, `events.jsonl line ${index + 1}: dedupeState`);
  assertOptionalNonNegativeSafeInteger(
    record.duplicateOfSequence,
    `events.jsonl line ${index + 1}: duplicateOfSequence`,
  );
  if (record.dedupeReason !== null)
    assertString(record.dedupeReason, `events.jsonl line ${index + 1}: dedupeReason`);
  if (record.rawText !== null)
    assertString(record.rawText, `events.jsonl line ${index + 1}: rawText`);
  if (record.rawMessageText !== null)
    assertString(record.rawMessageText, `events.jsonl line ${index + 1}: rawMessageText`);
  const label = `events.jsonl line ${index + 1}`;
  const frameId = record.frameId as string | null;
  const messageIndex = record.messageIndex as number | null;
  const messageCount = record.messageCount as number | null;
  const hasFrameId = frameId !== null;
  const hasMessageIndex = messageIndex !== null;
  const hasMessageCount = messageCount !== null;
  if (hasFrameId !== hasMessageIndex || hasFrameId !== hasMessageCount) {
    throw new Error(
      `${label}: frameId, messageIndex, messageCount must be all null or all non-null (got frameId=${String(frameId)}, messageIndex=${String(messageIndex)}, messageCount=${String(messageCount)})`,
    );
  }
  if (hasMessageCount && messageCount <= 0) {
    throw new Error(`${label}: messageCount must be > 0 for framed messages, got ${messageCount}`);
  }
  const source = record.source as string;
  const eventType = record.eventType as string;
  const isFeedDerivedEvent =
    source === "POLYMARKET_CLOB" || source === "POLYMARKET_REFERENCE_PRICE";
  const isLifecycleEvent =
    eventType === "DATA_GAP_STARTED" ||
    eventType === "DATA_GAP_ENDED" ||
    eventType === "CONNECTION_ATTEMPT" ||
    eventType === "CONNECTION_OPEN" ||
    eventType === "CONNECTION_CLOSED" ||
    eventType === "SUBSCRIPTION_SENT" ||
    eventType === "STALE_STREAM" ||
    eventType === "RECONNECT_SCHEDULED";
  if (isFeedDerivedEvent && !isLifecycleEvent) {
    const gen = record.connectionGeneration as number | null;
    if (gen === null || gen <= 0) {
      throw new Error(
        `${label}: connectionGeneration must be > 0 for feed-derived events, got ${String(gen)}`,
      );
    }
  }
  const receivedAt = record.receivedAt as number;
  const processedAt = record.processedAt as number;
  if (processedAt < receivedAt) {
    throw new Error(
      `${label}: processedAt ${processedAt} is before receivedAt ${receivedAt} (local clock regression)`,
    );
  }
  const dedupeState = record.dedupeState as string;
  const isDuplicate = record.isDuplicate as boolean;
  const duplicateOfSequence = record.duplicateOfSequence as number | null;
  const sequence = record.sequence as number;
  if (dedupeState === "UNIQUE_APPLIED") {
    if (isDuplicate) {
      throw new Error(`${label}: UNIQUE_APPLIED requires isDuplicate=false, got ${isDuplicate}`);
    }
    if (duplicateOfSequence !== null) {
      throw new Error(
        `${label}: UNIQUE_APPLIED requires duplicateOfSequence=null, got ${duplicateOfSequence}`,
      );
    }
  } else if (dedupeState === "DUPLICATE_APPLIED") {
    if (!isDuplicate) {
      throw new Error(`${label}: DUPLICATE_APPLIED requires isDuplicate=true, got ${isDuplicate}`);
    }
    if (
      duplicateOfSequence === null ||
      !Number.isSafeInteger(duplicateOfSequence) ||
      duplicateOfSequence <= 0
    ) {
      throw new Error(
        `${label}: DUPLICATE_APPLIED requires duplicateOfSequence to be a positive prior sequence, got ${String(duplicateOfSequence)}`,
      );
    }
    if (duplicateOfSequence >= sequence) {
      throw new Error(
        `${label}: DUPLICATE_APPLIED duplicateOfSequence ${duplicateOfSequence} must be less than sequence ${sequence}`,
      );
    }
  } else if (dedupeState === "RECEIVED_NOT_APPLIED") {
    if (isDuplicate) {
      throw new Error(
        `${label}: RECEIVED_NOT_APPLIED requires isDuplicate=false, got ${isDuplicate}`,
      );
    }
    if (duplicateOfSequence !== null) {
      throw new Error(
        `${label}: RECEIVED_NOT_APPLIED requires duplicateOfSequence=null, got ${duplicateOfSequence}`,
      );
    }
  }
  if (messageIndex !== null && messageCount !== null && messageIndex >= messageCount) {
    throw new Error(
      `${label}: messageIndex ${messageIndex} must be less than messageCount ${messageCount}`,
    );
  }
  validateJsonValue(record.payload, `${label}.payload`);
  return raw as RawEventEnvelope;
}

function validateJsonValue(value: unknown, label: string): void {
  if (value === null || typeof value === "boolean" || typeof value === "string") return;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error(`${label} contains non-finite number`);
    }
    return;
  }
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i += 1) {
      validateJsonValue(value[i], `${label}[${i}]`);
    }
    return;
  }
  if (typeof value === "object") {
    for (const [key, val] of Object.entries(value as Record<string, unknown>)) {
      validateJsonValue(val, `${label}.${key}`);
    }
    return;
  }
  throw new Error(`${label} contains unsupported value type: ${typeof value}`);
}

export type ClobReplayResult = {
  snapshot: Record<string, BookHealth>;
  appliedBookGenerations: Map<string, number>;
  latestAcceptedClobGeneration: number | null;
  resyncPending: boolean;
  errors: string[];
};

export function replayClobState(
  events: RawEventEnvelope[],
  conditionId: string,
  tokenIds: string[],
  endedAtMs: number,
  staleAfterMs: number,
): ClobReplayResult {
  const expectedTokenSet = new Set(tokenIds);
  const replayBooks = new OrderBookTracker(tokenIds);
  const appliedBookGenerations = new Map<string, number>();
  let latestAcceptedClobGeneration: number | null = null;
  let replayResyncPending = false;
  const errors: string[] = [];
  const clobLifecycleEventTypes = new Set([
    "CONNECTION_ATTEMPT",
    "CONNECTION_OPEN",
    "CONNECTION_CLOSED",
    "SUBSCRIPTION_SENT",
    "RECONNECT_SCHEDULED",
    "DATA_GAP_STARTED",
    "DATA_GAP_ENDED",
    "STALE_STREAM",
    "EMPTY_FRAME",
    "IGNORED_FOREIGN_MARKET",
    "MALFORMED_EVENT",
    "PONG",
    "PING",
  ]);

  for (const event of events) {
    if (
      event.source === "POLYMARKET_CLOB" &&
      !event.isDuplicate &&
      event.dedupeState === "UNIQUE_APPLIED"
    ) {
      if (event.connectionGeneration !== null) {
        if (
          latestAcceptedClobGeneration === null ||
          event.connectionGeneration > latestAcceptedClobGeneration
        ) {
          latestAcceptedClobGeneration = event.connectionGeneration;
        }
      }
    }

    // CLOB feed resync lifecycle: set pending on gap/stale, clear on DATA_GAP_ENDED.
    if (event.source === "POLYMARKET_CLOB" && event.eventType === "DATA_GAP_ENDED") {
      const feed = (event.payload as Record<string, unknown>).feed;
      if (feed === "clob-market") {
        replayResyncPending = false;
      }
    }
    if (
      event.source === "POLYMARKET_CLOB" &&
      (event.eventType === "DATA_GAP_STARTED" || event.eventType === "STALE_STREAM") &&
      (event.payload as Record<string, unknown>).feed === "clob-market"
    ) {
      if (!replayResyncPending) {
        replayResyncPending = true;
        replayBooks.requireResync();
      }
    }

    if (
      event.source === "POLYMARKET_CLOB" &&
      !event.isDuplicate &&
      event.dedupeState === "UNIQUE_APPLIED"
    ) {
      // Lifecycle events carry feed state, not market payloads; skip generic data checks.
      if (clobLifecycleEventTypes.has(event.eventType)) continue;

      // Generic CLOB applied-event identity
      if (event.sourceMarketId === null) {
        errors.push(`event sequence ${event.sequence}: sourceMarketId is null for CLOB data event`);
        continue;
      }
      if (
        typeof event.payload !== "object" ||
        event.payload === null ||
        Array.isArray(event.payload)
      ) {
        errors.push(
          `event sequence ${event.sequence}: CLOB data payload must be an object, got ${typeof event.payload}`,
        );
        continue;
      }
      const payload = event.payload as Record<string, unknown>;
      const payloadEventType = payload.event_type;
      if (payloadEventType !== event.eventType) {
        errors.push(
          `event sequence ${event.sequence}: payload.event_type ${String(payloadEventType)} does not match envelope.eventType ${event.eventType}`,
        );
        continue;
      }
      const payloadMarket = payload.market;
      if (payloadMarket !== event.sourceMarketId) {
        errors.push(
          `event sequence ${event.sequence}: payload.market ${String(payloadMarket)} does not match envelope.sourceMarketId ${event.sourceMarketId}`,
        );
        continue;
      }
      if (payloadMarket !== conditionId) {
        errors.push(
          `event sequence ${event.sequence}: payload.market ${payloadMarket} does not match conditionId ${conditionId}`,
        );
        continue;
      }

      if (event.eventType === "book") {
        if (event.tokenId === null || !expectedTokenSet.has(event.tokenId)) {
          errors.push(
            `book event sequence ${event.sequence}: tokenId ${String(event.tokenId)} is not an expected token`,
          );
          continue;
        }
        if (event.observedTokenIds !== null) {
          errors.push(
            `book event sequence ${event.sequence}: observedTokenIds must be null for a full snapshot, got ${JSON.stringify(event.observedTokenIds)}`,
          );
          continue;
        }
        const assetId = payload.asset_id;
        if (assetId !== event.tokenId) {
          errors.push(
            `book event sequence ${event.sequence}: payload.asset_id ${String(assetId)} does not match tokenId ${event.tokenId}`,
          );
          continue;
        }
        try {
          replayBooks.apply(event.payload, event.processedAt, event.connectionGeneration ?? 0);
          appliedBookGenerations.set(event.tokenId, event.connectionGeneration ?? 0);
        } catch (replayError) {
          errors.push(
            `replay failed for book event sequence ${event.sequence}: ${replayError instanceof Error ? replayError.message : String(replayError)}`,
          );
        }
      } else if (event.eventType === "price_change") {
        const payloadTokens = new Set<string>();
        if (typeof payload.asset_id === "string") payloadTokens.add(payload.asset_id);
        if (Array.isArray(payload.price_changes)) {
          for (const item of payload.price_changes) {
            const record =
              item !== null && typeof item === "object" ? (item as Record<string, unknown>) : null;
            if (record !== null && typeof record.asset_id === "string") {
              payloadTokens.add(record.asset_id);
            }
          }
        }
        const payloadTokenSet = [...payloadTokens];
        if (payloadTokenSet.length === 1) {
          const token = payloadTokenSet[0];
          if (event.tokenId !== token) {
            errors.push(
              `price_change event sequence ${event.sequence}: envelope.tokenId ${String(event.tokenId)} does not match single payload token ${token}`,
            );
            continue;
          }
          if (event.observedTokenIds !== null) {
            errors.push(
              `price_change event sequence ${event.sequence}: observedTokenIds must be null for single-token payload, got ${JSON.stringify(event.observedTokenIds)}`,
            );
            continue;
          }
        } else if (payloadTokenSet.length > 1) {
          if (event.tokenId !== null) {
            errors.push(
              `price_change event sequence ${event.sequence}: envelope.tokenId must be null for multi-token payload, got ${event.tokenId}`,
            );
            continue;
          }
          const observed =
            event.observedTokenIds !== null ? [...event.observedTokenIds].sort() : [];
          const sortedPayload = [...payloadTokenSet].sort();
          if (
            observed.length !== sortedPayload.length ||
            observed.some((t, i) => t !== sortedPayload[i])
          ) {
            errors.push(
              `price_change event sequence ${event.sequence}: observedTokenIds ${JSON.stringify(event.observedTokenIds)} does not match payload token set ${JSON.stringify(sortedPayload)}`,
            );
            continue;
          }
        } else {
          errors.push(
            `price_change event sequence ${event.sequence}: payload has no asset_id tokens`,
          );
          continue;
        }
        try {
          replayBooks.apply(event.payload, event.processedAt, event.connectionGeneration ?? 0);
        } catch (replayError) {
          errors.push(
            `replay failed for price_change event sequence ${event.sequence}: ${replayError instanceof Error ? replayError.message : String(replayError)}`,
          );
        }
      } else {
        // Other CLOB event types (e.g. last_trade_price) only need the generic identity checks
        try {
          replayBooks.apply(event.payload, event.processedAt, event.connectionGeneration ?? 0);
        } catch (replayError) {
          errors.push(
            `replay failed for ${event.eventType} event sequence ${event.sequence}: ${replayError instanceof Error ? replayError.message : String(replayError)}`,
          );
        }
      }
    }
  }

  return {
    snapshot: replayBooks.snapshot(endedAtMs, staleAfterMs),
    appliedBookGenerations,
    latestAcceptedClobGeneration,
    resyncPending: replayResyncPending,
    errors,
  };
}

export async function verifyRun(directory: string, minReferenceObservations = 5): Promise<void> {
  if (!Number.isSafeInteger(minReferenceObservations) || minReferenceObservations <= 0) {
    throw new Error(
      `minReferenceObservations must be a positive safe integer, got ${minReferenceObservations}`,
    );
  }
  const manifestPath = path.join(directory, "manifest.json");
  const healthPath = path.join(directory, "health.json");
  const eventsPath = path.join(directory, "events.jsonl");

  let manifestRaw: unknown;
  let healthRaw: unknown;
  try {
    manifestRaw = JSON.parse(await readFile(manifestPath, "utf8"));
  } catch {
    throw new Error(`Cannot read manifest at ${manifestPath}`);
  }
  try {
    healthRaw = JSON.parse(await readFile(healthPath, "utf8"));
  } catch {
    throw new Error(`Cannot read health at ${healthPath}`);
  }

  let manifest: RunManifest;
  let health: HealthSummary;
  try {
    manifest = runtimeValidateManifest(manifestRaw);
  } catch (error) {
    throw new Error(
      `Manifest validation failed: ${error instanceof Error ? error.message : String(error)}`,
      { cause: error },
    );
  }
  try {
    health = runtimeValidateHealth(healthRaw);
  } catch (error) {
    throw new Error(
      `Health validation failed: ${error instanceof Error ? error.message : String(error)}`,
      { cause: error },
    );
  }

  const errors: string[] = [];

  if (manifest.completionStatus !== "COMPLETED") {
    errors.push(`completionStatus is ${manifest.completionStatus}, expected COMPLETED`);
  }
  if (manifest.captureStatus !== "COMPLETE") {
    errors.push(`captureStatus is ${manifest.captureStatus}, expected COMPLETE`);
  }
  if (!manifest.usableForReplay) {
    errors.push("usableForReplay is false in manifest");
  }
  if (manifest.failureReason !== null) {
    errors.push(`failureReason is non-null: ${manifest.failureReason}`);
  }
  if (manifest.finalEventCount === null || manifest.finalEventCount <= 0) {
    errors.push(`finalEventCount is ${manifest.finalEventCount}, expected a positive number`);
  }
  if (manifest.endedAt === null) {
    errors.push("manifest.endedAt is null on a completed run, expected a valid ISO timestamp");
  } else {
    assertIsoTimestamp(manifest.endedAt, "manifest.endedAt");
  }
  if (health.endedAt === null) {
    errors.push("health.endedAt is null on a completed run, expected a valid ISO timestamp");
  }

  if (health.runId !== manifest.runId) {
    errors.push(`health.runId ${health.runId} does not match manifest.runId ${manifest.runId}`);
  }
  if (health.startedAt !== manifest.startedAt) {
    errors.push(
      `health.startedAt ${health.startedAt} does not match manifest.startedAt ${manifest.startedAt}`,
    );
  }
  if (manifest.endedAt !== null && health.endedAt !== null) {
    if (manifest.endedAt !== health.endedAt) {
      errors.push(
        `manifest.endedAt ${manifest.endedAt} does not match health.endedAt ${health.endedAt}`,
      );
    }
    if (Date.parse(health.endedAt) < Date.parse(manifest.startedAt)) {
      errors.push(
        `health.endedAt ${health.endedAt} is before manifest.startedAt ${manifest.startedAt}`,
      );
    }
  }
  if (health.clobStaleAfterMs !== manifest.clobStaleAfterMs) {
    errors.push(
      `health.clobStaleAfterMs ${health.clobStaleAfterMs} does not match manifest.clobStaleAfterMs ${manifest.clobStaleAfterMs}`,
    );
  }
  if (health.referenceStaleAfterMs !== manifest.referenceStaleAfterMs) {
    errors.push(
      `health.referenceStaleAfterMs ${health.referenceStaleAfterMs} does not match manifest.referenceStaleAfterMs ${manifest.referenceStaleAfterMs}`,
    );
  }
  if (health.captureStatus !== manifest.captureStatus) {
    errors.push(
      `health.captureStatus ${health.captureStatus} does not match manifest.captureStatus ${manifest.captureStatus}`,
    );
  }
  if (health.usableForReplay !== manifest.usableForReplay) {
    errors.push(
      `health.usableForReplay ${health.usableForReplay} does not match manifest.usableForReplay ${manifest.usableForReplay}`,
    );
  }
  const manifestFailureCodes = new Set(manifest.qualityFailures.map((f) => f.code));
  const healthFailureCodes = new Set(health.qualityFailures.map((f) => f.code));
  if (manifestFailureCodes.size !== healthFailureCodes.size) {
    errors.push(
      `manifest.qualityFailures codes ${[...manifestFailureCodes].join(", ")} do not match health.qualityFailures codes ${[...healthFailureCodes].join(", ")}`,
    );
  } else {
    for (const code of manifestFailureCodes) {
      if (!healthFailureCodes.has(code)) {
        errors.push(
          `manifest.qualityFailures codes do not match health.qualityFailures codes (missing ${code})`,
        );
        break;
      }
    }
  }
  if (health.captureStatus !== "COMPLETE") {
    errors.push(`health captureStatus is ${health.captureStatus}, expected COMPLETE`);
  }
  if (!health.usableForReplay) {
    errors.push("usableForReplay is false in health");
  }
  if (!health.bothTokenStreamsObserved) {
    errors.push("bothTokenStreamsObserved is false");
  }
  if (!health.referencePriceStreamObserved) {
    errors.push("referencePriceStreamObserved is false");
  }
  if (!health.referenceFresh) {
    errors.push("referenceFresh is false");
  }
  if (health.referenceTimestampAnomalyCount > 0) {
    errors.push(
      `referenceTimestampAnomalyCount is ${health.referenceTimestampAnomalyCount}, expected 0`,
    );
  }
  if (health.malformedEventCount > 0) {
    errors.push(`malformedEventCount is ${health.malformedEventCount}, expected 0`);
  }
  if (health.qualityFailures.length > 0) {
    const codes = health.qualityFailures.map((f) => f.code).join(", ");
    errors.push(`qualityFailures is non-empty: ${codes}`);
  }
  const expectedTokenIds = new Set([manifest.market.upTokenId, manifest.market.downTokenId]);
  for (const [tokenId, book] of Object.entries(health.finalBookHealth)) {
    if (book.state !== "SYNCHRONIZED") {
      errors.push(`book for token ${tokenId} state is ${book.state}, expected SYNCHRONIZED`);
    }
    if (!book.healthy) {
      errors.push(`book for token ${tokenId} is not healthy`);
    }
    if (book.integrity === "CROSSED") {
      errors.push(
        `book for token ${tokenId} is CROSSED (bid ${book.bestBid} > ask ${book.bestAsk})`,
      );
    }
    if (book.crossedIncidentCount > 0) {
      errors.push(`book for token ${tokenId} had ${book.crossedIncidentCount} crossed incident(s)`);
    }
    if (!expectedTokenIds.has(tokenId)) {
      errors.push(`book health has unexpected token ID ${tokenId}`);
    }
  }
  for (const expected of expectedTokenIds) {
    if (!(expected in health.finalBookHealth)) {
      errors.push(`missing book health for expected token ID ${expected}`);
    }
  }
  for (const [tokenId, book] of Object.entries(health.finalBookHealth)) {
    if (book.tokenId !== tokenId) {
      errors.push(`finalBookHealth map key ${tokenId} does not match book.tokenId ${book.tokenId}`);
    }
  }

  let eventsText: string;
  try {
    eventsText = await readFile(eventsPath, "utf8");
  } catch {
    throw new Error(`Cannot read events at ${eventsPath}`);
  }
  const lines = eventsText.trim().split("\n");
  if (lines.length === 0 || (lines.length === 1 && lines[0] === "")) {
    errors.push("events.jsonl is empty");
  } else {
    const events: RawEventEnvelope[] = [];
    let parseErrors = 0;
    for (let i = 0; i < lines.length; i += 1) {
      const line = lines[i];
      if (line === undefined || line === "") {
        errors.push(`events.jsonl line ${i + 1} is empty`);
        parseErrors += 1;
        continue;
      }
      try {
        const parsed = JSON.parse(line) as unknown;
        events.push(runtimeValidateEvent(parsed, i));
      } catch (error) {
        errors.push(
          `events.jsonl line ${i + 1}: ${error instanceof Error ? error.message : "parse error"}`,
        );
        parseErrors += 1;
      }
    }
    if (parseErrors === 0) {
      if (events.length !== manifest.finalEventCount) {
        errors.push(
          `events.jsonl line count ${events.length} does not match manifest.finalEventCount ${manifest.finalEventCount}`,
        );
      }
      if (events.length !== health.totalEventsReceived) {
        errors.push(
          `events.jsonl line count ${events.length} does not match health.totalEventsReceived ${health.totalEventsReceived}`,
        );
      }
      let expectedSeq = 1;
      for (let i = 0; i < events.length; i += 1) {
        const event = events[i];
        if (event === undefined) continue;
        if (event.sequence !== expectedSeq) {
          errors.push(
            `events.jsonl line ${i + 1}: sequence is ${event.sequence}, expected ${expectedSeq}`,
          );
          break;
        }
        expectedSeq += 1;
      }
      if (events.length > 0) {
        const lastEvent = events[events.length - 1];
        if (lastEvent !== undefined && lastEvent.sequence !== events.length) {
          errors.push(
            `final sequence ${lastEvent.sequence} does not match line count ${events.length}`,
          );
        }
        for (const event of events) {
          if (event.runId !== manifest.runId) {
            errors.push(
              `event sequence ${event.sequence} runId ${event.runId} does not match manifest runId ${manifest.runId}`,
            );
            break;
          }
          if (event.marketId !== manifest.market.marketId) {
            errors.push(
              `event sequence ${event.sequence} marketId ${event.marketId} does not match manifest marketId ${manifest.market.marketId}`,
            );
            break;
          }
          if (event.conditionId !== manifest.market.conditionId) {
            errors.push(
              `event sequence ${event.sequence} conditionId ${event.conditionId} does not match manifest conditionId ${manifest.market.conditionId}`,
            );
            break;
          }
        }
      }
      const recomputedSourceCounts: Record<string, number> = {};
      const recomputedTypeCounts: Record<string, number> = {};
      let recomputedMalformed = 0;
      let recomputedDuplicates = 0;
      let recomputedDataGapCount = 0;
      let recomputedClosedGapCount = 0;
      let recomputedTotalGapDurationMs = 0;
      let recomputedMaxGapDurationMs = 0;
      let recomputedStaleFeedIncidents = 0;
      let recomputedReconnectCount = 0;
      const openGaps = new Map<string, number>();
      for (const event of events) {
        recomputedSourceCounts[event.source] = (recomputedSourceCounts[event.source] ?? 0) + 1;
        recomputedTypeCounts[`${event.source}:${event.eventType}`] =
          (recomputedTypeCounts[`${event.source}:${event.eventType}`] ?? 0) + 1;
        if (event.eventType === "MALFORMED_EVENT") recomputedMalformed += 1;
        if (event.isDuplicate) recomputedDuplicates += 1;
        if (event.eventType === "STALE_STREAM") recomputedStaleFeedIncidents += 1;
        if (event.eventType === "RECONNECT_SCHEDULED") recomputedReconnectCount += 1;
        if (event.eventType === "DATA_GAP_STARTED") {
          const feed = (event.payload as Record<string, unknown>).feed;
          if (typeof feed === "string") {
            openGaps.set(feed, event.receivedAt);
            recomputedDataGapCount += 1;
          }
        }
        if (event.eventType === "DATA_GAP_ENDED") {
          const feed = (event.payload as Record<string, unknown>).feed;
          if (typeof feed === "string") {
            const startedAt = openGaps.get(feed);
            if (startedAt !== undefined) {
              const duration = Math.max(0, event.receivedAt - startedAt);
              recomputedTotalGapDurationMs += duration;
              if (duration > recomputedMaxGapDurationMs) recomputedMaxGapDurationMs = duration;
              recomputedClosedGapCount += 1;
            }
            openGaps.delete(feed);
          }
        }
      }
      if (recomputedMalformed !== health.malformedEventCount) {
        errors.push(
          `recomputed malformed count ${recomputedMalformed} does not match health.malformedEventCount ${health.malformedEventCount}`,
        );
      }
      if (recomputedDuplicates !== health.duplicateEventCount) {
        errors.push(
          `recomputed duplicate count ${recomputedDuplicates} does not match health.duplicateEventCount ${health.duplicateEventCount}`,
        );
      }
      const healthSourceKeys = Object.keys(health.eventCountsBySource).sort();
      const recomputedSourceKeys = Object.keys(recomputedSourceCounts).sort();
      if (JSON.stringify(healthSourceKeys) !== JSON.stringify(recomputedSourceKeys)) {
        errors.push(
          `recomputed eventCountsBySource keys ${recomputedSourceKeys.join(", ")} do not match health keys ${healthSourceKeys.join(", ")}`,
        );
      } else {
        for (const key of recomputedSourceKeys) {
          if (recomputedSourceCounts[key] !== health.eventCountsBySource[key]) {
            errors.push(
              `recomputed eventCountsBySource.${key} ${recomputedSourceCounts[key]} does not match health ${health.eventCountsBySource[key]}`,
            );
          }
        }
      }
      const healthTypeKeys = Object.keys(health.eventCountsByType).sort();
      const recomputedTypeKeys = Object.keys(recomputedTypeCounts).sort();
      if (JSON.stringify(healthTypeKeys) !== JSON.stringify(recomputedTypeKeys)) {
        errors.push(
          `recomputed eventCountsByType keys ${recomputedTypeKeys.join(", ")} do not match health keys ${healthTypeKeys.join(", ")}`,
        );
      } else {
        for (const key of recomputedTypeKeys) {
          if (recomputedTypeCounts[key] !== health.eventCountsByType[key]) {
            errors.push(
              `recomputed eventCountsByType.${key} ${recomputedTypeCounts[key]} does not match health ${health.eventCountsByType[key]}`,
            );
          }
        }
      }
      if (recomputedDataGapCount !== health.dataGapCount) {
        errors.push(
          `recomputed dataGapCount ${recomputedDataGapCount} does not match health.dataGapCount ${health.dataGapCount}`,
        );
      }
      if (recomputedClosedGapCount !== health.closedGapCount) {
        errors.push(
          `recomputed closedGapCount ${recomputedClosedGapCount} does not match health.closedGapCount ${health.closedGapCount}`,
        );
      }
      if (recomputedTotalGapDurationMs !== health.totalGapDurationMs) {
        errors.push(
          `recomputed totalGapDurationMs ${recomputedTotalGapDurationMs} does not match health.totalGapDurationMs ${health.totalGapDurationMs}`,
        );
      }
      if (recomputedMaxGapDurationMs !== health.maximumGapDurationMs) {
        errors.push(
          `recomputed maximumGapDurationMs ${recomputedMaxGapDurationMs} does not match health.maximumGapDurationMs ${health.maximumGapDurationMs}`,
        );
      }
      if (recomputedStaleFeedIncidents !== health.staleFeedIncidents) {
        errors.push(
          `recomputed staleFeedIncidents ${recomputedStaleFeedIncidents} does not match health.staleFeedIncidents ${health.staleFeedIncidents}`,
        );
      }
      if (recomputedReconnectCount !== health.reconnectCount) {
        errors.push(
          `recomputed reconnectCount ${recomputedReconnectCount} does not match health.reconnectCount ${health.reconnectCount}`,
        );
      }
      const healthOpenGaps = Object.entries(health.openGapsByFeed).sort(([a], [b]) =>
        a.localeCompare(b),
      );
      const recomputedOpenGaps = [...openGaps.entries()].sort(([a], [b]) => a.localeCompare(b));
      if (healthOpenGaps.length !== recomputedOpenGaps.length) {
        errors.push(
          `recomputed open gap count ${recomputedOpenGaps.length} does not match health.openGapsByFeed count ${healthOpenGaps.length}`,
        );
      } else {
        for (let i = 0; i < healthOpenGaps.length; i += 1) {
          const healthOpenGap = healthOpenGaps[i];
          const recomputedOpenGap = recomputedOpenGaps[i];
          if (healthOpenGap === undefined || recomputedOpenGap === undefined) {
            break;
          }
          const [healthFeed, healthGap] = healthOpenGap;
          const [recomputedFeed, recomputedStartedAt] = recomputedOpenGap;
          if (healthFeed !== recomputedFeed) {
            errors.push(
              `recomputed open gap feed ${recomputedFeed} does not match health ${healthFeed}`,
            );
            break;
          }
          if (healthGap.startedAt !== recomputedStartedAt) {
            errors.push(
              `recomputed open gap ${recomputedFeed} startedAt ${recomputedStartedAt} does not match health ${healthGap.startedAt}`,
            );
            break;
          }
        }
      }
      if (recomputedDataGapCount > 0) {
        errors.push(
          `recomputed dataGapCount ${recomputedDataGapCount} > 0; run contains at least one DATA_GAP_STARTED event`,
        );
      }
      if (recomputedClosedGapCount > 0) {
        errors.push(
          `recomputed closedGapCount ${recomputedClosedGapCount} > 0; run contains at least one DATA_GAP_ENDED event`,
        );
      }
      if (recomputedOpenGaps.length > 0) {
        errors.push(
          `recomputed openGapsByFeed is non-empty: ${recomputedOpenGaps.map(([feed]) => feed).join(", ")}`,
        );
      }
      const referenceObs = new Set<number>();
      let lastRefReceivedAt: number | null = null;
      let lastRefSourceTimestamp: number | null = null;
      let recomputedMissing = 0;
      let recomputedStaleAtReceipt = 0;
      let recomputedFutureSkew = 0;
      const recomputedAnomalyCodes = new Set<QualityFailureCode>();
      for (const event of events) {
        if (
          event.eventType === "REFERENCE_PRICE" &&
          !event.isDuplicate &&
          event.dedupeState === "UNIQUE_APPLIED"
        ) {
          if (event.sourceTimestamp !== null) {
            referenceObs.add(event.sourceTimestamp);
            lastRefSourceTimestamp = event.sourceTimestamp;
            const sourceAge = event.receivedAt - event.sourceTimestamp;
            if (sourceAge < -health.referenceClockSkewMs) {
              recomputedFutureSkew += 1;
              recomputedAnomalyCodes.add("REFERENCE_CLOCK_SKEW");
            } else if (sourceAge > health.referenceStaleAfterMs) {
              recomputedStaleAtReceipt += 1;
              recomputedAnomalyCodes.add("REFERENCE_STREAM_STALE");
            }
          } else {
            recomputedMissing += 1;
            recomputedAnomalyCodes.add("REFERENCE_TIMESTAMP_MISSING");
          }
          lastRefReceivedAt = event.receivedAt;
        }
      }
      const recomputedAnomalyCount =
        recomputedMissing + recomputedStaleAtReceipt + recomputedFutureSkew;
      if (referenceObs.size < minReferenceObservations) {
        errors.push(
          `unique Chainlink reference observations ${referenceObs.size} < minimum ${minReferenceObservations}`,
        );
      }
      if (recomputedAnomalyCount !== health.referenceTimestampAnomalyCount) {
        errors.push(
          `recomputed reference anomaly count ${recomputedAnomalyCount} does not match health.referenceTimestampAnomalyCount ${health.referenceTimestampAnomalyCount}`,
        );
      }
      if (recomputedMissing !== health.referenceTimestampMissingCount) {
        errors.push(
          `recomputed missing count ${recomputedMissing} does not match health.referenceTimestampMissingCount ${health.referenceTimestampMissingCount}`,
        );
      }
      if (recomputedStaleAtReceipt !== health.referenceStaleAtReceiptCount) {
        errors.push(
          `recomputed stale-at-receipt count ${recomputedStaleAtReceipt} does not match health.referenceStaleAtReceiptCount ${health.referenceStaleAtReceiptCount}`,
        );
      }
      if (recomputedFutureSkew !== health.referenceFutureSourceSkewCount) {
        errors.push(
          `recomputed future-skew count ${recomputedFutureSkew} does not match health.referenceFutureSourceSkewCount ${health.referenceFutureSourceSkewCount}`,
        );
      }
      const healthAnomalyCodes = new Set<QualityFailureCode>();
      if (health.referenceTimestampMissingCount > 0)
        healthAnomalyCodes.add("REFERENCE_TIMESTAMP_MISSING");
      if (health.referenceStaleAtReceiptCount > 0) healthAnomalyCodes.add("REFERENCE_STREAM_STALE");
      if (health.referenceFutureSourceSkewCount > 0) healthAnomalyCodes.add("REFERENCE_CLOCK_SKEW");
      if (recomputedAnomalyCodes.size !== healthAnomalyCodes.size) {
        errors.push(
          `recomputed anomaly codes ${[...recomputedAnomalyCodes].join(", ")} do not match health anomaly codes ${[...healthAnomalyCodes].join(", ")}`,
        );
      } else {
        for (const code of recomputedAnomalyCodes) {
          if (!healthAnomalyCodes.has(code)) {
            errors.push(
              `recomputed anomaly codes do not match health anomaly codes (missing ${code})`,
            );
            break;
          }
        }
      }
      for (const code of recomputedAnomalyCodes) {
        if (!healthFailureCodes.has(code)) {
          errors.push(`recomputed anomaly code ${code} is not present in health.qualityFailures`);
        }
        if (!manifestFailureCodes.has(code)) {
          errors.push(`recomputed anomaly code ${code} is not present in manifest.qualityFailures`);
        }
      }
      if (health.endedAt !== null) {
        const endedAtMs = Date.parse(health.endedAt);
        if (Number.isNaN(endedAtMs)) {
          errors.push(`health.endedAt is not a valid timestamp: ${health.endedAt}`);
        } else if (lastRefReceivedAt !== null) {
          const receiptAge = endedAtMs - lastRefReceivedAt;
          if (receiptAge < 0) {
            errors.push(
              `recomputed reference receipt age ${receiptAge}ms is negative (local clock skew)`,
            );
          } else if (receiptAge > health.referenceStaleAfterMs) {
            errors.push(
              `recomputed reference receipt age ${receiptAge}ms exceeds staleness threshold ${health.referenceStaleAfterMs}ms`,
            );
          }
        }
        if (lastRefSourceTimestamp !== null) {
          const sourceAge = endedAtMs - lastRefSourceTimestamp;
          if (sourceAge < -health.referenceClockSkewMs) {
            errors.push(
              `recomputed reference source age ${sourceAge}ms is beyond future clock skew tolerance -${health.referenceClockSkewMs}ms`,
            );
          } else if (sourceAge > health.referenceStaleAfterMs) {
            errors.push(
              `recomputed reference source age ${sourceAge}ms exceeds staleness threshold ${health.referenceStaleAfterMs}ms`,
            );
          }
        }
        if (lastRefSourceTimestamp === null && lastRefReceivedAt !== null) {
          errors.push("recomputed reference has no canonical source timestamp");
        }
      }
      if (recomputedAnomalyCount > 0) {
        errors.push(`recomputed reference timestamp anomaly count ${recomputedAnomalyCount} > 0`);
      }
      const tokenIds = [manifest.market.upTokenId, manifest.market.downTokenId];
      let recomputedMaxSourceLatency: number | null = null;
      let recomputedMaxProcessingLatency: number | null = null;
      let recomputedLastRefReceivedAt: number | null = null;
      let recomputedLastRefSourceTimestamp: number | null = null;
      let recomputedLastRefConnectionGeneration: number | null = null;
      for (const event of events) {
        if (event.sourceTimestamp !== null) {
          const latency = event.receivedAt - event.sourceTimestamp;
          if (recomputedMaxSourceLatency === null || latency > recomputedMaxSourceLatency) {
            recomputedMaxSourceLatency = latency;
          }
        }
        const processingLatency = event.processedAt - event.receivedAt;
        if (
          recomputedMaxProcessingLatency === null ||
          processingLatency > recomputedMaxProcessingLatency
        ) {
          recomputedMaxProcessingLatency = processingLatency;
        }
        if (
          event.source === "POLYMARKET_REFERENCE_PRICE" &&
          event.eventType === "REFERENCE_PRICE" &&
          !event.isDuplicate &&
          event.dedupeState === "UNIQUE_APPLIED"
        ) {
          recomputedLastRefReceivedAt = event.receivedAt;
          if (event.sourceTimestamp !== null) {
            recomputedLastRefSourceTimestamp = event.sourceTimestamp;
          }
          if (event.connectionGeneration !== null) {
            recomputedLastRefConnectionGeneration = event.connectionGeneration;
          }
        }
      }
      const clobEndedAtMs = health.endedAt !== null ? Date.parse(health.endedAt) : 0;
      const clobReplay = replayClobState(
        events,
        manifest.market.conditionId,
        tokenIds,
        clobEndedAtMs,
        health.clobStaleAfterMs,
      );
      for (const error of clobReplay.errors) {
        errors.push(error);
      }
      for (const expected of tokenIds) {
        let found = false;
        for (const event of events) {
          if (
            event.source === "POLYMARKET_CLOB" &&
            event.eventType === "book" &&
            !event.isDuplicate &&
            event.dedupeState === "UNIQUE_APPLIED" &&
            event.tokenId === expected
          ) {
            found = true;
            break;
          }
        }
        if (!found) {
          errors.push(
            `no UNIQUE_APPLIED book snapshot for expected token ${expected} found in events.jsonl`,
          );
        }
      }
      if (manifest.captureStatus === "COMPLETE") {
        for (const expected of tokenIds) {
          const healthBook = health.finalBookHealth[expected];
          if (healthBook === undefined) continue;
          const replayGen = clobReplay.appliedBookGenerations.get(expected);
          if (replayGen !== undefined && healthBook.snapshotConnectionGeneration !== null) {
            if (replayGen !== healthBook.snapshotConnectionGeneration) {
              errors.push(
                `token ${expected}: latest applied snapshot generation ${replayGen} does not match health.snapshotConnectionGeneration ${healthBook.snapshotConnectionGeneration}`,
              );
            }
          }
          if (
            replayGen !== undefined &&
            clobReplay.latestAcceptedClobGeneration !== null &&
            replayGen !== clobReplay.latestAcceptedClobGeneration
          ) {
            errors.push(
              `token ${expected}: latest applied snapshot generation ${replayGen} does not equal latest accepted CLOB generation ${clobReplay.latestAcceptedClobGeneration}`,
            );
          }
        }
      }
      const replaySnapshot = clobReplay.snapshot;
      for (const expected of tokenIds) {
        const healthBook = health.finalBookHealth[expected];
        if (healthBook === undefined) continue;
        const replayBook = replaySnapshot[expected];
        if (replayBook === undefined) {
          errors.push(`token ${expected}: no replayed book state to compare with health`);
          continue;
        }
        const fields: [string, unknown, unknown][] = [
          ["tokenId", replayBook.tokenId, healthBook.tokenId],
          ["state", replayBook.state, healthBook.state],
          ["integrity", replayBook.integrity, healthBook.integrity],
          [
            "crossedIncidentCount",
            replayBook.crossedIncidentCount,
            healthBook.crossedIncidentCount,
          ],
          [
            "synchronizationEpoch",
            replayBook.synchronizationEpoch,
            healthBook.synchronizationEpoch,
          ],
          [
            "snapshotConnectionGeneration",
            replayBook.snapshotConnectionGeneration,
            healthBook.snapshotConnectionGeneration,
          ],
          ["bestBid", replayBook.bestBid, healthBook.bestBid],
          ["bestAsk", replayBook.bestAsk, healthBook.bestAsk],
          ["bidDepthCount", replayBook.bidDepthCount, healthBook.bidDepthCount],
          ["askDepthCount", replayBook.askDepthCount, healthBook.askDepthCount],
          [
            "lastSuccessfulUpdateTime",
            replayBook.lastSuccessfulUpdateTime,
            healthBook.lastSuccessfulUpdateTime,
          ],
          ["healthy", replayBook.healthy, healthBook.healthy],
        ];
        for (const [field, replayVal, healthVal] of fields) {
          if (replayVal !== healthVal) {
            errors.push(
              `token ${expected}: replayed ${field} ${JSON.stringify(replayVal)} does not match health ${JSON.stringify(healthVal)}`,
            );
          }
        }
      }
      if (recomputedMaxSourceLatency !== health.maximumSourceToReceiveLatencyMs) {
        errors.push(
          `recomputed maximumSourceToReceiveLatencyMs ${recomputedMaxSourceLatency} does not match health ${health.maximumSourceToReceiveLatencyMs}`,
        );
      }
      if (recomputedMaxProcessingLatency !== health.maximumReceiveToProcessLatencyMs) {
        errors.push(
          `recomputed maximumReceiveToProcessLatencyMs ${recomputedMaxProcessingLatency} does not match health ${health.maximumReceiveToProcessLatencyMs}`,
        );
      }
      if (recomputedLastRefReceivedAt !== health.lastReferenceReceivedAt) {
        errors.push(
          `recomputed lastReferenceReceivedAt ${recomputedLastRefReceivedAt} does not match health ${health.lastReferenceReceivedAt}`,
        );
      }
      if (recomputedLastRefSourceTimestamp !== health.lastReferenceSourceTimestamp) {
        errors.push(
          `recomputed lastReferenceSourceTimestamp ${recomputedLastRefSourceTimestamp} does not match health ${health.lastReferenceSourceTimestamp}`,
        );
      }
      if (recomputedLastRefConnectionGeneration !== health.lastReferenceConnectionGeneration) {
        errors.push(
          `recomputed lastReferenceConnectionGeneration ${recomputedLastRefConnectionGeneration} does not match health ${health.lastReferenceConnectionGeneration}`,
        );
      }
      if (health.endedAt !== null) {
        const endedAtMs = Date.parse(health.endedAt);
        if (!Number.isNaN(endedAtMs)) {
          const recomputedRefAge =
            recomputedLastRefReceivedAt !== null ? endedAtMs - recomputedLastRefReceivedAt : null;
          if (recomputedRefAge !== health.finalReferenceAgeMs) {
            errors.push(
              `recomputed finalReferenceAgeMs ${recomputedRefAge} does not match health ${health.finalReferenceAgeMs}`,
            );
          }
          const recomputedRefSourceAge =
            recomputedLastRefSourceTimestamp !== null
              ? endedAtMs - recomputedLastRefSourceTimestamp
              : null;
          if (recomputedRefSourceAge !== health.finalReferenceSourceAgeMs) {
            errors.push(
              `recomputed finalReferenceSourceAgeMs ${recomputedRefSourceAge} does not match health ${health.finalReferenceSourceAgeMs}`,
            );
          }
        }
      }
    }
  }

  if (errors.length > 0) {
    const message = errors.map((e) => `  - ${e}`).join("\n");
    throw new Error(`Run verification failed with ${errors.length} error(s):\n${message}`);
  }

  console.log(
    JSON.stringify({
      verified: true,
      runDirectory: directory,
      runId: manifest.runId,
      captureStatus: manifest.captureStatus,
      usableForReplay: manifest.usableForReplay,
      finalEventCount: manifest.finalEventCount,
    }),
  );
}

const isMainModule = process.argv[1]?.includes("verify-run") ?? false;

if (isMainModule) {
  const program = new Command()
    .name("market:verify-run")
    .description("Verify that a recorded run is usable for deterministic replay")
    .argument(
      "<runDirectory>",
      "path to the run directory containing manifest.json and health.json",
    )
    .option("--min-reference-observations <number>", "minimum unique Chainlink observations", "5");

  program.parse();
  const [runDirectory] = program.args;
  const opts = program.opts();
  const rawMin = Number(opts.minReferenceObservations ?? 5);
  if (!Number.isSafeInteger(rawMin) || rawMin <= 0) {
    console.error(
      JSON.stringify({
        verified: false,
        runDirectory,
        error: `--min-reference-observations must be a positive safe integer, got ${String(opts.minReferenceObservations)}`,
      }),
    );
    process.exitCode = 1;
    throw new Error("invalid argument");
  }

  try {
    if (runDirectory === undefined) throw new Error("runDirectory argument is required");
    await verifyRun(runDirectory, rawMin);
  } catch (error) {
    if (!process.exitCode) {
      console.error(
        JSON.stringify({
          verified: false,
          runDirectory,
          error: error instanceof Error ? error.message : String(error),
        }),
      );
      process.exitCode = 1;
    }
  }
}
