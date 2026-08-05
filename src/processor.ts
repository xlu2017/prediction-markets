import { createHash } from "node:crypto";

import { MessageDeduplicator, EventSequencer, type MessageDedupeKey } from "./envelope.js";
import { assertExactWireDecimals, extractJsonDecimalLexeme } from "./decimal.js";
import { errorMessage } from "./logger.js";
import type { FeedFrame, FeedLifecycle } from "./feeds/managed-websocket.js";
import { HealthTracker } from "./health.js";
import { OrderBookTracker } from "./book.js";
import type { DedupeState, JsonValue, Logger, MarketSpecification } from "./types.js";
import { RunStore } from "./store.js";

function asRecord(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function timestampFrom(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && /^\d+$/u.test(value)) {
    const parsed = Number(value);
    return Number.isSafeInteger(parsed) ? parsed : null;
  }
  return null;
}

function clobTokenIds(event: Record<string, unknown>): string[] {
  const result = new Set<string>();
  if (typeof event.asset_id === "string") result.add(event.asset_id);
  if (Array.isArray(event.price_changes)) {
    for (const item of event.price_changes) {
      const record = asRecord(item);
      if (record !== null && typeof record.asset_id === "string") result.add(record.asset_id);
    }
  }
  return [...result];
}

function rawFrameHash(rawText: string): string {
  return createHash("sha256").update(rawText).digest("hex");
}

function messageHash(messageText: string): string {
  return createHash("sha256").update(messageText).digest("hex");
}

function sliceArrayMessages(rawText: string): string[] | null {
  const trimmed = rawText.trim();
  if (!trimmed.startsWith("[")) return null;
  try {
    const parsed = JSON.parse(rawText) as unknown;
    if (!Array.isArray(parsed)) return null;
  } catch {
    return null;
  }
  const messages: string[] = [];
  let depth = 0;
  let inString = false;
  let escape = false;
  let start = -1;
  for (let i = 0; i < rawText.length; i += 1) {
    const ch = rawText[i];
    if (inString) {
      if (escape) escape = false;
      else if (ch === "\\") escape = true;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '"') inString = true;
    else if (ch === "[" || ch === "{") {
      if (depth === 1 && start < 0) start = i;
      depth += 1;
    } else if (ch === "]" || ch === "}") {
      depth -= 1;
      if (depth === 1 && start >= 0) {
        messages.push(rawText.slice(start, i + 1));
        start = -1;
      }
    }
  }
  return messages.length > 0 ? messages : null;
}

type ParsedMessage = {
  message: unknown;
  rawMessageText: string;
  messageIndex: number;
};

function parseFrameMessages(rawText: string): ParsedMessage[] {
  const slices = sliceArrayMessages(rawText);
  if (slices !== null) {
    const result: ParsedMessage[] = [];
    for (let i = 0; i < slices.length; i += 1) {
      try {
        const message = JSON.parse(slices[i] ?? "") as unknown;
        result.push({ message, rawMessageText: slices[i] ?? "", messageIndex: i });
      } catch {
        result.push({ message: null, rawMessageText: slices[i] ?? "", messageIndex: i });
      }
    }
    return result;
  }
  return [{ message: JSON.parse(rawText) as unknown, rawMessageText: rawText, messageIndex: 0 }];
}

type MalformedIdentity = {
  sourceMarketId: string | null;
  tokenIds: string[];
  sourceTimestamp: number | null;
  eventType: string | null;
  topic: string | null;
  symbol: string | null;
};

function extractMalformedIdentity(message: unknown): MalformedIdentity {
  const record = asRecord(message);
  if (record === null) {
    return {
      sourceMarketId: null,
      tokenIds: [],
      sourceTimestamp: null,
      eventType: null,
      topic: null,
      symbol: null,
    };
  }
  const sourceMarketId = typeof record.market === "string" ? record.market : null;
  const eventType = typeof record.event_type === "string" ? record.event_type : null;
  const sourceTimestamp = timestampFrom(record.timestamp);
  const tokenIds = clobTokenIds(record);
  const payload = asRecord(record.payload);
  const topic = typeof record.topic === "string" ? record.topic : null;
  const symbol = typeof payload?.symbol === "string" ? payload.symbol : null;
  const payloadTimestamp = payload !== null ? timestampFrom(payload.timestamp) : null;
  return {
    sourceMarketId,
    tokenIds,
    sourceTimestamp: sourceTimestamp ?? payloadTimestamp,
    eventType,
    topic,
    symbol,
  };
}

export class RecorderEventProcessor {
  private readonly sequencer: EventSequencer;
  private readonly deduplicator = new MessageDeduplicator();
  private frameCounter = 0;
  private queue: Promise<void> = Promise.resolve();
  private fatalError: Error | null = null;

  public constructor(
    runId: string,
    private readonly market: MarketSpecification,
    private readonly store: RunStore,
    private readonly books: OrderBookTracker,
    private readonly health: HealthTracker,
    private readonly logger: Logger,
    private readonly now: () => number = Date.now,
    private readonly onValidData?: (frame: FeedFrame) => void,
    private readonly onFatalError?: () => void,
    private readonly onClobGap?: () => void,
  ) {
    this.sequencer = new EventSequencer(runId);
  }

  public enqueueFrame(frame: FeedFrame): void {
    this.enqueue(() => this.processFrame(frame));
  }

  public enqueueLifecycle(event: FeedLifecycle): void {
    this.enqueue(async () => {
      if (event.eventType === "RECONNECT_SCHEDULED") this.health.reconnect();
      if (event.eventType === "STALE_STREAM") this.health.staleIncident();
      if (
        event.eventType === "DATA_GAP_STARTED" &&
        (event.payload as Record<string, unknown>).feed === "clob-market"
      ) {
        this.books.requireResync();
        this.onClobGap?.();
      }
      if (
        event.eventType === "STALE_STREAM" &&
        (event.payload as Record<string, unknown>).feed === "clob-market"
      ) {
        this.books.requireResync();
        this.onClobGap?.();
      }
      const envelope = this.sequencer.next({
        source: event.source,
        eventType: event.eventType,
        marketId: this.market.marketId,
        conditionId: this.market.conditionId,
        receivedAt: event.receivedAt,
        processedAt: this.now(),
        connectionGeneration: event.connectionGeneration,
        payload: event.payload,
      });
      await this.store.append(envelope);
      this.health.observe(envelope);
    });
  }

  public enqueueMarketMetadata(receivedAt: number): void {
    this.enqueue(async () => {
      const envelope = this.sequencer.next({
        source: "POLYMARKET_MARKET",
        eventType: "MARKET_DISCOVERED",
        marketId: this.market.marketId,
        conditionId: this.market.conditionId,
        receivedAt,
        processedAt: this.now(),
        payload: this.market.rawMetadata,
        rawText: this.market.rawGammaText,
      });
      await this.store.append(envelope);
      this.health.observe(envelope);
    });
  }

  public async drain(): Promise<number> {
    await this.queue;
    if (this.fatalError !== null) throw this.fatalError;
    return this.sequencer.count;
  }

  public get count(): number {
    return this.sequencer.count;
  }

  private enqueue(task: () => Promise<void>): void {
    this.queue = this.queue.then(task).catch((error: unknown) => {
      const normalized = error instanceof Error ? error : new Error(String(error));
      this.fatalError ??= normalized;
      this.health.markFatalError();
      this.logger.error("Event processing failed", { message: normalized.message });
      this.onFatalError?.();
    });
  }

  private nextFrameId(): string {
    this.frameCounter += 1;
    return `frame-${this.frameCounter}`;
  }

  private async processFrame(frame: FeedFrame): Promise<void> {
    const frameId = this.nextFrameId();
    const frameHash = rawFrameHash(frame.rawText);
    if (
      frame.source === "POLYMARKET_CLOB" &&
      frame.rawText !== "PONG" &&
      frame.rawText !== "PING"
    ) {
      this.logger.debug("CLOB frame received", { frameId, rawText: frame.rawText.slice(0, 100) });
    }

    if (frame.rawText === "PONG" || frame.rawText === "PING") {
      const envelope = this.sequencer.next({
        source: frame.source,
        eventType: frame.rawText,
        marketId: this.market.marketId,
        conditionId: this.market.conditionId,
        receivedAt: frame.receivedAt,
        processedAt: this.now(),
        connectionGeneration: frame.connectionGeneration,
        frameId,
        messageIndex: 0,
        messageCount: 1,
        payload: { value: frame.rawText },
        rawText: frame.rawText,
        rawMessageText: frame.rawText,
      });
      await this.store.append(envelope);
      this.health.observe(envelope);
      return;
    }

    let parsedMessages: ParsedMessage[];
    try {
      parsedMessages = parseFrameMessages(frame.rawText);
    } catch (error) {
      await this.writeMalformed(frame, frameId, frameHash, 0, 1, null, error, frame.rawText);
      return;
    }

    const messageCount = parsedMessages.length;
    for (const parsed of parsedMessages) {
      try {
        if (parsed.message === null) {
          throw new Error("Message could not be parsed");
        }
        if (frame.source === "POLYMARKET_CLOB") {
          await this.processClob(frame, frameId, frameHash, parsed, messageCount);
        } else if (frame.source === "POLYMARKET_REFERENCE_PRICE") {
          await this.processReference(frame, frameId, frameHash, parsed, messageCount);
        }
      } catch (error) {
        const identity = extractMalformedIdentity(parsed.message);
        await this.writeMalformed(
          frame,
          frameId,
          frameHash,
          parsed.messageIndex,
          messageCount,
          identity,
          error,
          parsed.rawMessageText,
        );
      }
    }
  }

  private buildDedupeKey(
    frame: FeedFrame,
    frameHash: string,
    parsed: ParsedMessage,
    sourceTimestamp: number | null,
  ): MessageDedupeKey {
    return {
      connectionGeneration: frame.connectionGeneration,
      frameHash,
      messageIndex: parsed.messageIndex,
      messageHash: messageHash(parsed.rawMessageText),
      sourceTimestamp,
    };
  }

  private async processClob(
    frame: FeedFrame,
    frameId: string,
    frameHash: string,
    parsed: ParsedMessage,
    messageCount: number,
  ): Promise<void> {
    const event = asRecord(parsed.message);
    if (event === null || typeof event.event_type !== "string") {
      throw new Error("CLOB frame is missing event_type");
    }
    assertExactWireDecimals(event);
    const eventType = event.event_type;
    const sourceMarketId = event.market;
    if (typeof sourceMarketId !== "string") {
      throw new Error("CLOB event is missing required market field");
    }
    if (sourceMarketId !== this.market.conditionId) {
      throw new Error(
        `CLOB event.market "${sourceMarketId}" does not match conditionId "${this.market.conditionId}"`,
      );
    }
    const sourceTimestamp = timestampFrom(event.timestamp);
    const dedupeKey = this.buildDedupeKey(frame, frameHash, parsed, sourceTimestamp);
    const dedupeResult = this.deduplicator.classify(dedupeKey);
    const isDuplicate = dedupeResult.isDuplicate;
    const processedAt = this.now();
    let observedTokens: string[] = [];
    let dedupeState: DedupeState;

    if (isDuplicate) {
      dedupeState = "DUPLICATE_APPLIED";
    } else {
      observedTokens = this.books.apply(event, processedAt, frame.connectionGeneration);
      this.deduplicator.registerApplied(dedupeKey, this.sequencer.count + 1);
      dedupeState = "UNIQUE_APPLIED";
    }

    const frameTokens = clobTokenIds(event);
    const tokens = [...new Set([...observedTokens, ...frameTokens])];
    const envelope = this.sequencer.next({
      source: frame.source,
      eventType,
      marketId: this.market.marketId,
      conditionId: this.market.conditionId,
      sourceMarketId,
      tokenId: tokens.length === 1 ? (tokens[0] ?? null) : null,
      observedTokenIds: tokens.length > 1 ? tokens : null,
      sourceTimestamp,
      receivedAt: frame.receivedAt,
      processedAt,
      connectionGeneration: frame.connectionGeneration,
      frameId,
      messageIndex: parsed.messageIndex,
      messageCount,
      isDuplicate,
      dedupeState,
      duplicateOfSequence: dedupeResult.duplicateOfSequence,
      dedupeReason: isDuplicate
        ? "matching connection generation, frame hash, message index, message hash, and source timestamp"
        : null,
      payload: event,
      rawText: frame.rawText,
      rawMessageText: parsed.rawMessageText,
    });
    await this.store.append(envelope);
    this.health.observe(envelope, tokens);
    if (isDuplicate) this.health.duplicateEvent();
    else this.onValidData?.(frame);
  }

  private async processReference(
    frame: FeedFrame,
    frameId: string,
    frameHash: string,
    parsed: ParsedMessage,
    messageCount: number,
  ): Promise<void> {
    const parsedEvent = asRecord(parsed.message);
    if (
      parsedEvent === null ||
      typeof parsedEvent.topic !== "string" ||
      typeof parsedEvent.type !== "string"
    ) {
      throw new Error("Reference-price frame has an unexpected shape");
    }
    const referenceEventType = parsedEvent.type;
    const parsedPayload = asRecord(parsedEvent.payload);
    const exactValue = extractJsonDecimalLexeme(parsed.rawMessageText, "value");
    const event =
      parsedPayload === null || exactValue === null
        ? parsedEvent
        : { ...parsedEvent, payload: { ...parsedPayload, value: exactValue } };
    const topLevelTimestamp = timestampFrom(parsedEvent.timestamp);
    const payloadTimestamp = parsedPayload !== null ? timestampFrom(parsedPayload.timestamp) : null;
    const sourceTimestamp = payloadTimestamp ?? topLevelTimestamp;
    const dedupeKey = this.buildDedupeKey(frame, frameHash, parsed, sourceTimestamp);
    const dedupeResult = this.deduplicator.classify(dedupeKey);
    const isDuplicate = dedupeResult.isDuplicate;
    let dedupeState: DedupeState;

    if (isDuplicate) {
      dedupeState = "DUPLICATE_APPLIED";
    } else {
      this.deduplicator.registerApplied(dedupeKey, this.sequencer.count + 1);
      dedupeState = "UNIQUE_APPLIED";
    }

    const payload = asRecord(event.payload);
    const isBtcPrice =
      event.topic === "crypto_prices_chainlink" &&
      payload?.symbol === "btc/usd" &&
      (typeof payload.value === "number" || typeof payload.value === "string");
    const envelope = this.sequencer.next({
      source: frame.source,
      eventType: isBtcPrice ? "REFERENCE_PRICE" : referenceEventType.toUpperCase(),
      marketId: this.market.marketId,
      conditionId: this.market.conditionId,
      tokenId: null,
      sourceTimestamp,
      topLevelSourceTimestamp: topLevelTimestamp,
      payloadSourceTimestamp: payloadTimestamp,
      receivedAt: frame.receivedAt,
      processedAt: this.now(),
      connectionGeneration: frame.connectionGeneration,
      frameId,
      messageIndex: parsed.messageIndex,
      messageCount,
      isDuplicate,
      dedupeState,
      duplicateOfSequence: dedupeResult.duplicateOfSequence,
      dedupeReason: isDuplicate
        ? "matching connection generation, frame hash, message index, message hash, and source timestamp"
        : null,
      payload: event,
      rawText: frame.rawText,
      rawMessageText: parsed.rawMessageText,
    });
    await this.store.append(envelope);
    this.health.observe(envelope);
    if (isDuplicate) this.health.duplicateEvent();
    else if (isBtcPrice) this.onValidData?.(frame);
  }

  private async writeMalformed(
    frame: FeedFrame,
    frameId: string,
    _frameHash: string,
    messageIndex: number,
    messageCount: number,
    identity: MalformedIdentity | null,
    error: unknown,
    rawMessageText: string,
  ): Promise<void> {
    this.health.malformedEvent();
    const parsedIdentity: JsonValue | null =
      identity === null
        ? null
        : {
            sourceMarketId: identity.sourceMarketId,
            tokenIds: identity.tokenIds,
            sourceTimestamp: identity.sourceTimestamp,
            eventType: identity.eventType,
            topic: identity.topic,
            symbol: identity.symbol,
          };
    const envelope = this.sequencer.next({
      source: frame.source,
      eventType: "MALFORMED_EVENT",
      marketId: this.market.marketId,
      conditionId: this.market.conditionId,
      sourceMarketId: identity?.sourceMarketId ?? null,
      tokenId: identity?.tokenIds.length === 1 ? (identity.tokenIds[0] ?? null) : null,
      observedTokenIds:
        identity !== null && identity.tokenIds.length > 1 ? identity.tokenIds : null,
      sourceTimestamp: identity?.sourceTimestamp ?? null,
      receivedAt: frame.receivedAt,
      processedAt: this.now(),
      connectionGeneration: frame.connectionGeneration,
      frameId,
      messageIndex,
      messageCount,
      dedupeState: "RECEIVED_NOT_APPLIED",
      payload: {
        errorCode: "MALFORMED_EVENT",
        errorMessage: errorMessage(error),
        parsedIdentity,
      },
      rawText: frame.rawText,
      rawMessageText,
    });
    await this.store.append(envelope);
    this.health.observe(envelope);
  }
}
