import { createHash } from "node:crypto";

import { FrameDeduplicator, EventSequencer } from "./envelope.js";
import { assertExactWireDecimals, extractJsonDecimalLexeme } from "./decimal.js";
import { errorMessage } from "./logger.js";
import type { FeedFrame, FeedLifecycle } from "./feeds/managed-websocket.js";
import { HealthTracker } from "./health.js";
import { OrderBookTracker } from "./book.js";
import type { Logger, MarketSpecification } from "./types.js";
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

function conservativeDedupeKey(
  connectionGeneration: number,
  sourceTimestamp: number | null,
  rawText: string,
): string {
  return `${connectionGeneration}:${sourceTimestamp ?? "null"}:${rawFrameHash(rawText)}`;
}

export class RecorderEventProcessor {
  private readonly sequencer: EventSequencer;
  private readonly deduplicator = new FrameDeduplicator();
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
      });
      await this.store.append(envelope);
      this.health.observe(envelope);
      return;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(frame.rawText) as unknown;
    } catch (error) {
      await this.writeMalformed(frame, frameId, 0, 1, error);
      return;
    }

    const messages: unknown[] = Array.isArray(parsed) ? parsed : [parsed];
    const messageCount = messages.length;
    for (let index = 0; index < messages.length; index += 1) {
      const message: unknown = messages[index];
      try {
        if (frame.source === "POLYMARKET_CLOB") {
          await this.processClob(frame, frameId, index, messageCount, message);
        } else if (frame.source === "POLYMARKET_REFERENCE_PRICE") {
          await this.processReference(frame, frameId, index, messageCount, message);
        }
      } catch (error) {
        await this.writeMalformed(frame, frameId, index, messageCount, error);
      }
    }
  }

  private async processClob(
    frame: FeedFrame,
    frameId: string,
    messageIndex: number,
    messageCount: number,
    message: unknown,
  ): Promise<void> {
    const event = asRecord(message);
    if (event === null || typeof event.event_type !== "string") {
      throw new Error("CLOB frame is missing event_type");
    }
    assertExactWireDecimals(event);
    const eventType = event.event_type;
    const sourceMarketId = typeof event.market === "string" ? event.market : null;
    if (sourceMarketId !== null && sourceMarketId !== this.market.conditionId) {
      throw new Error(
        `CLOB event.market "${sourceMarketId}" does not match conditionId "${this.market.conditionId}"`,
      );
    }
    const sourceTimestamp = timestampFrom(event.timestamp);
    const dedupeKey = conservativeDedupeKey(
      frame.connectionGeneration,
      sourceTimestamp,
      frame.rawText,
    );
    const processedAt = this.now();
    const preSequence = this.sequencer.count + 1;
    const dedupeResult = this.deduplicator.classify(dedupeKey, preSequence);
    const isDuplicate = dedupeResult.isDuplicate;
    let observedTokens: string[] = [];
    if (!isDuplicate) {
      observedTokens = this.books.apply(event, processedAt);
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
      sourceTimestamp,
      receivedAt: frame.receivedAt,
      processedAt,
      connectionGeneration: frame.connectionGeneration,
      frameId,
      messageIndex,
      messageCount,
      isDuplicate,
      duplicateOfSequence: dedupeResult.duplicateOfSequence,
      dedupeReason: isDuplicate
        ? "matching connection generation, source timestamp, and raw-frame hash"
        : null,
      payload: event,
      rawText: frame.rawText,
    });
    await this.store.append(envelope);
    this.health.observe(envelope, tokens);
    if (isDuplicate) this.health.duplicateEvent();
    else this.onValidData?.(frame);
  }

  private async processReference(
    frame: FeedFrame,
    frameId: string,
    messageIndex: number,
    messageCount: number,
    message: unknown,
  ): Promise<void> {
    const parsedEvent = asRecord(message);
    if (
      parsedEvent === null ||
      typeof parsedEvent.topic !== "string" ||
      typeof parsedEvent.type !== "string"
    ) {
      throw new Error("Reference-price frame has an unexpected shape");
    }
    const referenceEventType = parsedEvent.type;
    const parsedPayload = asRecord(parsedEvent.payload);
    const exactValue = extractJsonDecimalLexeme(frame.rawText, "value");
    const event =
      parsedPayload === null || exactValue === null
        ? parsedEvent
        : { ...parsedEvent, payload: { ...parsedPayload, value: exactValue } };
    const sourceTimestamp = timestampFrom(parsedEvent.timestamp);
    const dedupeKey = conservativeDedupeKey(
      frame.connectionGeneration,
      sourceTimestamp,
      frame.rawText,
    );
    const preSequence = this.sequencer.count + 1;
    const dedupeResult = this.deduplicator.classify(dedupeKey, preSequence);
    const isDuplicate = dedupeResult.isDuplicate;
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
      receivedAt: frame.receivedAt,
      processedAt: this.now(),
      connectionGeneration: frame.connectionGeneration,
      frameId,
      messageIndex,
      messageCount,
      isDuplicate,
      duplicateOfSequence: dedupeResult.duplicateOfSequence,
      dedupeReason: isDuplicate
        ? "matching connection generation, source timestamp, and raw-frame hash"
        : null,
      payload: event,
      rawText: frame.rawText,
    });
    await this.store.append(envelope);
    this.health.observe(envelope);
    if (isDuplicate) this.health.duplicateEvent();
    else this.onValidData?.(frame);
  }

  private async writeMalformed(
    frame: FeedFrame,
    frameId: string,
    messageIndex: number,
    messageCount: number,
    error: unknown,
  ): Promise<void> {
    this.health.malformedEvent();
    const envelope = this.sequencer.next({
      source: frame.source,
      eventType: "MALFORMED_EVENT",
      marketId: this.market.marketId,
      conditionId: this.market.conditionId,
      receivedAt: frame.receivedAt,
      processedAt: this.now(),
      connectionGeneration: frame.connectionGeneration,
      frameId,
      messageIndex,
      messageCount,
      payload: { error: errorMessage(error) },
      rawText: frame.rawText,
    });
    await this.store.append(envelope);
    this.health.observe(envelope);
  }
}
