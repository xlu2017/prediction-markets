import { BoundedDeduplicator, EventSequencer } from "./envelope.js";
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

function clobDedupeKey(event: Record<string, unknown>, eventType: string): string | null {
  const hashes = new Set<string>();
  if (typeof event.hash === "string") hashes.add(event.hash);
  if (Array.isArray(event.price_changes)) {
    for (const item of event.price_changes) {
      const record = asRecord(item);
      if (record !== null && typeof record.hash === "string") hashes.add(record.hash);
    }
  }
  if (hashes.size === 0) return null;
  return `${eventType}:${[...hashes].sort().join(",")}`;
}

function referenceDedupeKey(event: Record<string, unknown>): string | null {
  const payload = asRecord(event.payload);
  if (payload?.symbol !== "btc/usd") return null;
  const timestamp = timestampFrom(payload.timestamp);
  if (
    timestamp === null ||
    (typeof payload.value !== "number" && typeof payload.value !== "string")
  ) {
    return null;
  }
  return `btc/usd:${timestamp}:${String(payload.value)}`;
}

export class RecorderEventProcessor {
  private readonly sequencer: EventSequencer;
  private readonly deduplicator = new BoundedDeduplicator();
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
        receivedAt,
        processedAt: this.now(),
        payload: this.market.rawMetadata,
        rawText: JSON.stringify(this.market.rawMetadata),
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
      this.logger.error("Event processing failed", { message: normalized.message });
    });
  }

  private async processFrame(frame: FeedFrame): Promise<void> {
    if (frame.rawText === "PONG" || frame.rawText === "PING") {
      const envelope = this.sequencer.next({
        source: frame.source,
        eventType: frame.rawText,
        marketId: this.market.marketId,
        receivedAt: frame.receivedAt,
        processedAt: this.now(),
        connectionGeneration: frame.connectionGeneration,
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
      await this.writeMalformed(frame, error);
      return;
    }

    const messages = Array.isArray(parsed) ? parsed : [parsed];
    for (const message of messages) {
      try {
        if (frame.source === "POLYMARKET_CLOB") await this.processClob(frame, message);
        else if (frame.source === "POLYMARKET_REFERENCE_PRICE") {
          await this.processReference(frame, message);
        }
      } catch (error) {
        await this.writeMalformed(frame, error);
      }
    }
  }

  private async processClob(frame: FeedFrame, message: unknown): Promise<void> {
    const event = asRecord(message);
    if (event === null || typeof event.event_type !== "string") {
      throw new Error("CLOB frame is missing event_type");
    }
    assertExactWireDecimals(event);
    const eventType = event.event_type;
    if (this.deduplicator.seen(clobDedupeKey(event, eventType))) {
      this.health.duplicateEvent();
      return;
    }
    const processedAt = this.now();
    const observedTokens = this.books.apply(event, processedAt);
    const frameTokens = clobTokenIds(event);
    const tokens = [...new Set([...observedTokens, ...frameTokens])];
    const envelope = this.sequencer.next({
      source: frame.source,
      eventType,
      marketId: typeof event.market === "string" ? event.market : this.market.marketId,
      tokenId: tokens.length === 1 ? (tokens[0] ?? null) : null,
      sourceTimestamp: timestampFrom(event.timestamp),
      receivedAt: frame.receivedAt,
      processedAt,
      connectionGeneration: frame.connectionGeneration,
      payload: event,
      rawText: frame.rawText,
    });
    await this.store.append(envelope);
    this.health.observe(envelope, tokens);
  }

  private async processReference(frame: FeedFrame, message: unknown): Promise<void> {
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
    if (this.deduplicator.seen(referenceDedupeKey(event))) {
      this.health.duplicateEvent();
      return;
    }
    const payload = asRecord(event.payload);
    const isBtcPrice =
      event.topic === "crypto_prices_chainlink" &&
      payload?.symbol === "btc/usd" &&
      (typeof payload.value === "number" || typeof payload.value === "string");
    const sourceTimestamp = timestampFrom(payload?.timestamp) ?? timestampFrom(event.timestamp);
    const envelope = this.sequencer.next({
      source: frame.source,
      eventType: isBtcPrice ? "REFERENCE_PRICE" : `REFERENCE_${referenceEventType.toUpperCase()}`,
      marketId: this.market.marketId,
      sourceTimestamp,
      receivedAt: frame.receivedAt,
      processedAt: this.now(),
      connectionGeneration: frame.connectionGeneration,
      payload: event,
      rawText: frame.rawText,
    });
    await this.store.append(envelope);
    this.health.observe(envelope);
  }

  private async writeMalformed(frame: FeedFrame, error: unknown): Promise<void> {
    this.health.malformedEvent();
    const envelope = this.sequencer.next({
      source: frame.source,
      eventType: "MALFORMED_EVENT",
      marketId: this.market.marketId,
      receivedAt: frame.receivedAt,
      processedAt: this.now(),
      connectionGeneration: frame.connectionGeneration,
      payload: { error: errorMessage(error) },
      rawText: frame.rawText,
    });
    await this.store.append(envelope);
    this.health.observe(envelope);
  }
}
