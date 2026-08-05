import type { RawEventEnvelope } from "./types.js";
import type { HealthSummary } from "./types.js";
import { OrderBookTracker } from "./book.js";

function increment(counter: Record<string, number>, key: string): void {
  counter[key] = (counter[key] ?? 0) + 1;
}

export class HealthTracker {
  private readonly sourceCounts: Record<string, number> = {};
  private readonly typeCounts: Record<string, number> = {};
  private readonly tokenIdsObserved = new Set<string>();
  private total = 0;
  private malformed = 0;
  private duplicates = 0;
  private reconnects = 0;
  private staleIncidents = 0;
  private referenceObserved = false;
  private maxSourceLatency: number | null = null;
  private maxProcessingLatency: number | null = null;

  public constructor(
    private readonly runId: string,
    private readonly startedAt: string,
    private readonly expectedTokenIds: [string, string],
    private readonly books: OrderBookTracker,
  ) {}

  public observe(envelope: RawEventEnvelope, tokenIds: string[] = []): void {
    this.total += 1;
    increment(this.sourceCounts, envelope.source);
    increment(this.typeCounts, `${envelope.source}:${envelope.eventType}`);
    if (envelope.sourceTimestamp !== null) {
      const latency = Math.max(0, envelope.receivedAt - envelope.sourceTimestamp);
      this.maxSourceLatency = Math.max(this.maxSourceLatency ?? 0, latency);
    }
    const processingLatency = Math.max(0, envelope.processedAt - envelope.receivedAt);
    this.maxProcessingLatency = Math.max(this.maxProcessingLatency ?? 0, processingLatency);
    for (const tokenId of tokenIds) this.tokenIdsObserved.add(tokenId);
    if (envelope.tokenId !== null) this.tokenIdsObserved.add(envelope.tokenId);
    if (
      envelope.source === "POLYMARKET_REFERENCE_PRICE" &&
      envelope.eventType === "REFERENCE_PRICE"
    ) {
      this.referenceObserved = true;
    }
  }

  public malformedEvent(): void {
    this.malformed += 1;
  }

  public duplicateEvent(): void {
    this.duplicates += 1;
  }

  public reconnect(): void {
    this.reconnects += 1;
  }

  public staleIncident(): void {
    this.staleIncidents += 1;
  }

  public summary(endedAt: string | null, nowMs: number, staleAfterMs: number): HealthSummary {
    return {
      schemaVersion: 1,
      runId: this.runId,
      startedAt: this.startedAt,
      endedAt,
      totalEventsReceived: this.total,
      eventCountsBySource: { ...this.sourceCounts },
      eventCountsByType: { ...this.typeCounts },
      malformedEventCount: this.malformed,
      duplicateEventCount: this.duplicates,
      reconnectCount: this.reconnects,
      maximumSourceToReceiveLatencyMs: this.maxSourceLatency,
      maximumReceiveToProcessLatencyMs: this.maxProcessingLatency,
      staleFeedIncidents: this.staleIncidents,
      bothTokenStreamsObserved: this.expectedTokenIds.every((tokenId) =>
        this.tokenIdsObserved.has(tokenId),
      ),
      referencePriceStreamObserved: this.referenceObserved,
      finalBookHealth: this.books.snapshot(nowMs, staleAfterMs),
    };
  }
}
