import type { CaptureStatus, HealthSummary, OpenGap, RawEventEnvelope } from "./types.js";
import { OrderBookTracker } from "./book.js";

function increment(counter: Record<string, number>, key: string): void {
  counter[key] = (counter[key] ?? 0) + 1;
}

function feedFromEnvelope(envelope: RawEventEnvelope): string | null {
  if (
    typeof envelope.payload !== "object" ||
    envelope.payload === null ||
    Array.isArray(envelope.payload)
  )
    return null;
  const feed = (envelope.payload as Record<string, unknown>).feed;
  return typeof feed === "string" ? feed : null;
}

export class HealthTracker {
  private readonly sourceCounts: Record<string, number> = {};
  private readonly typeCounts: Record<string, number> = {};
  private readonly tokenIdsObserved = new Set<string>();
  private readonly tokenSnapshotsObserved = new Set<string>();
  private readonly openGaps = new Map<string, OpenGap>();
  private total = 0;
  private malformed = 0;
  private duplicates = 0;
  private reconnects = 0;
  private staleIncidents = 0;
  private referenceObserved = false;
  private fatalError = false;
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
      const latency = envelope.receivedAt - envelope.sourceTimestamp;
      this.maxSourceLatency = Math.max(this.maxSourceLatency ?? latency, latency);
    }
    const processingLatency = envelope.processedAt - envelope.receivedAt;
    this.maxProcessingLatency = Math.max(
      this.maxProcessingLatency ?? processingLatency,
      processingLatency,
    );
    for (const tokenId of tokenIds) this.tokenIdsObserved.add(tokenId);
    if (envelope.tokenId !== null) this.tokenIdsObserved.add(envelope.tokenId);
    if (
      envelope.source === "POLYMARKET_CLOB" &&
      envelope.eventType === "book" &&
      !envelope.isDuplicate &&
      envelope.tokenId !== null
    ) {
      this.tokenSnapshotsObserved.add(envelope.tokenId);
    }
    if (
      envelope.source === "POLYMARKET_REFERENCE_PRICE" &&
      envelope.eventType === "REFERENCE_PRICE"
    ) {
      this.referenceObserved = true;
    }
    if (envelope.eventType === "DATA_GAP_STARTED") {
      const feed = feedFromEnvelope(envelope);
      if (feed !== null) {
        this.openGaps.set(feed, {
          feed,
          startedAt: envelope.receivedAt,
        });
      }
    }
    if (envelope.eventType === "DATA_GAP_ENDED") {
      const feed = feedFromEnvelope(envelope);
      if (feed !== null) this.openGaps.delete(feed);
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

  public markFatalError(): void {
    this.fatalError = true;
  }

  public get hasFatalError(): boolean {
    return this.fatalError;
  }

  public get openGapCount(): number {
    return this.openGaps.size;
  }

  public computeCaptureStatus(): { status: CaptureStatus; usable: boolean; failures: string[] } {
    const failures: string[] = [];
    const bothTokens = this.expectedTokenIds.every((tokenId) =>
      this.tokenSnapshotsObserved.has(tokenId),
    );
    if (!bothTokens) {
      const missing = this.expectedTokenIds.filter(
        (tokenId) => !this.tokenSnapshotsObserved.has(tokenId),
      );
      failures.push(`Missing token snapshots: ${missing.join(", ")}`);
    }
    if (!this.referenceObserved) failures.push("Chainlink reference stream not observed");
    if (this.fatalError) failures.push("Fatal processor or storage error occurred");
    if (this.openGaps.size > 0) {
      failures.push(`Open data gaps remain: ${[...this.openGaps.keys()].join(", ")}`);
    }
    const usable = failures.length === 0;
    let status: CaptureStatus;
    if (usable) status = "COMPLETE";
    else if (this.fatalError || (this.total === 0 && this.malformed === 0)) status = "UNUSABLE";
    else status = "PARTIAL";
    return { status, usable, failures };
  }

  public summary(endedAt: string | null, nowMs: number, staleAfterMs: number): HealthSummary {
    this.books.markStale(nowMs, staleAfterMs);
    const capture = this.computeCaptureStatus();
    const openGapsRecord: Record<string, OpenGap> = {};
    for (const [feed, gap] of this.openGaps) openGapsRecord[feed] = gap;
    return {
      schemaVersion: 2,
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
      openGapsByFeed: openGapsRecord,
      captureStatus: capture.status,
      usableForReplay: capture.usable,
      qualityFailures: capture.failures,
    };
  }
}
