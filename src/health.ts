import type {
  BookHealth,
  CaptureStatus,
  HealthSummary,
  OpenGap,
  QualityFailure,
  RawEventEnvelope,
} from "./types.js";
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
    if (envelope.observedTokenIds !== null) {
      for (const tokenId of envelope.observedTokenIds) this.tokenIdsObserved.add(tokenId);
    }
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

  public computeCaptureStatus(
    bookHealth: Record<string, BookHealth>,
    currentClobGeneration: number | null,
  ): { status: CaptureStatus; usable: boolean; failures: QualityFailure[] } {
    const failures: QualityFailure[] = [];

    if (this.fatalError) {
      failures.push({ code: "FATAL_PROCESSOR_ERROR", message: "Fatal processor or storage error" });
    }

    for (const tokenId of this.expectedTokenIds) {
      if (!this.tokenSnapshotsObserved.has(tokenId)) {
        failures.push({
          code:
            tokenId === this.expectedTokenIds[0] ? "MISSING_UP_SNAPSHOT" : "MISSING_DOWN_SNAPSHOT",
          message: `Missing token snapshot: ${tokenId}`,
          tokenId,
        });
      }
    }

    if (!this.referenceObserved) {
      failures.push({
        code: "REFERENCE_STREAM_MISSING",
        message: "Chainlink reference stream not observed",
      });
    }

    if (this.openGaps.size > 0) {
      for (const [feed] of this.openGaps) {
        failures.push({
          code: "OPEN_DATA_GAP",
          message: `Open data gap on feed ${feed}`,
          feed,
        });
      }
    }

    if (this.malformed > 0) {
      failures.push({
        code: "MALFORMED_EVENTS_PRESENT",
        message: `${this.malformed} malformed event(s) occurred`,
      });
    }

    for (const [tokenId, health] of Object.entries(bookHealth)) {
      if (health.state === "STALE") {
        failures.push({
          code: "BOOK_STALE",
          message: `Book for token ${tokenId} is stale`,
          tokenId,
        });
      }
      if (health.state === "INVALID") {
        failures.push({
          code: "BOOK_INVALID",
          message: `Book for token ${tokenId} is invalid`,
          tokenId,
        });
      }
      if (health.state === "RESYNC_REQUIRED") {
        failures.push({
          code: "BOOK_RESYNC_REQUIRED",
          message: `Book for token ${tokenId} requires resynchronization`,
          tokenId,
        });
      }
      if (health.state === "UNINITIALIZED") {
        failures.push({
          code:
            tokenId === this.expectedTokenIds[0] ? "MISSING_UP_SNAPSHOT" : "MISSING_DOWN_SNAPSHOT",
          message: `Book for token ${tokenId} was never initialized`,
          tokenId,
        });
      }
      if (
        health.snapshotConnectionGeneration !== null &&
        currentClobGeneration !== null &&
        health.snapshotConnectionGeneration !== currentClobGeneration
      ) {
        failures.push({
          code: "BOOK_SNAPSHOT_STALE_GENERATION",
          message: `Book for token ${tokenId} snapshot from generation ${health.snapshotConnectionGeneration}, current is ${currentClobGeneration}`,
          tokenId,
        });
      }
    }

    const usable = failures.length === 0;
    let status: CaptureStatus;
    if (usable) status = "COMPLETE";
    else if (this.fatalError || (this.total === 0 && this.malformed === 0)) status = "UNUSABLE";
    else status = "PARTIAL";
    return { status, usable, failures };
  }

  public summary(
    endedAt: string | null,
    nowMs: number,
    clobStaleAfterMs: number,
    referenceStaleAfterMs: number,
    currentClobGeneration: number | null,
  ): HealthSummary {
    this.books.markStale(nowMs, clobStaleAfterMs);
    const bookHealth = this.books.snapshot(nowMs, clobStaleAfterMs);
    const capture = this.computeCaptureStatus(bookHealth, currentClobGeneration);
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
      finalBookHealth: bookHealth,
      openGapsByFeed: openGapsRecord,
      captureStatus: capture.status,
      usableForReplay: capture.usable,
      qualityFailures: capture.failures,
      clobStaleAfterMs,
      referenceStaleAfterMs,
    };
  }
}
