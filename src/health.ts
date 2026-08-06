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
  private lastReferenceReceivedAt: number | null = null;
  private lastReferenceConnectionGeneration: number | null = null;
  private lastReferenceSourceTimestamp: number | null = null;
  private fatalError = false;
  private maxSourceLatency: number | null = null;
  private maxProcessingLatency: number | null = null;
  private dataGapCount = 0;
  private closedGapCount = 0;
  private totalGapDurationMs = 0;
  private maximumGapDurationMs = 0;
  private readonly referenceClockSkewMs = 5_000;
  private readonly referenceStaleAfterMs: number;
  private referenceTimestampAnomalyCount = 0;
  private referenceTimestampMissingCount = 0;
  private referenceStaleAtReceiptCount = 0;
  private referenceFutureSourceSkewCount = 0;

  public constructor(
    private readonly runId: string,
    private readonly startedAt: string,
    private readonly expectedTokenIds: [string, string],
    private readonly books: OrderBookTracker,
    referenceStaleAfterMs = 10_000,
  ) {
    this.referenceStaleAfterMs = referenceStaleAfterMs;
  }

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
      envelope.dedupeState === "UNIQUE_APPLIED" &&
      envelope.tokenId !== null
    ) {
      this.tokenSnapshotsObserved.add(envelope.tokenId);
    }
    if (
      envelope.source === "POLYMARKET_REFERENCE_PRICE" &&
      envelope.eventType === "REFERENCE_PRICE" &&
      !envelope.isDuplicate
    ) {
      this.referenceObserved = true;
      this.lastReferenceReceivedAt = envelope.receivedAt;
      if (envelope.connectionGeneration !== null) {
        this.lastReferenceConnectionGeneration = envelope.connectionGeneration;
      }
      if (envelope.sourceTimestamp !== null) {
        this.lastReferenceSourceTimestamp = envelope.sourceTimestamp;
        const sourceAge = envelope.receivedAt - envelope.sourceTimestamp;
        if (sourceAge < -this.referenceClockSkewMs) {
          this.referenceTimestampAnomalyCount += 1;
          this.referenceFutureSourceSkewCount += 1;
        } else if (sourceAge > this.referenceStaleAfterMs) {
          this.referenceTimestampAnomalyCount += 1;
          this.referenceStaleAtReceiptCount += 1;
        }
      } else {
        this.referenceTimestampAnomalyCount += 1;
        this.referenceTimestampMissingCount += 1;
      }
    }
    if (envelope.eventType === "DATA_GAP_STARTED") {
      const feed = feedFromEnvelope(envelope);
      if (feed !== null) {
        this.openGaps.set(feed, {
          feed,
          startedAt: envelope.receivedAt,
        });
        this.dataGapCount += 1;
      }
    }
    if (envelope.eventType === "DATA_GAP_ENDED") {
      const feed = feedFromEnvelope(envelope);
      if (feed !== null) {
        const openGap = this.openGaps.get(feed);
        if (openGap !== undefined) {
          const duration = Math.max(0, envelope.receivedAt - openGap.startedAt);
          this.totalGapDurationMs += duration;
          if (duration > this.maximumGapDurationMs) this.maximumGapDurationMs = duration;
          this.closedGapCount += 1;
        }
        this.openGaps.delete(feed);
      }
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

  private evaluateReferenceQuality(
    nowMs: number,
    referenceStaleAfterMs: number,
  ): {
    fresh: boolean;
    failures: QualityFailure[];
  } {
    const failures: QualityFailure[] = [];
    if (!this.referenceObserved) {
      failures.push({
        code: "REFERENCE_STREAM_MISSING",
        message: "Chainlink reference stream not observed",
      });
      return { fresh: false, failures };
    }
    if (this.lastReferenceSourceTimestamp === null) {
      failures.push({
        code: "REFERENCE_TIMESTAMP_MISSING",
        message: "Chainlink reference observation has no canonical source timestamp",
      });
    }
    if (this.lastReferenceReceivedAt !== null) {
      const receiptAge = nowMs - this.lastReferenceReceivedAt;
      if (receiptAge < 0) {
        failures.push({
          code: "REFERENCE_LOCAL_CLOCK_SKEW",
          message: `Chainlink reference receipt time is ${-receiptAge}ms in the future relative to finalization time`,
        });
      } else if (receiptAge > referenceStaleAfterMs) {
        failures.push({
          code: "REFERENCE_STREAM_STALE",
          message: `Chainlink reference stream is stale: last observation ${receiptAge}ms ago by receipt time (threshold ${referenceStaleAfterMs}ms)`,
        });
      }
    }
    if (this.lastReferenceSourceTimestamp !== null) {
      const sourceAge = nowMs - this.lastReferenceSourceTimestamp;
      if (sourceAge > referenceStaleAfterMs) {
        failures.push({
          code: "REFERENCE_STREAM_STALE",
          message: `Chainlink reference stream is stale: last observation ${sourceAge}ms ago by source timestamp (threshold ${referenceStaleAfterMs}ms)`,
        });
      }
      if (sourceAge < -this.referenceClockSkewMs) {
        failures.push({
          code: "REFERENCE_CLOCK_SKEW",
          message: `Chainlink reference source timestamp is ${-sourceAge}ms in the future, exceeding clock skew tolerance of ${this.referenceClockSkewMs}ms`,
        });
      }
    }
    if (this.referenceTimestampMissingCount > 0) {
      failures.push({
        code: "REFERENCE_TIMESTAMP_MISSING",
        message: `${this.referenceTimestampMissingCount} reference observation(s) had missing source timestamp`,
      });
    }
    if (this.referenceStaleAtReceiptCount > 0) {
      failures.push({
        code: "REFERENCE_STREAM_STALE",
        message: `${this.referenceStaleAtReceiptCount} reference observation(s) were stale at receipt time (source age exceeded ${this.referenceStaleAfterMs}ms)`,
      });
    }
    if (this.referenceFutureSourceSkewCount > 0) {
      failures.push({
        code: "REFERENCE_CLOCK_SKEW",
        message: `${this.referenceFutureSourceSkewCount} reference observation(s) had source timestamps beyond clock skew tolerance of ${this.referenceClockSkewMs}ms in the future`,
      });
    }
    return { fresh: failures.length === 0, failures };
  }

  public computeCaptureStatus(
    bookHealth: Record<string, BookHealth>,
    currentClobGeneration: number | null,
    options: {
      processFailed?: boolean;
      nowMs: number;
      referenceStaleAfterMs: number;
    },
  ): { status: CaptureStatus; usable: boolean; failures: QualityFailure[] } {
    const failures: QualityFailure[] = [];
    const processFailed = options.processFailed ?? false;

    if (this.fatalError) {
      failures.push({ code: "FATAL_PROCESSOR_ERROR", message: "Fatal processor or storage error" });
    }

    if (processFailed) {
      failures.push({
        code: "PROCESS_FAILURE",
        message: "Run did not complete successfully (completionStatus is FAILED)",
      });
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

    const refQuality = this.evaluateReferenceQuality(options.nowMs, options.referenceStaleAfterMs);
    failures.push(...refQuality.failures);

    if (this.openGaps.size > 0) {
      for (const [feed] of this.openGaps) {
        failures.push({
          code: "OPEN_DATA_GAP",
          message: `Open data gap on feed ${feed}`,
          feed,
        });
      }
    }

    if (this.closedGapCount > 0) {
      failures.push({
        code: "CLOSED_DATA_GAP",
        message: `${this.closedGapCount} data gap(s) occurred during the run (total ${this.totalGapDurationMs}ms)`,
      });
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
      if (health.integrity === "CROSSED") {
        failures.push({
          code: "BOOK_CROSSED",
          message: `Book for token ${tokenId} is crossed: best bid ${health.bestBid} > best ask ${health.bestAsk}`,
          tokenId,
        });
      }
      if (health.crossedIncidentCount > 0 && health.integrity !== "CROSSED") {
        failures.push({
          code: "BOOK_CROSSED",
          message: `Book for token ${tokenId} had ${health.crossedIncidentCount} crossed incident(s) during the run`,
          tokenId,
        });
      }
    }

    const usable = failures.length === 0;
    let status: CaptureStatus;
    if (usable) status = "COMPLETE";
    else if (this.fatalError || processFailed || (this.total === 0 && this.malformed === 0))
      status = "UNUSABLE";
    else status = "PARTIAL";
    return { status, usable, failures };
  }

  public summary(
    endedAt: string | null,
    nowMs: number,
    clobStaleAfterMs: number,
    referenceStaleAfterMs: number,
    currentClobGeneration: number | null,
    processFailed = false,
  ): HealthSummary {
    this.books.markStale(nowMs, clobStaleAfterMs);
    const bookHealth = this.books.snapshot(nowMs, clobStaleAfterMs);
    const capture = this.computeCaptureStatus(bookHealth, currentClobGeneration, {
      processFailed,
      nowMs,
      referenceStaleAfterMs,
    });
    const openGapsRecord: Record<string, OpenGap> = {};
    for (const [feed, gap] of this.openGaps) openGapsRecord[feed] = gap;
    const finalReferenceAgeMs =
      this.lastReferenceReceivedAt !== null ? nowMs - this.lastReferenceReceivedAt : null;
    const finalReferenceSourceAgeMs =
      this.lastReferenceSourceTimestamp !== null ? nowMs - this.lastReferenceSourceTimestamp : null;
    const refQuality = this.evaluateReferenceQuality(nowMs, referenceStaleAfterMs);
    const referenceFresh = refQuality.fresh;
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
      dataGapCount: this.dataGapCount,
      closedGapCount: this.closedGapCount,
      totalGapDurationMs: this.totalGapDurationMs,
      maximumGapDurationMs: this.maximumGapDurationMs,
      captureStatus: capture.status,
      usableForReplay: capture.usable,
      qualityFailures: capture.failures,
      clobStaleAfterMs,
      referenceStaleAfterMs,
      lastReferenceReceivedAt: this.lastReferenceReceivedAt,
      lastReferenceConnectionGeneration: this.lastReferenceConnectionGeneration,
      lastReferenceSourceTimestamp: this.lastReferenceSourceTimestamp,
      finalReferenceAgeMs,
      finalReferenceSourceAgeMs,
      referenceFresh,
      referenceClockSkewMs: this.referenceClockSkewMs,
      referenceTimestampAnomalyCount: this.referenceTimestampAnomalyCount,
      referenceTimestampMissingCount: this.referenceTimestampMissingCount,
      referenceStaleAtReceiptCount: this.referenceStaleAtReceiptCount,
      referenceFutureSourceSkewCount: this.referenceFutureSourceSkewCount,
    };
  }
}
