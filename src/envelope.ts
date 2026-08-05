import { normalizeJson } from "./decimal.js";
import type { EventSource, JsonValue, RawEventEnvelope } from "./types.js";

export type EventInput = {
  source: EventSource;
  eventType: string;
  marketId?: string | null;
  conditionId?: string | null;
  sourceMarketId?: string | null;
  tokenId?: string | null;
  sourceTimestamp?: number | null;
  receivedAt: number;
  processedAt: number;
  connectionGeneration?: number | null;
  frameId?: string | null;
  messageIndex?: number | null;
  messageCount?: number | null;
  isDuplicate?: boolean;
  duplicateOfSequence?: number | null;
  dedupeReason?: string | null;
  payload: unknown;
  rawText?: string | null;
};

export class EventSequencer {
  private sequence = 0;

  public constructor(private readonly runId: string) {}

  public next(input: EventInput): RawEventEnvelope {
    this.sequence += 1;
    return {
      schemaVersion: 2,
      runId: this.runId,
      sequence: this.sequence,
      source: input.source,
      eventType: input.eventType,
      marketId: input.marketId ?? null,
      conditionId: input.conditionId ?? null,
      sourceMarketId: input.sourceMarketId ?? null,
      tokenId: input.tokenId ?? null,
      sourceTimestamp: input.sourceTimestamp ?? null,
      receivedAt: input.receivedAt,
      processedAt: input.processedAt,
      connectionGeneration: input.connectionGeneration ?? null,
      frameId: input.frameId ?? null,
      messageIndex: input.messageIndex ?? null,
      messageCount: input.messageCount ?? null,
      isDuplicate: input.isDuplicate ?? false,
      duplicateOfSequence: input.duplicateOfSequence ?? null,
      dedupeReason: input.dedupeReason ?? null,
      payload: normalizeJson(input.payload),
      rawText: input.rawText ?? null,
    };
  }

  public get count(): number {
    return this.sequence;
  }
}

export type DedupeResult = {
  isDuplicate: boolean;
  duplicateOfSequence: number | null;
};

export class FrameDeduplicator {
  private readonly sequences = new Map<string, number>();
  private readonly order: string[] = [];

  public constructor(private readonly capacity = 10_000) {
    if (!Number.isSafeInteger(capacity) || capacity <= 0) {
      throw new Error("Deduplicator capacity must be a positive integer");
    }
  }

  public classify(key: string | null, currentSequence: number): DedupeResult {
    if (key === null) return { isDuplicate: false, duplicateOfSequence: null };
    const existing = this.sequences.get(key);
    if (existing !== undefined) return { isDuplicate: true, duplicateOfSequence: existing };
    this.sequences.set(key, currentSequence);
    this.order.push(key);
    if (this.order.length > this.capacity) {
      const oldest = this.order.shift();
      if (oldest !== undefined) this.sequences.delete(oldest);
    }
    return { isDuplicate: false, duplicateOfSequence: null };
  }
}

export function lifecyclePayload(
  feed: string,
  detail: Record<string, JsonValue> = {},
): Record<string, JsonValue> {
  return { feed, ...detail };
}
