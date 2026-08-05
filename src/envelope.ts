import { normalizeJson } from "./decimal.js";
import type { EventSource, JsonValue, RawEventEnvelope } from "./types.js";

export type EventInput = {
  source: EventSource;
  eventType: string;
  marketId?: string | null;
  tokenId?: string | null;
  sourceTimestamp?: number | null;
  receivedAt: number;
  processedAt: number;
  connectionGeneration?: number | null;
  payload: unknown;
  rawText?: string | null;
};

export class EventSequencer {
  private sequence = 0;

  public constructor(private readonly runId: string) {}

  public next(input: EventInput): RawEventEnvelope {
    this.sequence += 1;
    return {
      schemaVersion: 1,
      runId: this.runId,
      sequence: this.sequence,
      source: input.source,
      eventType: input.eventType,
      marketId: input.marketId ?? null,
      tokenId: input.tokenId ?? null,
      sourceTimestamp: input.sourceTimestamp ?? null,
      receivedAt: input.receivedAt,
      processedAt: input.processedAt,
      connectionGeneration: input.connectionGeneration ?? null,
      payload: normalizeJson(input.payload),
      rawText: input.rawText ?? null,
    };
  }

  public get count(): number {
    return this.sequence;
  }
}

export class BoundedDeduplicator {
  private readonly values = new Set<string>();
  private readonly order: string[] = [];

  public constructor(private readonly capacity = 10_000) {
    if (!Number.isSafeInteger(capacity) || capacity <= 0) {
      throw new Error("Deduplicator capacity must be a positive integer");
    }
  }

  public seen(key: string | null): boolean {
    if (key === null) return false;
    if (this.values.has(key)) return true;
    this.values.add(key);
    this.order.push(key);
    if (this.order.length > this.capacity) {
      const oldest = this.order.shift();
      if (oldest !== undefined) this.values.delete(oldest);
    }
    return false;
  }
}

export function lifecyclePayload(
  feed: string,
  detail: Record<string, JsonValue> = {},
): Record<string, JsonValue> {
  return { feed, ...detail };
}
