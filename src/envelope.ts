import { normalizeJson } from "./decimal.js";
import type { DedupeState, EventSource, JsonValue, RawEventEnvelope } from "./types.js";

export type EventInput = {
  source: EventSource;
  eventType: string;
  marketId?: string | null;
  conditionId?: string | null;
  sourceMarketId?: string | null;
  tokenId?: string | null;
  observedTokenIds?: string[] | null;
  sourceTimestamp?: number | null;
  topLevelSourceTimestamp?: number | null;
  payloadSourceTimestamp?: number | null;
  receivedAt: number;
  processedAt: number;
  connectionGeneration?: number | null;
  frameId?: string | null;
  messageIndex?: number | null;
  messageCount?: number | null;
  isDuplicate?: boolean;
  dedupeState?: DedupeState;
  duplicateOfSequence?: number | null;
  dedupeReason?: string | null;
  payload: unknown;
  rawText?: string | null;
  rawMessageText?: string | null;
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
      observedTokenIds: input.observedTokenIds ?? null,
      sourceTimestamp: input.sourceTimestamp ?? null,
      topLevelSourceTimestamp: input.topLevelSourceTimestamp ?? null,
      payloadSourceTimestamp: input.payloadSourceTimestamp ?? null,
      receivedAt: input.receivedAt,
      processedAt: input.processedAt,
      connectionGeneration: input.connectionGeneration ?? null,
      frameId: input.frameId ?? null,
      messageIndex: input.messageIndex ?? null,
      messageCount: input.messageCount ?? null,
      isDuplicate: input.isDuplicate ?? false,
      dedupeState: input.dedupeState ?? "UNIQUE_APPLIED",
      duplicateOfSequence: input.duplicateOfSequence ?? null,
      dedupeReason: input.dedupeReason ?? null,
      payload: normalizeJson(input.payload),
      rawText: input.rawText ?? null,
      rawMessageText: input.rawMessageText ?? null,
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

export type MessageDedupeKey = {
  connectionGeneration: number;
  frameHash: string;
  messageIndex: number;
  messageHash: string;
  sourceTimestamp: number | null;
};

function messageDedupeKeyToString(key: MessageDedupeKey): string {
  return `${key.connectionGeneration}:${key.frameHash}:${key.messageIndex}:${key.messageHash}:${key.sourceTimestamp ?? "null"}`;
}

export class MessageDeduplicator {
  private readonly appliedSequences = new Map<string, number>();
  private readonly order: string[] = [];

  public constructor(private readonly capacity = 10_000) {
    if (!Number.isSafeInteger(capacity) || capacity <= 0) {
      throw new Error("Deduplicator capacity must be a positive integer");
    }
  }

  public classify(key: MessageDedupeKey | null): DedupeResult {
    if (key === null) return { isDuplicate: false, duplicateOfSequence: null };
    const keyString = messageDedupeKeyToString(key);
    const existing = this.appliedSequences.get(keyString);
    if (existing !== undefined) return { isDuplicate: true, duplicateOfSequence: existing };
    return { isDuplicate: false, duplicateOfSequence: null };
  }

  public registerApplied(key: MessageDedupeKey | null, sequence: number): void {
    if (key === null) return;
    const keyString = messageDedupeKeyToString(key);
    if (this.appliedSequences.has(keyString)) return;
    this.appliedSequences.set(keyString, sequence);
    this.order.push(keyString);
    if (this.order.length > this.capacity) {
      const oldest = this.order.shift();
      if (oldest !== undefined) this.appliedSequences.delete(oldest);
    }
  }
}

export function lifecyclePayload(
  feed: string,
  detail: Record<string, JsonValue> = {},
): Record<string, JsonValue> {
  return { feed, ...detail };
}
