import { describe, expect, it } from "vitest";

import { replayClobState } from "../src/verify-run.js";
import type { RawEventEnvelope } from "../src/types.js";

const conditionId = "0xcondition";
const tokenIds = ["111111", "222222"];

function baseBookEvent(sequence: number, tokenId: string, generation: number): RawEventEnvelope {
  return {
    schemaVersion: 2,
    runId: "fixture-run-001",
    sequence,
    source: "POLYMARKET_CLOB",
    eventType: "book",
    marketId: "market-200",
    conditionId,
    sourceMarketId: conditionId,
    tokenId,
    observedTokenIds: null,
    sourceTimestamp: 1785909720100 + sequence,
    topLevelSourceTimestamp: 1785909720100 + sequence,
    payloadSourceTimestamp: null,
    receivedAt: 1785909720105 + sequence,
    processedAt: 1785909720106 + sequence,
    connectionGeneration: generation,
    frameId: `frame-${sequence}`,
    messageIndex: 0,
    messageCount: 1,
    isDuplicate: false,
    dedupeState: "UNIQUE_APPLIED",
    duplicateOfSequence: null,
    dedupeReason: null,
    payload: {
      event_type: "book",
      asset_id: tokenId,
      market: conditionId,
      bids: [{ price: ".48", size: "30" }],
      asks: [{ price: ".52", size: "25" }],
      timestamp: String(1785909720100 + sequence),
    },
    rawText: "",
    rawMessageText: null,
  };
}

function lifecycleEvent(
  sequence: number,
  eventType: "STALE_STREAM" | "DATA_GAP_STARTED" | "DATA_GAP_ENDED",
  generation: number,
): RawEventEnvelope {
  return {
    schemaVersion: 2,
    runId: "fixture-run-001",
    sequence,
    source: "POLYMARKET_CLOB",
    eventType,
    marketId: "market-200",
    conditionId,
    sourceMarketId: null,
    tokenId: null,
    observedTokenIds: null,
    sourceTimestamp: null,
    topLevelSourceTimestamp: null,
    payloadSourceTimestamp: null,
    receivedAt: 1785909720150 + sequence,
    processedAt: 1785909720151 + sequence,
    connectionGeneration: generation,
    frameId: null,
    messageIndex: null,
    messageCount: null,
    isDuplicate: false,
    dedupeState: "UNIQUE_APPLIED",
    duplicateOfSequence: null,
    dedupeReason: null,
    payload: { feed: "clob-market" },
    rawText: "",
    rawMessageText: null,
  };
}

describe("replayClobState", () => {
  it("replays book snapshots in order", () => {
    const events = [baseBookEvent(1, "111111", 1), baseBookEvent(2, "222222", 1)];
    const result = replayClobState(events, conditionId, tokenIds, 1785909720200, 15000);
    expect(result.errors).toHaveLength(0);
    const upBook = result.snapshot["111111"];
    const downBook = result.snapshot["222222"];
    if (upBook === undefined || downBook === undefined) {
      throw new Error("snapshot missing expected token books");
    }
    expect(upBook.state).toBe("SYNCHRONIZED");
    expect(downBook.state).toBe("SYNCHRONIZED");
    expect(result.appliedBookGenerations.get("111111")).toBe(1);
    expect(result.appliedBookGenerations.get("222222")).toBe(1);
  });

  it("advances synchronization epoch once despite both STALE_STREAM and DATA_GAP_STARTED", () => {
    const events = [
      baseBookEvent(1, "111111", 1),
      baseBookEvent(2, "222222", 1),
      lifecycleEvent(3, "STALE_STREAM", 1),
      lifecycleEvent(4, "DATA_GAP_STARTED", 1),
      baseBookEvent(5, "111111", 2),
      baseBookEvent(6, "222222", 2),
    ];
    const result = replayClobState(events, conditionId, tokenIds, 1785909720200, 15000);
    expect(result.errors).toHaveLength(0);
    const upBook = result.snapshot["111111"];
    const downBook = result.snapshot["222222"];
    if (upBook === undefined || downBook === undefined) {
      throw new Error("snapshot missing expected token books");
    }
    expect(upBook.synchronizationEpoch).toBe(1);
    expect(downBook.synchronizationEpoch).toBe(1);
  });

  it("keeps resync pending when a new-generation book is applied without DATA_GAP_ENDED", () => {
    const events = [
      baseBookEvent(1, "111111", 1),
      lifecycleEvent(2, "DATA_GAP_STARTED", 1),
      baseBookEvent(3, "111111", 2),
    ];
    const result = replayClobState(events, conditionId, tokenIds, 1785909720200, 15000);
    expect(result.errors).toHaveLength(0);
    // Guard stays pending because no DATA_GAP_ENDED exists for the clob-market gap.
    expect(result.resyncPending).toBe(true);
  });
});
