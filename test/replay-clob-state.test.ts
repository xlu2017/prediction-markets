import { describe, expect, it } from "vitest";

import { replayClobState } from "../src/verify-run.js";
import type { JsonValue, RawEventEnvelope } from "../src/types.js";

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
  eventType: string,
  generation: number,
  payload: Record<string, JsonValue> = { feed: "clob-market" },
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
    payload,
    rawText: "",
    rawMessageText: null,
  };
}

function lastTradeEvent(sequence: number, tokenId: string, generation: number): RawEventEnvelope {
  return {
    schemaVersion: 2,
    runId: "fixture-run-001",
    sequence,
    source: "POLYMARKET_CLOB",
    eventType: "last_trade_price",
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
      event_type: "last_trade_price",
      market: conditionId,
      asset_id: tokenId,
      price: ".5",
      size: "10",
      side: "BUY",
      timestamp: String(1785909720100 + sequence),
    },
    rawText: "",
    rawMessageText: null,
  };
}

function pongEvent(sequence: number, generation: number): RawEventEnvelope {
  return {
    schemaVersion: 2,
    runId: "fixture-run-001",
    sequence,
    source: "POLYMARKET_CLOB",
    eventType: "PONG",
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
    frameId: `frame-${sequence}`,
    messageIndex: 0,
    messageCount: 1,
    isDuplicate: false,
    dedupeState: "UNIQUE_APPLIED",
    duplicateOfSequence: null,
    dedupeReason: null,
    payload: { value: "PONG" },
    rawText: "PONG",
    rawMessageText: "PONG",
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

  it("does not double resync epoch after DATA_GAP_ENDED on a non-book new-generation event", () => {
    const events = [
      baseBookEvent(1, "111111", 1),
      lifecycleEvent(2, "DATA_GAP_STARTED", 1),
      lastTradeEvent(3, "111111", 2),
      lifecycleEvent(4, "DATA_GAP_ENDED", 1),
      lifecycleEvent(5, "STALE_STREAM", 1),
      baseBookEvent(6, "111111", 2),
    ];
    const result = replayClobState(events, conditionId, tokenIds, 1785909720200, 15000);
    expect(result.errors).toHaveLength(0);
    const upBook = result.snapshot["111111"];
    if (upBook === undefined) {
      throw new Error("snapshot missing expected token book");
    }
    expect(upBook.synchronizationEpoch).toBe(1);
  });

  it("valid PONG passes replay validation", () => {
    const events = [pongEvent(1, 1)];
    const result = replayClobState(events, conditionId, tokenIds, 1785909720200, 15000);
    expect(result.errors).toHaveLength(0);
  });

  it("price_change envelope relabeled PONG is rejected", () => {
    const events = [
      {
        ...pongEvent(1, 1),
        payload: {
          event_type: "price_change",
          market: conditionId,
          asset_id: "111111",
          price_changes: [{ asset_id: "111111", price: ".49", size: "10", side: "BUY" }],
          timestamp: String(1785909720100),
        },
      },
    ];
    const result = replayClobState(events, conditionId, tokenIds, 1785909720200, 15000);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0]).toMatch(/PONG.*market-data payload/);
  });

  it("last_trade_price envelope relabeled CONNECTION_OPEN is rejected", () => {
    const events = [
      {
        ...lifecycleEvent(1, "CONNECTION_OPEN", 1),
        payload: {
          event_type: "last_trade_price",
          market: conditionId,
          asset_id: "111111",
          price: ".5",
          size: "10",
          side: "BUY",
          timestamp: String(1785909720100),
        },
      },
    ];
    const result = replayClobState(events, conditionId, tokenIds, 1785909720200, 15000);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0]).toMatch(/CONNECTION_OPEN.*market-data payload/);
  });

  it("PONG with sourceMarketId set is rejected", () => {
    const events = [{ ...pongEvent(1, 1), sourceMarketId: conditionId }];
    const result = replayClobState(events, conditionId, tokenIds, 1785909720200, 15000);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0]).toMatch(/PONG.*sourceMarketId/);
  });

  it("PONG with payload.event_type is rejected", () => {
    const events = [
      {
        ...pongEvent(1, 1),
        payload: { value: "PONG", event_type: "PONG" },
      },
    ];
    const result = replayClobState(events, conditionId, tokenIds, 1785909720200, 15000);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0]).toMatch(/PONG.*market-data payload/);
  });
});
