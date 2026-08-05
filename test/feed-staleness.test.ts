import { describe, expect, it } from "vitest";

import { OrderBookTracker } from "../src/book.js";
import { HealthTracker } from "../src/health.js";
import type { RawEventEnvelope } from "../src/types.js";

const TOKENS: [string, string] = ["111111", "222222"];
const NOW = 1785909720000;

function makeEnvelope(
  overrides: Partial<RawEventEnvelope> & { source: RawEventEnvelope["source"]; eventType: string },
): RawEventEnvelope {
  return {
    schemaVersion: 2,
    runId: "test",
    sequence: 0,
    marketId: "market-200",
    conditionId: "0xcondition",
    sourceMarketId: null,
    tokenId: null,
    observedTokenIds: null,
    sourceTimestamp: null,
    topLevelSourceTimestamp: null,
    payloadSourceTimestamp: null,
    receivedAt: NOW,
    processedAt: NOW,
    connectionGeneration: null,
    frameId: null,
    messageIndex: null,
    messageCount: null,
    isDuplicate: false,
    dedupeState: "UNIQUE_APPLIED",
    duplicateOfSequence: null,
    dedupeReason: null,
    payload: {},
    rawText: null,
    rawMessageText: null,
    ...overrides,
  };
}

describe("feed-specific staleness thresholds", () => {
  it("CLOB threshold shorter than RTDS: book is stale while RTDS is healthy", () => {
    const books = new OrderBookTracker(TOKENS, "0.01");
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    books.apply(
      {
        event_type: "book",
        asset_id: "111111",
        market: "0xcondition",
        bids: [{ price: ".48", size: "30" }],
        asks: [{ price: ".52", size: "25" }],
        timestamp: String(NOW),
      },
      NOW,
      1,
    );
    books.apply(
      {
        event_type: "book",
        asset_id: "222222",
        market: "0xcondition",
        bids: [{ price: ".47", size: "20" }],
        asks: [{ price: ".53", size: "22" }],
        timestamp: String(NOW),
      },
      NOW,
      1,
    );
    health.observe(
      makeEnvelope({
        source: "POLYMARKET_CLOB",
        eventType: "book",
        sequence: 1,
        tokenId: "111111",
      }),
      ["111111"],
    );
    health.observe(
      makeEnvelope({
        source: "POLYMARKET_CLOB",
        eventType: "book",
        sequence: 2,
        tokenId: "222222",
      }),
      ["222222"],
    );
    health.observe(
      makeEnvelope({
        source: "POLYMARKET_REFERENCE_PRICE",
        eventType: "REFERENCE_PRICE",
        sequence: 3,
      }),
    );
    const clobStale = 5_000;
    const rtdsStale = 20_000;
    const summary = health.summary(null, NOW + 10_000, clobStale, rtdsStale, 1);
    expect(summary.finalBookHealth["111111"]?.state).toBe("STALE");
    expect(summary.usableForReplay).toBe(false);
    expect(summary.qualityFailures.some((f) => f.code === "BOOK_STALE")).toBe(true);
  });

  it("RTDS threshold shorter than CLOB: book remains healthy", () => {
    const books = new OrderBookTracker(TOKENS, "0.01");
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    books.apply(
      {
        event_type: "book",
        asset_id: "111111",
        market: "0xcondition",
        bids: [{ price: ".48", size: "30" }],
        asks: [{ price: ".52", size: "25" }],
        timestamp: String(NOW),
      },
      NOW,
      1,
    );
    books.apply(
      {
        event_type: "book",
        asset_id: "222222",
        market: "0xcondition",
        bids: [{ price: ".47", size: "20" }],
        asks: [{ price: ".53", size: "22" }],
        timestamp: String(NOW),
      },
      NOW,
      1,
    );
    health.observe(
      makeEnvelope({
        source: "POLYMARKET_CLOB",
        eventType: "book",
        sequence: 1,
        tokenId: "111111",
      }),
      ["111111"],
    );
    health.observe(
      makeEnvelope({
        source: "POLYMARKET_CLOB",
        eventType: "book",
        sequence: 2,
        tokenId: "222222",
      }),
      ["222222"],
    );
    health.observe(
      makeEnvelope({
        source: "POLYMARKET_REFERENCE_PRICE",
        eventType: "REFERENCE_PRICE",
        sequence: 3,
        receivedAt: NOW + 9_000,
      }),
    );
    const clobStale = 20_000;
    const rtdsStale = 5_000;
    const summary = health.summary(null, NOW + 10_000, clobStale, rtdsStale, 1);
    expect(summary.finalBookHealth["111111"]?.state).toBe("SYNCHRONIZED");
    expect(summary.usableForReplay).toBe(true);
  });

  it("stores both thresholds in health summary", () => {
    const books = new OrderBookTracker(TOKENS, "0.01");
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    const summary = health.summary(null, NOW, 15_000, 10_000, null);
    expect(summary.clobStaleAfterMs).toBe(15_000);
    expect(summary.referenceStaleAfterMs).toBe(10_000);
  });
});
