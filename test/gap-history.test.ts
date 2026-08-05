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

function gapEnvelope(
  eventType: "DATA_GAP_STARTED" | "DATA_GAP_ENDED",
  feed: string,
  receivedAt: number,
): RawEventEnvelope {
  return makeEnvelope({
    source: "POLYMARKET_CLOB",
    eventType,
    sequence: 0,
    receivedAt,
    payload: { feed },
  });
}

describe("gap history tracking", () => {
  it("records closed gap count, total duration, and maximum duration", () => {
    const books = new OrderBookTracker(TOKENS);
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    health.observe(gapEnvelope("DATA_GAP_STARTED", "clob-market", NOW));
    health.observe(gapEnvelope("DATA_GAP_ENDED", "clob-market", NOW + 3_000));
    health.observe(gapEnvelope("DATA_GAP_STARTED", "clob-market", NOW + 10_000));
    health.observe(gapEnvelope("DATA_GAP_ENDED", "clob-market", NOW + 16_000));
    const summary = health.summary(null, NOW + 20_000, 15_000, 10_000, 1);
    expect(summary.dataGapCount).toBe(2);
    expect(summary.closedGapCount).toBe(2);
    expect(summary.totalGapDurationMs).toBe(9_000);
    expect(summary.maximumGapDurationMs).toBe(6_000);
  });

  it("any closed gap invalidates COMPLETE via CLOSED_DATA_GAP", () => {
    const books = new OrderBookTracker(TOKENS);
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
        sourceTimestamp: NOW,
      }),
    );
    health.observe(gapEnvelope("DATA_GAP_STARTED", "clob-market", NOW + 1_000));
    health.observe(gapEnvelope("DATA_GAP_ENDED", "clob-market", NOW + 2_000));
    const summary = health.summary(null, NOW + 100, 15_000, 10_000, 1);
    expect(summary.closedGapCount).toBe(1);
    expect(summary.usableForReplay).toBe(false);
    expect(summary.captureStatus).toBe("PARTIAL");
    expect(summary.qualityFailures.some((f) => f.code === "CLOSED_DATA_GAP")).toBe(true);
  });

  it("open gaps still report OPEN_DATA_GAP and are not counted as closed", () => {
    const books = new OrderBookTracker(TOKENS);
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    health.observe(gapEnvelope("DATA_GAP_STARTED", "clob-market", NOW));
    const summary = health.summary(null, NOW + 100, 15_000, 10_000, 1);
    expect(summary.dataGapCount).toBe(1);
    expect(summary.closedGapCount).toBe(0);
    expect(summary.totalGapDurationMs).toBe(0);
    expect(summary.usableForReplay).toBe(false);
    expect(summary.qualityFailures.some((f) => f.code === "OPEN_DATA_GAP")).toBe(true);
  });
});
