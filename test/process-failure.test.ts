import { OrderBookTracker } from "../src/book.js";
import { HealthTracker } from "../src/health.js";
import type { RawEventEnvelope } from "../src/types.js";

import { describe, expect, it } from "vitest";

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

function setupComplete(books: OrderBookTracker, health: HealthTracker): void {
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
    makeEnvelope({ source: "POLYMARKET_CLOB", eventType: "book", sequence: 1, tokenId: "111111" }),
    ["111111"],
  );
  health.observe(
    makeEnvelope({ source: "POLYMARKET_CLOB", eventType: "book", sequence: 2, tokenId: "222222" }),
    ["222222"],
  );
  health.observe(
    makeEnvelope({
      source: "POLYMARKET_REFERENCE_PRICE",
      eventType: "REFERENCE_PRICE",
      sequence: 3,
      receivedAt: NOW,
    }),
  );
}

describe("process failure forces unusable", () => {
  it("produces COMPLETE when process succeeds and all checks pass", () => {
    const books = new OrderBookTracker(TOKENS);
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    setupComplete(books, health);
    const summary = health.summary(null, NOW + 100, 15_000, 10_000, 1, false);
    expect(summary.captureStatus).toBe("COMPLETE");
    expect(summary.usableForReplay).toBe(true);
    expect(summary.qualityFailures).toHaveLength(0);
  });

  it("produces UNUSABLE when process failed even if all other checks pass", () => {
    const books = new OrderBookTracker(TOKENS);
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    setupComplete(books, health);
    const summary = health.summary(null, NOW + 100, 15_000, 10_000, 1, true);
    expect(summary.usableForReplay).toBe(false);
    expect(summary.captureStatus).toBe("UNUSABLE");
    expect(summary.qualityFailures.some((f) => f.code === "PROCESS_FAILURE")).toBe(true);
  });

  it("includes PROCESS_FAILURE code in quality failures when process failed", () => {
    const books = new OrderBookTracker(TOKENS);
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    setupComplete(books, health);
    const summary = health.summary(null, NOW + 100, 15_000, 10_000, 1, true);
    const codes = summary.qualityFailures.map((f) => f.code);
    expect(codes).toContain("PROCESS_FAILURE");
  });
});

describe("reference freshness tracking", () => {
  it("reports referenceFresh=true when reference is recent", () => {
    const books = new OrderBookTracker(TOKENS);
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    setupComplete(books, health);
    const summary = health.summary(null, NOW + 100, 15_000, 10_000, 1, false);
    expect(summary.referenceFresh).toBe(true);
    expect(summary.lastReferenceReceivedAt).toBe(NOW);
    expect(summary.finalReferenceAgeMs).toBe(100);
  });

  it("reports referenceFresh=false and REFERENCE_STREAM_STALE when reference is old", () => {
    const books = new OrderBookTracker(TOKENS);
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    setupComplete(books, health);
    const summary = health.summary(null, NOW + 20_000, 15_000, 10_000, 1, false);
    expect(summary.referenceFresh).toBe(false);
    expect(summary.usableForReplay).toBe(false);
    expect(summary.qualityFailures.some((f) => f.code === "REFERENCE_STREAM_STALE")).toBe(true);
  });

  it("reports referenceFresh=false when no reference observed", () => {
    const books = new OrderBookTracker(TOKENS);
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    const summary = health.summary(null, NOW, 15_000, 10_000, null, false);
    expect(summary.referenceFresh).toBe(false);
    expect(summary.lastReferenceReceivedAt).toBeNull();
    expect(summary.finalReferenceAgeMs).toBeNull();
  });
});
