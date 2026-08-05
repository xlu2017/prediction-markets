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

function setupBooks(books: OrderBookTracker, gen = 1): void {
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
    gen,
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
    gen,
  );
}

function observeReference(health: HealthTracker): void {
  health.observe(
    makeEnvelope({
      source: "POLYMARKET_REFERENCE_PRICE",
      eventType: "REFERENCE_PRICE",
      sequence: 1,
    }),
  );
}

function observeBookSnapshots(health: HealthTracker): void {
  health.observe(
    makeEnvelope({
      source: "POLYMARKET_CLOB",
      eventType: "book",
      sequence: 2,
      tokenId: "111111",
    }),
    ["111111"],
  );
  health.observe(
    makeEnvelope({
      source: "POLYMARKET_CLOB",
      eventType: "book",
      sequence: 3,
      tokenId: "222222",
    }),
    ["222222"],
  );
}

describe("capture status fail-closed semantics", () => {
  it("produces COMPLETE when all conditions are met", () => {
    const books = new OrderBookTracker(TOKENS, "0.01");
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    setupBooks(books);
    observeBookSnapshots(health);
    observeReference(health);
    const summary = health.summary(null, NOW + 100, 15_000, 10_000, 1);
    expect(summary.captureStatus).toBe("COMPLETE");
    expect(summary.usableForReplay).toBe(true);
    expect(summary.qualityFailures).toHaveLength(0);
  });

  it("produces PARTIAL when one malformed event occurred", () => {
    const books = new OrderBookTracker(TOKENS, "0.01");
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    setupBooks(books);
    observeBookSnapshots(health);
    observeReference(health);
    health.malformedEvent();
    const summary = health.summary(null, NOW + 100, 15_000, 10_000, 1);
    expect(summary.usableForReplay).toBe(false);
    expect(summary.captureStatus).toBe("PARTIAL");
    expect(summary.qualityFailures.some((f) => f.code === "MALFORMED_EVENTS_PRESENT")).toBe(true);
  });

  it("produces PARTIAL when a book is stale", () => {
    const books = new OrderBookTracker(TOKENS, "0.01");
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    setupBooks(books);
    observeBookSnapshots(health);
    observeReference(health);
    const summary = health.summary(null, NOW + 20_000, 15_000, 10_000, 1);
    expect(summary.usableForReplay).toBe(false);
    expect(summary.captureStatus).toBe("PARTIAL");
    expect(summary.qualityFailures.some((f) => f.code === "BOOK_STALE")).toBe(true);
  });

  it("produces PARTIAL when a book is invalid", () => {
    const books = new OrderBookTracker(TOKENS, "0.01");
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    setupBooks(books);
    observeBookSnapshots(health);
    observeReference(health);
    expect(() =>
      books.apply(
        {
          event_type: "price_change",
          price_changes: [{ asset_id: "111111", side: "SELL", price: ".40", size: "5" }],
        },
        NOW + 10,
        1,
      ),
    ).toThrow(/Crossed book/u);
    const summary = health.summary(null, NOW + 100, 15_000, 10_000, 1);
    expect(summary.usableForReplay).toBe(false);
    expect(summary.qualityFailures.some((f) => f.code === "BOOK_INVALID")).toBe(true);
  });

  it("produces PARTIAL when a book is RESYNC_REQUIRED", () => {
    const books = new OrderBookTracker(TOKENS, "0.01");
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    setupBooks(books);
    observeBookSnapshots(health);
    observeReference(health);
    books.requireResync();
    const summary = health.summary(null, NOW + 100, 15_000, 10_000, 1);
    expect(summary.usableForReplay).toBe(false);
    expect(summary.qualityFailures.some((f) => f.code === "BOOK_RESYNC_REQUIRED")).toBe(true);
  });

  it("produces PARTIAL when snapshot is from old generation", () => {
    const books = new OrderBookTracker(TOKENS, "0.01");
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    setupBooks(books, 1);
    observeBookSnapshots(health);
    observeReference(health);
    const summary = health.summary(null, NOW + 100, 15_000, 10_000, 2);
    expect(summary.usableForReplay).toBe(false);
    expect(summary.qualityFailures.some((f) => f.code === "BOOK_SNAPSHOT_STALE_GENERATION")).toBe(
      true,
    );
  });

  it("produces UNUSABLE when fatal error occurred", () => {
    const books = new OrderBookTracker(TOKENS, "0.01");
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    setupBooks(books);
    observeBookSnapshots(health);
    observeReference(health);
    health.markFatalError();
    const summary = health.summary(null, NOW + 100, 15_000, 10_000, 1);
    expect(summary.usableForReplay).toBe(false);
    expect(summary.captureStatus).toBe("UNUSABLE");
    expect(summary.qualityFailures.some((f) => f.code === "FATAL_PROCESSOR_ERROR")).toBe(true);
  });

  it("produces UNUSABLE for empty capture", () => {
    const books = new OrderBookTracker(TOKENS, "0.01");
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    const summary = health.summary(null, NOW + 100, 15_000, 10_000, null);
    expect(summary.usableForReplay).toBe(false);
    expect(summary.captureStatus).toBe("UNUSABLE");
  });

  it("produces PARTIAL when reference stream is missing", () => {
    const books = new OrderBookTracker(TOKENS, "0.01");
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    setupBooks(books);
    observeBookSnapshots(health);
    const summary = health.summary(null, NOW + 100, 15_000, 10_000, 1);
    expect(summary.usableForReplay).toBe(false);
    expect(summary.qualityFailures.some((f) => f.code === "REFERENCE_STREAM_MISSING")).toBe(true);
  });

  it("uses QualityFailure codes not human strings", () => {
    const books = new OrderBookTracker(TOKENS, "0.01");
    const health = new HealthTracker("test", "2026-08-05T06:02:00.000Z", TOKENS, books);
    const summary = health.summary(null, NOW + 100, 15_000, 10_000, null);
    for (const failure of summary.qualityFailures) {
      expect(typeof failure.code).toBe("string");
      expect(typeof failure.message).toBe("string");
    }
  });
});
