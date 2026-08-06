import { describe, expect, it } from "vitest";

import { RecorderEventProcessor } from "../src/processor.js";
import { OrderBookTracker } from "../src/book.js";
import { HealthTracker } from "../src/health.js";
import { createLogger } from "../src/logger.js";
import type { EventSource, RawEventEnvelope } from "../src/types.js";
import type { FeedFrame } from "../src/feeds/managed-websocket.js";
import { FIXED_NOW, validMarket } from "./helpers.js";

class CapturingStore {
  public envelopes: RawEventEnvelope[] = [];
  public append(envelope: RawEventEnvelope): void {
    this.envelopes.push(envelope);
  }
  public flush(): void {
    void this.envelopes;
  }
  public get count(): number {
    return this.envelopes.length;
  }
}

function makeFrame(rawText: string, gen: number): FeedFrame {
  const source: EventSource = "POLYMARKET_CLOB";
  return {
    source,
    receivedAt: FIXED_NOW,
    connectionGeneration: gen,
    rawText,
  };
}

describe("resync guard clears only on book events", () => {
  it("does not clear clobResyncPending on non-book CLOB events", async () => {
    const market = validMarket();
    const books = new OrderBookTracker([market.upTokenId, market.downTokenId]);
    const health = new HealthTracker(
      "test",
      "2026-08-05T06:02:00.000Z",
      [market.upTokenId, market.downTokenId],
      books,
    );
    const store = new CapturingStore();
    const logger = createLogger("error");
    const noop = (): void => {
      void 0;
    };
    let gapCallbacks = 0;
    const onClobGap = (): void => {
      gapCallbacks += 1;
    };
    const processor = new RecorderEventProcessor(
      "test-run",
      market,
      store as unknown as ConstructorParameters<typeof RecorderEventProcessor>[2],
      books,
      health,
      logger,
      () => FIXED_NOW,
      noop,
      noop,
      onClobGap,
    );

    // Apply initial book snapshots so books are SYNCHRONIZED
    const bookFrame = makeFrame(
      JSON.stringify({
        event_type: "book",
        asset_id: market.upTokenId,
        market: market.conditionId,
        bids: [{ price: ".48", size: "30" }],
        asks: [{ price: ".52", size: "25" }],
        timestamp: String(FIXED_NOW),
      }),
      1,
    );
    processor.enqueueFrame(bookFrame);
    const bookFrame2 = makeFrame(
      JSON.stringify({
        event_type: "book",
        asset_id: market.downTokenId,
        market: market.conditionId,
        bids: [{ price: ".47", size: "20" }],
        asks: [{ price: ".53", size: "22" }],
        timestamp: String(FIXED_NOW),
      }),
      1,
    );
    processor.enqueueFrame(bookFrame2);

    // Wait for initial processing
    await new Promise((resolve) => setTimeout(resolve, 100));

    const upHealthBefore = books.snapshot(FIXED_NOW, 50)[market.upTokenId];
    expect(upHealthBefore?.state).toBe("SYNCHRONIZED");

    // Emit STALE_STREAM lifecycle for clob-market
    processor.enqueueLifecycle({
      source: "POLYMARKET_CLOB",
      eventType: "STALE_STREAM",
      receivedAt: FIXED_NOW,
      connectionGeneration: 1,
      payload: { feed: "clob-market", staleAfterMs: 15000 },
    });

    // Emit a non-book CLOB event (tick_size_change) — should NOT clear the guard
    const nonBookFrame = makeFrame(
      JSON.stringify({
        event_type: "tick_size_change",
        asset_id: market.upTokenId,
        market: market.conditionId,
        tick_size: ".01",
        timestamp: String(FIXED_NOW),
      }),
      1,
    );
    processor.enqueueFrame(nonBookFrame);

    // Emit DATA_GAP_STARTED lifecycle for clob-market
    processor.enqueueLifecycle({
      source: "POLYMARKET_CLOB",
      eventType: "DATA_GAP_STARTED",
      receivedAt: FIXED_NOW,
      connectionGeneration: 1,
      payload: { feed: "clob-market", gapStartedAt: FIXED_NOW },
    });

    // Wait for all processing
    await new Promise((resolve) => setTimeout(resolve, 200));

    const upHealth = books.snapshot(FIXED_NOW, 50)[market.upTokenId];

    // STALE_STREAM should have triggered requireResync, setting state to RESYNC_REQUIRED
    expect(upHealth?.state).toBe("RESYNC_REQUIRED");
    // The non-book CLOB event should NOT have cleared clobResyncPending,
    // so DATA_GAP_STARTED should not have triggered a second requireClobResync.
    // gapCallbacks should be 1 (from STALE_STREAM only), not 2.
    expect(gapCallbacks).toBe(1);
  });

  it("does not clear clobResyncPending on book events from the same stale generation", async () => {
    const market = validMarket();
    const books = new OrderBookTracker([market.upTokenId, market.downTokenId]);
    const health = new HealthTracker(
      "test",
      "2026-08-05T06:02:00.000Z",
      [market.upTokenId, market.downTokenId],
      books,
    );
    const store = new CapturingStore();
    const logger = createLogger("error");
    const noop = (): void => {
      void 0;
    };
    let gapCallbacks = 0;
    const onClobGap = (): void => {
      gapCallbacks += 1;
    };
    const processor = new RecorderEventProcessor(
      "test-run",
      market,
      store as unknown as ConstructorParameters<typeof RecorderEventProcessor>[2],
      books,
      health,
      logger,
      () => FIXED_NOW,
      noop,
      noop,
      onClobGap,
    );

    // Apply initial book snapshots at generation 1
    const bookFrame = makeFrame(
      JSON.stringify({
        event_type: "book",
        asset_id: market.upTokenId,
        market: market.conditionId,
        bids: [{ price: ".48", size: "30" }],
        asks: [{ price: ".52", size: "25" }],
        timestamp: String(FIXED_NOW),
      }),
      1,
    );
    processor.enqueueFrame(bookFrame);
    const bookFrame2 = makeFrame(
      JSON.stringify({
        event_type: "book",
        asset_id: market.downTokenId,
        market: market.conditionId,
        bids: [{ price: ".47", size: "20" }],
        asks: [{ price: ".53", size: "22" }],
        timestamp: String(FIXED_NOW),
      }),
      1,
    );
    processor.enqueueFrame(bookFrame2);
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Emit STALE_STREAM lifecycle for clob-market at generation 1
    processor.enqueueLifecycle({
      source: "POLYMARKET_CLOB",
      eventType: "STALE_STREAM",
      receivedAt: FIXED_NOW,
      connectionGeneration: 1,
      payload: { feed: "clob-market", staleAfterMs: 15000 },
    });

    // A late book from the SAME stale generation 1 arrives — should NOT clear the guard
    const lateBookFrame = makeFrame(
      JSON.stringify({
        event_type: "book",
        asset_id: market.upTokenId,
        market: market.conditionId,
        bids: [{ price: ".49", size: "10" }],
        asks: [{ price: ".51", size: "15" }],
        timestamp: String(FIXED_NOW),
      }),
      1,
    );
    processor.enqueueFrame(lateBookFrame);

    // Emit DATA_GAP_STARTED lifecycle for clob-market at generation 1
    processor.enqueueLifecycle({
      source: "POLYMARKET_CLOB",
      eventType: "DATA_GAP_STARTED",
      receivedAt: FIXED_NOW,
      connectionGeneration: 1,
      payload: { feed: "clob-market", gapStartedAt: FIXED_NOW },
    });

    await new Promise((resolve) => setTimeout(resolve, 200));

    // The late book from the same generation should NOT have cleared the guard,
    // so DATA_GAP_STARTED should not trigger a second resync.
    expect(gapCallbacks).toBe(1);
  });

  it("clears clobResyncPending on book events from a strictly newer generation", async () => {
    const market = validMarket();
    const books = new OrderBookTracker([market.upTokenId, market.downTokenId]);
    const health = new HealthTracker(
      "test",
      "2026-08-05T06:02:00.000Z",
      [market.upTokenId, market.downTokenId],
      books,
    );
    const store = new CapturingStore();
    const logger = createLogger("error");
    const noop = (): void => {
      void 0;
    };
    let gapCallbacks = 0;
    const onClobGap = (): void => {
      gapCallbacks += 1;
    };
    const processor = new RecorderEventProcessor(
      "test-run",
      market,
      store as unknown as ConstructorParameters<typeof RecorderEventProcessor>[2],
      books,
      health,
      logger,
      () => FIXED_NOW,
      noop,
      noop,
      onClobGap,
    );

    // Apply initial book snapshots at generation 1
    const bookFrame = makeFrame(
      JSON.stringify({
        event_type: "book",
        asset_id: market.upTokenId,
        market: market.conditionId,
        bids: [{ price: ".48", size: "30" }],
        asks: [{ price: ".52", size: "25" }],
        timestamp: String(FIXED_NOW),
      }),
      1,
    );
    processor.enqueueFrame(bookFrame);
    const bookFrame2 = makeFrame(
      JSON.stringify({
        event_type: "book",
        asset_id: market.downTokenId,
        market: market.conditionId,
        bids: [{ price: ".47", size: "20" }],
        asks: [{ price: ".53", size: "22" }],
        timestamp: String(FIXED_NOW),
      }),
      1,
    );
    processor.enqueueFrame(bookFrame2);
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Emit STALE_STREAM lifecycle for clob-market at generation 1
    processor.enqueueLifecycle({
      source: "POLYMARKET_CLOB",
      eventType: "STALE_STREAM",
      receivedAt: FIXED_NOW,
      connectionGeneration: 1,
      payload: { feed: "clob-market", staleAfterMs: 15000 },
    });

    // A book from generation 2 (newer) arrives — should clear the guard
    const newGenBookFrame = makeFrame(
      JSON.stringify({
        event_type: "book",
        asset_id: market.upTokenId,
        market: market.conditionId,
        bids: [{ price: ".49", size: "10" }],
        asks: [{ price: ".51", size: "15" }],
        timestamp: String(FIXED_NOW),
      }),
      2,
    );
    processor.enqueueFrame(newGenBookFrame);

    // Emit DATA_GAP_STARTED lifecycle for clob-market at generation 1
    // Since the guard was cleared by the gen-2 book, this should trigger a second resync
    processor.enqueueLifecycle({
      source: "POLYMARKET_CLOB",
      eventType: "DATA_GAP_STARTED",
      receivedAt: FIXED_NOW,
      connectionGeneration: 1,
      payload: { feed: "clob-market", gapStartedAt: FIXED_NOW },
    });

    await new Promise((resolve) => setTimeout(resolve, 200));

    // The gen-2 book should have cleared the guard,
    // so DATA_GAP_STARTED should trigger a second resync.
    expect(gapCallbacks).toBe(2);
  });
});
