import { describe, expect, it } from "vitest";

import { OrderBookTracker } from "../src/book.js";
import { reconnectDelayMs, StaleDetector } from "../src/reliability.js";

describe("order-book health projection", () => {
  it("applies snapshots and changes while preserving decimal strings", () => {
    const tracker = new OrderBookTracker(["up"]);
    tracker.apply(
      {
        event_type: "book",
        asset_id: "up",
        bids: [
          { price: ".48", size: "30" },
          { price: ".49", size: "20" },
        ],
        asks: [{ price: ".52", size: "25" }],
      },
      100,
    );
    tracker.apply(
      {
        event_type: "price_change",
        price_changes: [
          { asset_id: "up", side: "BUY", price: ".49", size: "0" },
          { asset_id: "up", side: "SELL", price: ".51", size: "10" },
        ],
      },
      110,
    );

    expect(tracker.snapshot(120, 50).up).toEqual({
      tokenId: "up",
      state: "SYNCHRONIZED",
      bestBid: ".48",
      bestAsk: ".51",
      bidDepthCount: 1,
      askDepthCount: 2,
      lastSuccessfulUpdateTime: 110,
      healthy: true,
    });
    expect(tracker.snapshot(200, 50).up?.healthy).toBe(false);
  });
});

describe("transactional order-book updates", () => {
  it("requires an initial book snapshot before accepting price_change", () => {
    const tracker = new OrderBookTracker(["up"]);
    expect(() =>
      tracker.apply(
        {
          event_type: "price_change",
          price_changes: [{ asset_id: "up", side: "BUY", price: ".49", size: "10" }],
        },
        100,
      ),
    ).toThrow(/initial book snapshot/u);
    expect(tracker.snapshot(100, 50).up?.state).toBe("UNINITIALIZED");
  });

  it("rejects unknown token IDs", () => {
    const tracker = new OrderBookTracker(["up"]);
    expect(() =>
      tracker.apply(
        {
          event_type: "book",
          asset_id: "unknown",
          bids: [{ price: ".48", size: "30" }],
          asks: [],
        },
        100,
      ),
    ).toThrow(/Unknown token ID/u);
  });

  it("rejects invalid sides", () => {
    const tracker = new OrderBookTracker(["up"]);
    tracker.apply(
      { event_type: "book", asset_id: "up", bids: [{ price: ".48", size: "30" }], asks: [] },
      100,
    );
    expect(() =>
      tracker.apply(
        {
          event_type: "price_change",
          price_changes: [{ asset_id: "up", side: "BID", price: ".49", size: "10" }],
        },
        110,
      ),
    ).toThrow(/only BUY and SELL/u);
  });

  it("rejects negative sizes", () => {
    const tracker = new OrderBookTracker(["up"]);
    expect(() =>
      tracker.apply(
        { event_type: "book", asset_id: "up", bids: [{ price: ".48", size: "-5" }], asks: [] },
        100,
      ),
    ).toThrow(/negative/u);
  });

  it("rejects crossed books in snapshots", () => {
    const tracker = new OrderBookTracker(["up"]);
    expect(() =>
      tracker.apply(
        {
          event_type: "book",
          asset_id: "up",
          bids: [{ price: ".55", size: "10" }],
          asks: [{ price: ".50", size: "10" }],
        },
        100,
      ),
    ).toThrow(/Crossed book/u);
  });

  it("rolls back partial mutations when a batch fails mid-way", () => {
    const tracker = new OrderBookTracker(["up"]);
    tracker.apply(
      {
        event_type: "book",
        asset_id: "up",
        bids: [{ price: ".48", size: "30" }],
        asks: [{ price: ".52", size: "25" }],
      },
      100,
    );
    const snapshotBefore = tracker.snapshot(100, 50).up;
    expect(() =>
      tracker.apply(
        {
          event_type: "price_change",
          price_changes: [
            { asset_id: "up", side: "BUY", price: ".49", size: "10" },
            { asset_id: "up", side: "SELL", price: ".47", size: "5" },
          ],
        },
        110,
      ),
    ).toThrow(/Crossed book/u);
    const snapshotAfter = tracker.snapshot(110, 50).up;
    expect(snapshotAfter?.bestBid).toBe(snapshotBefore?.bestBid);
    expect(snapshotAfter?.bestAsk).toBe(snapshotBefore?.bestAsk);
    expect(snapshotAfter?.state).toBe("INVALID");
    expect(snapshotAfter?.lastSuccessfulUpdateTime).toBeNull();
  });

  it("recovers from INVALID state after a new full snapshot", () => {
    const tracker = new OrderBookTracker(["up"]);
    tracker.apply(
      { event_type: "book", asset_id: "up", bids: [{ price: ".55", size: "10" }], asks: [] },
      100,
    );
    expect(() =>
      tracker.apply(
        {
          event_type: "price_change",
          price_changes: [{ asset_id: "up", side: "SELL", price: ".50", size: "5" }],
        },
        110,
      ),
    ).toThrow(/Crossed book/u);
    expect(tracker.snapshot(110, 50).up?.state).toBe("INVALID");
    tracker.apply(
      {
        event_type: "book",
        asset_id: "up",
        bids: [{ price: ".48", size: "30" }],
        asks: [{ price: ".52", size: "25" }],
      },
      120,
    );
    expect(tracker.snapshot(120, 50).up?.state).toBe("SYNCHRONIZED");
  });

  it("does not refresh freshness for trade-only events", () => {
    const tracker = new OrderBookTracker(["up"]);
    tracker.apply(
      {
        event_type: "book",
        asset_id: "up",
        bids: [{ price: ".48", size: "30" }],
        asks: [{ price: ".52", size: "25" }],
      },
      100,
    );
    const before = tracker.snapshot(100, 50).up;
    tracker.apply(
      { event_type: "last_trade_price", asset_id: "up", price: ".50", size: "2.5" },
      200,
    );
    const after = tracker.snapshot(200, 50).up;
    expect(after?.lastSuccessfulUpdateTime).toBe(before?.lastSuccessfulUpdateTime);
  });
});

describe("reliability utilities", () => {
  it("caps exponential backoff and applies bounded jitter", () => {
    expect(reconnectDelayMs(0, 100, 1_000, () => 0.5)).toBe(100);
    expect(reconnectDelayMs(5, 100, 1_000, () => 1)).toBe(1_000);
  });

  it("reports one stale incident until a new observation arrives", () => {
    const detector = new StaleDetector(100);
    detector.observe(1_000);
    expect(detector.check(1_100)).toBe(false);
    expect(detector.check(1_101)).toBe(true);
    expect(detector.check(1_500)).toBe(false);
    detector.observe(1_600);
    expect(detector.check(1_701)).toBe(true);
  });
});
