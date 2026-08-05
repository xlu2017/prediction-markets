import { describe, expect, it } from "vitest";

import { OrderBookTracker } from "../src/book.js";
import { reconnectDelayMs, StaleDetector } from "../src/reliability.js";

const GEN = 1;

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
      GEN,
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
      GEN,
    );

    const snap = tracker.snapshot(120, 50).up;
    expect(snap?.bestBid).toBe(".48");
    expect(snap?.bestAsk).toBe(".51");
    expect(snap?.bidDepthCount).toBe(1);
    expect(snap?.askDepthCount).toBe(2);
    expect(snap?.lastSuccessfulUpdateTime).toBe(110);
    expect(snap?.healthy).toBe(true);
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
        GEN,
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
        GEN,
      ),
    ).toThrow(/Unknown token ID/u);
  });

  it("rejects invalid sides", () => {
    const tracker = new OrderBookTracker(["up"]);
    tracker.apply(
      { event_type: "book", asset_id: "up", bids: [{ price: ".48", size: "30" }], asks: [] },
      100,
      GEN,
    );
    expect(() =>
      tracker.apply(
        {
          event_type: "price_change",
          price_changes: [{ asset_id: "up", side: "BID", price: ".49", size: "10" }],
        },
        110,
        GEN,
      ),
    ).toThrow(/only BUY and SELL/u);
  });

  it("rejects negative sizes", () => {
    const tracker = new OrderBookTracker(["up"]);
    expect(() =>
      tracker.apply(
        { event_type: "book", asset_id: "up", bids: [{ price: ".48", size: "-5" }], asks: [] },
        100,
        GEN,
      ),
    ).toThrow(/negative/u);
  });

  it("accepts crossed books in snapshots", () => {
    const tracker = new OrderBookTracker(["up"]);
    tracker.apply(
      {
        event_type: "book",
        asset_id: "up",
        bids: [{ price: ".55", size: "10" }],
        asks: [{ price: ".50", size: "10" }],
      },
      100,
      GEN,
    );
    const snap = tracker.snapshot(100, 50).up;
    expect(snap?.bestBid).toBe(".55");
    expect(snap?.bestAsk).toBe(".50");
    expect(snap?.state).toBe("SYNCHRONIZED");
  });

  it("accepts crossed books from price_change batches", () => {
    const tracker = new OrderBookTracker(["up"]);
    tracker.apply(
      {
        event_type: "book",
        asset_id: "up",
        bids: [{ price: ".48", size: "30" }],
        asks: [{ price: ".52", size: "25" }],
      },
      100,
      GEN,
    );
    tracker.apply(
      {
        event_type: "price_change",
        price_changes: [
          { asset_id: "up", side: "BUY", price: ".49", size: "10" },
          { asset_id: "up", side: "SELL", price: ".47", size: "5" },
        ],
      },
      110,
      GEN,
    );
    const snap = tracker.snapshot(110, 50).up;
    expect(snap?.bestBid).toBe(".49");
    expect(snap?.bestAsk).toBe(".47");
    expect(snap?.state).toBe("SYNCHRONIZED");
  });

  it("recovers from INVALID state after a new full snapshot", () => {
    const tracker = new OrderBookTracker(["up"]);
    tracker.apply(
      { event_type: "book", asset_id: "up", bids: [{ price: ".55", size: "10" }], asks: [] },
      100,
      GEN,
    );
    tracker.apply(
      {
        event_type: "price_change",
        price_changes: [{ asset_id: "up", side: "SELL", price: ".50", size: "5" }],
      },
      110,
      GEN,
    );
    expect(tracker.snapshot(110, 50).up?.state).toBe("SYNCHRONIZED");
    tracker.apply(
      {
        event_type: "book",
        asset_id: "up",
        bids: [{ price: ".48", size: "30" }],
        asks: [{ price: ".52", size: "25" }],
      },
      120,
      GEN,
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
      GEN,
    );
    const before = tracker.snapshot(100, 50).up;
    tracker.apply(
      { event_type: "last_trade_price", asset_id: "up", price: ".50", size: "2.5" },
      200,
      GEN,
    );
    const after = tracker.snapshot(200, 50).up;
    expect(after?.lastSuccessfulUpdateTime).toBe(before?.lastSuccessfulUpdateTime);
  });

  it("rejects prices outside (0,1) bounds", () => {
    const tracker = new OrderBookTracker(["up"]);
    expect(() =>
      tracker.apply(
        { event_type: "book", asset_id: "up", bids: [{ price: "0", size: "10" }], asks: [] },
        100,
        GEN,
      ),
    ).toThrow(/price must be > 0/u);
    expect(() =>
      tracker.apply(
        { event_type: "book", asset_id: "up", bids: [{ price: "1", size: "10" }], asks: [] },
        100,
        GEN,
      ),
    ).toThrow(/price must be < 1/u);
    expect(() =>
      tracker.apply(
        { event_type: "book", asset_id: "up", bids: [{ price: "1.5", size: "10" }], asks: [] },
        100,
        GEN,
      ),
    ).toThrow(/price must be < 1/u);
  });

  it("rejects duplicate price levels in a snapshot", () => {
    const tracker = new OrderBookTracker(["up"]);
    expect(() =>
      tracker.apply(
        {
          event_type: "book",
          asset_id: "up",
          bids: [
            { price: ".48", size: "10" },
            { price: ".48", size: "20" },
          ],
          asks: [],
        },
        100,
        GEN,
      ),
    ).toThrow(/duplicate price level/u);
  });

  it("validates tick size when provided", () => {
    const tracker = new OrderBookTracker(["up"], "0.01");
    tracker.apply(
      { event_type: "book", asset_id: "up", bids: [{ price: ".48", size: "10" }], asks: [] },
      100,
      GEN,
    );
    expect(() =>
      tracker.apply(
        {
          event_type: "price_change",
          price_changes: [{ asset_id: "up", side: "BUY", price: ".485", size: "10" }],
        },
        110,
        GEN,
      ),
    ).toThrow(/not a multiple of tick size/u);
  });
});

describe("synchronization epoch and RESYNC_REQUIRED", () => {
  it("marks both books RESYNC_REQUIRED after requireResync", () => {
    const tracker = new OrderBookTracker(["up", "down"]);
    tracker.apply(
      { event_type: "book", asset_id: "up", bids: [{ price: ".48", size: "10" }], asks: [] },
      100,
      GEN,
    );
    tracker.apply(
      { event_type: "book", asset_id: "down", bids: [{ price: ".52", size: "10" }], asks: [] },
      100,
      GEN,
    );
    expect(tracker.snapshot(100, 50).up?.state).toBe("SYNCHRONIZED");
    expect(tracker.snapshot(100, 50).down?.state).toBe("SYNCHRONIZED");
    tracker.requireResync();
    expect(tracker.snapshot(100, 50).up?.state).toBe("RESYNC_REQUIRED");
    expect(tracker.snapshot(100, 50).down?.state).toBe("RESYNC_REQUIRED");
  });

  it("rejects price_change while RESYNC_REQUIRED", () => {
    const tracker = new OrderBookTracker(["up"]);
    tracker.apply(
      { event_type: "book", asset_id: "up", bids: [{ price: ".48", size: "10" }], asks: [] },
      100,
      GEN,
    );
    tracker.requireResync();
    expect(() =>
      tracker.apply(
        {
          event_type: "price_change",
          price_changes: [{ asset_id: "up", side: "BUY", price: ".49", size: "10" }],
        },
        110,
        GEN,
      ),
    ).toThrow(/resync-required/u);
    expect(tracker.snapshot(110, 50).up?.state).toBe("RESYNC_REQUIRED");
  });

  it("one fresh token snapshot does not resynchronize the other token", () => {
    const tracker = new OrderBookTracker(["up", "down"]);
    tracker.apply(
      { event_type: "book", asset_id: "up", bids: [{ price: ".48", size: "10" }], asks: [] },
      100,
      GEN,
    );
    tracker.apply(
      { event_type: "book", asset_id: "down", bids: [{ price: ".52", size: "10" }], asks: [] },
      100,
      GEN,
    );
    tracker.requireResync();
    tracker.apply(
      { event_type: "book", asset_id: "up", bids: [{ price: ".47", size: "10" }], asks: [] },
      200,
      2,
    );
    expect(tracker.snapshot(200, 50).up?.state).toBe("SYNCHRONIZED");
    expect(tracker.snapshot(200, 50).down?.state).toBe("RESYNC_REQUIRED");
  });

  it("both books recover after valid current-generation snapshots", () => {
    const tracker = new OrderBookTracker(["up", "down"]);
    tracker.apply(
      { event_type: "book", asset_id: "up", bids: [{ price: ".48", size: "10" }], asks: [] },
      100,
      GEN,
    );
    tracker.apply(
      { event_type: "book", asset_id: "down", bids: [{ price: ".52", size: "10" }], asks: [] },
      100,
      GEN,
    );
    tracker.requireResync();
    tracker.apply(
      { event_type: "book", asset_id: "up", bids: [{ price: ".47", size: "10" }], asks: [] },
      200,
      2,
    );
    tracker.apply(
      { event_type: "book", asset_id: "down", bids: [{ price: ".53", size: "10" }], asks: [] },
      200,
      2,
    );
    expect(tracker.snapshot(200, 50).up?.state).toBe("SYNCHRONIZED");
    expect(tracker.snapshot(200, 50).down?.state).toBe("SYNCHRONIZED");
  });

  it("rejects price_change to a STALE book", () => {
    const tracker = new OrderBookTracker(["up"]);
    tracker.apply(
      { event_type: "book", asset_id: "up", bids: [{ price: ".48", size: "10" }], asks: [] },
      100,
      GEN,
    );
    tracker.markStale(200, 50);
    expect(tracker.snapshot(200, 50).up?.state).toBe("STALE");
    expect(() =>
      tracker.apply(
        {
          event_type: "price_change",
          price_changes: [{ asset_id: "up", side: "BUY", price: ".49", size: "10" }],
        },
        210,
        GEN,
      ),
    ).toThrow(/stale book/u);
  });

  it("tracks snapshotConnectionGeneration per token", () => {
    const tracker = new OrderBookTracker(["up"]);
    tracker.apply(
      { event_type: "book", asset_id: "up", bids: [{ price: ".48", size: "10" }], asks: [] },
      100,
      1,
    );
    expect(tracker.snapshot(100, 50).up?.snapshotConnectionGeneration).toBe(1);
    tracker.requireResync();
    tracker.apply(
      { event_type: "book", asset_id: "up", bids: [{ price: ".47", size: "10" }], asks: [] },
      200,
      3,
    );
    expect(tracker.snapshot(200, 50).up?.snapshotConnectionGeneration).toBe(3);
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
