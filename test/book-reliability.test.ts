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
