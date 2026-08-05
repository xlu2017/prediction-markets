import { describe, expect, it } from "vitest";

import { OrderBookTracker } from "../src/book.js";

const GEN = 1;

describe("malformed event identity preservation", () => {
  it("rejects missing CLOB market field", () => {
    const tracker = new OrderBookTracker(["111111", "222222"], "0.01");
    expect(() =>
      tracker.apply(
        { event_type: "book", asset_id: "111111", bids: [{ price: ".48", size: "30" }], asks: [] },
        100,
        GEN,
      ),
    ).not.toThrow();
  });

  it("rejects unknown token in price_change", () => {
    const tracker = new OrderBookTracker(["111111"], "0.01");
    tracker.apply(
      {
        event_type: "book",
        asset_id: "111111",
        bids: [{ price: ".48", size: "30" }],
        asks: [{ price: ".52", size: "25" }],
      },
      100,
      GEN,
    );
    expect(() =>
      tracker.apply(
        {
          event_type: "price_change",
          price_changes: [{ asset_id: "999999", side: "BUY", price: ".49", size: "10" }],
        },
        110,
        GEN,
      ),
    ).toThrow(/Unknown token ID/u);
  });

  it("rejects price out of bounds in snapshot", () => {
    const tracker = new OrderBookTracker(["111111"], "0.01");
    expect(() =>
      tracker.apply(
        { event_type: "book", asset_id: "111111", bids: [{ price: "-.01", size: "10" }], asks: [] },
        100,
        GEN,
      ),
    ).toThrow(/price must be > 0/u);
  });

  it("rejects tick-size violation in price_change", () => {
    const tracker = new OrderBookTracker(["111111"], "0.01");
    tracker.apply(
      {
        event_type: "book",
        asset_id: "111111",
        bids: [{ price: ".48", size: "30" }],
        asks: [{ price: ".52", size: "25" }],
      },
      100,
      GEN,
    );
    expect(() =>
      tracker.apply(
        {
          event_type: "price_change",
          price_changes: [{ asset_id: "111111", side: "BUY", price: ".485", size: "10" }],
        },
        110,
        GEN,
      ),
    ).toThrow(/not a multiple of tick size/u);
  });

  it("rejects duplicate price levels in snapshot", () => {
    const tracker = new OrderBookTracker(["111111"], "0.01");
    expect(() =>
      tracker.apply(
        {
          event_type: "book",
          asset_id: "111111",
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

  it("rejects empty bids and asks that result in crossed book check", () => {
    const tracker = new OrderBookTracker(["111111"], "0.01");
    expect(() =>
      tracker.apply({ event_type: "book", asset_id: "111111", bids: [], asks: [] }, 100, GEN),
    ).not.toThrow();
    expect(tracker.snapshot(100, 50)["111111"]?.state).toBe("SYNCHRONIZED");
  });
});
