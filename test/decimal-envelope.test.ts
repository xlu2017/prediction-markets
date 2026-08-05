import { describe, expect, it } from "vitest";

import {
  assertExactWireDecimals,
  compareDecimalStrings,
  decimalString,
  extractJsonDecimalLexeme,
  normalizeJson,
} from "../src/decimal.js";
import { EventSequencer } from "../src/envelope.js";

describe("decimal preservation", () => {
  it("accepts source-style decimals and compares without floating point", () => {
    expect(decimalString(".48", "price")).toBe(".48");
    expect(compareDecimalStrings("0.4800", ".48")).toBe(0);
    expect(compareDecimalStrings("0.5000000000000001", "0.5")).toBeGreaterThan(0);
  });

  it("converts numeric price, size, and value fields to strings", () => {
    expect(
      normalizeJson({
        timestamp: 1_234,
        value: 67_234.5,
        bids: [{ price: 0.48, size: 30 }],
      }),
    ).toEqual({
      timestamp: 1_234,
      value: "67234.5",
      bids: [{ price: "0.48", size: "30" }],
    });
  });

  it("extracts the exact reference-price lexeme and rejects numeric CLOB decimals", () => {
    const raw = '{"payload":{"value":67234.5000000000000001}}';
    expect(extractJsonDecimalLexeme(raw, "value")).toBe("67234.5000000000000001");
    expect(() => assertExactWireDecimals({ price: 0.48 })).toThrow(/decimal string/u);
    expect(() => assertExactWireDecimals({ price: ".48", size: "20" })).not.toThrow();
  });
});

describe("event envelopes", () => {
  it("assigns strictly increasing local sequence numbers and retains raw text", () => {
    const sequencer = new EventSequencer("run-1");
    const first = sequencer.next({
      source: "POLYMARKET_REFERENCE_PRICE",
      eventType: "REFERENCE_PRICE",
      receivedAt: 10,
      processedAt: 11,
      payload: { value: 10.25 },
      rawText: '{"value":10.2500}',
    });
    const second = sequencer.next({
      source: "POLYMARKET_CLOB",
      eventType: "book",
      receivedAt: 12,
      processedAt: 13,
      payload: { price: ".48" },
    });

    expect([first.sequence, second.sequence]).toEqual([1, 2]);
    expect(first.payload).toEqual({ value: "10.25" });
    expect(first.rawText).toBe('{"value":10.2500}');
  });
});
