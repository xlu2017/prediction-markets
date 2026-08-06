import { describe, expect, it } from "vitest";

import { isMultipleOfTick } from "../src/decimal.js";

describe("exact decimal tick validation", () => {
  it("accepts exact tick multiples", () => {
    expect(isMultipleOfTick("0.48", "0.01")).toBe(true);
    expect(isMultipleOfTick("0.52", "0.01")).toBe(true);
    expect(isMultipleOfTick("0.50", "0.05")).toBe(true);
    expect(isMultipleOfTick("0.25", "0.25")).toBe(true);
  });

  it("rejects non-tick multiples", () => {
    expect(isMultipleOfTick("0.485", "0.01")).toBe(false);
    expect(isMultipleOfTick("0.123", "0.01")).toBe(false);
    expect(isMultipleOfTick("0.10", "0.03")).toBe(false);
  });

  it("handles different fraction widths", () => {
    expect(isMultipleOfTick("0.1", "0.01")).toBe(true);
    expect(isMultipleOfTick("0.10", "0.1")).toBe(true);
    expect(isMultipleOfTick("0.15", "0.005")).toBe(true);
    expect(isMultipleOfTick("0.16", "0.005")).toBe(true);
    expect(isMultipleOfTick("0.123", "0.005")).toBe(false);
  });

  it("rejects zero tick size", () => {
    expect(isMultipleOfTick("0.48", "0")).toBe(false);
  });

  it("rejects invalid inputs", () => {
    expect(isMultipleOfTick("", "0.01")).toBe(false);
    expect(isMultipleOfTick("0.48", "")).toBe(false);
    expect(isMultipleOfTick("abc", "0.01")).toBe(false);
  });
});
