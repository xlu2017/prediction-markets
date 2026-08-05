import { describe, expect, it } from "vitest";

import { extractJsonDecimalLexeme } from "../src/decimal.js";

describe("Chainlink timestamp and decimal extraction", () => {
  it("extracts value lexeme from a single message", () => {
    const raw =
      '{"topic":"crypto_prices_chainlink","type":"update","timestamp":1785909720120,"payload":{"symbol":"btc/usd","timestamp":1785909720120,"value":67234.5000000000000001}}';
    const lexeme = extractJsonDecimalLexeme(raw, "value");
    expect(lexeme).toBe("67234.5000000000000001");
  });

  it("extracts per-message value lexeme from array element 0", () => {
    const msg0 = '{"payload":{"value":67234.5000000000000001}}';
    const lexeme = extractJsonDecimalLexeme(msg0, "value");
    expect(lexeme).toBe("67234.5000000000000001");
  });

  it("extracts per-message value lexeme from array element 1", () => {
    const msg1 = '{"payload":{"value":67234.5000000000000002}}';
    const lexeme = extractJsonDecimalLexeme(msg1, "value");
    expect(lexeme).toBe("67234.5000000000000002");
  });

  it("distinguishes values in two array elements using raw message slicing", () => {
    const part = '{"payload":{"value":67234.5000000000000001}}';
    const part2 = '{"payload":{"value":67234.5000000000000002}}';
    const raw = `[${part},${part2}]`;
    const msg0 = raw.slice(1, 1 + part.length);
    const msg1 = raw.slice(1 + part.length + 1, 1 + part.length + 1 + part2.length);
    const lexeme0 = extractJsonDecimalLexeme(msg0, "value");
    const lexeme1 = extractJsonDecimalLexeme(msg1, "value");
    expect(lexeme0).toBe("67234.5000000000000001");
    expect(lexeme1).toBe("67234.5000000000000002");
  });

  it("returns null when value key is absent", () => {
    const raw = '{"payload":{"symbol":"btc/usd"}}';
    const lexeme = extractJsonDecimalLexeme(raw, "value");
    expect(lexeme).toBeNull();
  });
});
