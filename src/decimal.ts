import type { JsonValue } from "./types.js";

const decimalKey = /(?:^|_)(?:ask|bid|liquidity|price|quantity|size|spread|value)(?:$|_)/iu;
const decimalPattern = /^-?(?:(?:0|[1-9]\d*)(?:\.\d+)?|\.\d+)$/u;

export function isDecimalString(value: unknown): value is string {
  return typeof value === "string" && decimalPattern.test(value);
}

export function decimalString(value: unknown, label: string): string {
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  if (isDecimalString(value)) return value;
  throw new Error(`${label} must be a finite decimal value`);
}

export function exactWireDecimalString(value: unknown, label: string): string {
  if (isDecimalString(value)) return value;
  throw new Error(`${label} must arrive as a decimal string`);
}

export function assertExactWireDecimals(value: unknown, parentKey = "payload"): void {
  if (Array.isArray(value)) {
    for (const item of value) assertExactWireDecimals(item, parentKey);
    return;
  }
  if (value === null || typeof value !== "object") return;
  for (const [key, item] of Object.entries(value)) {
    if (decimalKey.test(key) && typeof item === "number") {
      throw new Error(`${parentKey}.${key} must arrive as a decimal string`);
    }
    assertExactWireDecimals(item, `${parentKey}.${key}`);
  }
}

export function extractJsonDecimalLexeme(rawText: string, key: string): string | null {
  const escapedKey = key.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  const pattern = new RegExp(
    `"${escapedKey}"\\s*:\\s*(-?(?:0|[1-9]\\d*)(?:\\.\\d+)?(?:[eE][+-]?\\d+)?)`,
    "u",
  );
  return pattern.exec(rawText)?.[1] ?? null;
}

export function normalizeJson(value: unknown, parentKey = ""): JsonValue {
  if (value === null || typeof value === "boolean" || typeof value === "string") return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error("Cannot serialize a non-finite number");
    return decimalKey.test(parentKey) ? String(value) : value;
  }
  if (Array.isArray(value)) return value.map((item) => normalizeJson(item, parentKey));
  if (typeof value === "object") {
    const normalized: Record<string, JsonValue> = {};
    for (const [key, item] of Object.entries(value)) normalized[key] = normalizeJson(item, key);
    return normalized;
  }
  throw new Error(`Unsupported JSON value: ${typeof value}`);
}

function splitDecimal(value: string): { integer: string; fraction: string; negative: boolean } {
  const negative = value.startsWith("-");
  const unsigned = negative ? value.slice(1) : value;
  const [rawInteger = "0", fraction = ""] = unsigned.split(".");
  const integer = rawInteger === "" ? "0" : rawInteger;
  return { integer: integer.replace(/^0+(?=\d)/u, ""), fraction, negative };
}

export function compareDecimalStrings(left: string, right: string): number {
  const a = splitDecimal(left);
  const b = splitDecimal(right);
  if (a.negative !== b.negative) return a.negative ? -1 : 1;

  const sign = a.negative ? -1 : 1;
  if (a.integer.length !== b.integer.length)
    return Math.sign(a.integer.length - b.integer.length) * sign;
  if (a.integer !== b.integer) return (a.integer < b.integer ? -1 : 1) * sign;

  const width = Math.max(a.fraction.length, b.fraction.length);
  const af = a.fraction.padEnd(width, "0");
  const bf = b.fraction.padEnd(width, "0");
  if (af === bf) return 0;
  return (af < bf ? -1 : 1) * sign;
}

export function isMultipleOfTick(price: string, tickSize: string): boolean {
  if (!isDecimalString(price) || !isDecimalString(tickSize)) return false;
  const p = splitDecimal(price);
  const t = splitDecimal(tickSize);
  if (p.negative || t.negative) return false;
  const fractionWidth = Math.max(p.fraction.length, t.fraction.length);
  const pScaled = BigInt(p.integer + p.fraction.padEnd(fractionWidth, "0"));
  const tScaled = BigInt(t.integer + t.fraction.padEnd(fractionWidth, "0"));
  if (tScaled === 0n) return false;
  return pScaled % tScaled === 0n;
}
