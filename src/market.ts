import { z } from "zod";

import { decimalString, normalizeJson } from "./decimal.js";
import type { MarketSpecification } from "./types.js";

const unknownRecord = z.record(z.string(), z.unknown());

const gammaMarketSchema = z
  .object({
    id: z.union([z.string(), z.number()]),
    conditionId: z.string().min(1),
    slug: z.string().min(1),
    question: z.string().min(1),
    resolutionSource: z.string().optional(),
    description: z.string().optional(),
    startDate: z.string().optional(),
    endDate: z.string().optional(),
    eventStartTime: z.string().optional(),
    active: z.boolean(),
    closed: z.boolean(),
    outcomes: z.union([z.string(), z.array(z.string())]),
    clobTokenIds: z.union([z.string(), z.array(z.string())]),
    orderPriceMinTickSize: z.union([z.string(), z.number()]).nullish(),
  })
  .loose();

const gammaEventSchema = z
  .object({
    id: z.union([z.string(), z.number()]),
    slug: z.string().min(1),
    title: z.string().min(1),
    description: z.string().optional(),
    resolutionSource: z.string().optional(),
    startDate: z.string().optional(),
    startTime: z.string().optional(),
    endDate: z.string().optional(),
    active: z.boolean(),
    closed: z.boolean(),
    markets: z.array(unknownRecord).min(1),
  })
  .loose();

function parseArrayField(value: string[] | string, field: string): string[] {
  if (Array.isArray(value)) return value;
  try {
    const parsed: unknown = JSON.parse(value);
    if (Array.isArray(parsed) && parsed.every((item) => typeof item === "string")) return parsed;
  } catch {
    // The common Gamma representation is a JSON-encoded array. Fail clearly below.
  }
  throw new Error(`${field} must be an array of strings or a JSON-encoded string array`);
}

function parseIsoTimestamp(candidates: (string | undefined)[], label: string): string {
  for (const candidate of candidates) {
    if (candidate === undefined || candidate.trim() === "") continue;
    const timestamp = Date.parse(candidate);
    if (Number.isFinite(timestamp)) return new Date(timestamp).toISOString();
  }
  throw new Error(`${label} timestamp is missing or invalid`);
}

function expectedOutcomeIndexes(outcomes: string[]): { up: number; down: number } {
  if (outcomes.length !== 2) throw new Error("Market must contain exactly two outcomes");
  const normalized = outcomes.map((outcome) => outcome.trim().toLowerCase());
  const up = normalized.indexOf("up");
  const down = normalized.indexOf("down");
  if (up === -1 || down === -1 || up === down) {
    throw new Error(`Expected Up and Down outcomes; received ${JSON.stringify(outcomes)}`);
  }
  return { up, down };
}

export function validateBtcFiveMinuteEvent(rawEvent: unknown, nowMs: number): MarketSpecification {
  const event = gammaEventSchema.parse(rawEvent);
  if (!event.active || event.closed) throw new Error("Event is not active and open");

  const candidates: { market: z.infer<typeof gammaMarketSchema>; error?: never }[] = [];
  const errors: string[] = [];
  for (const rawMarket of event.markets) {
    const result = gammaMarketSchema.safeParse(rawMarket);
    if (result.success) candidates.push({ market: result.data });
    else errors.push(z.prettifyError(result.error));
  }
  if (candidates.length === 0) {
    throw new Error(`Event has no parseable market: ${errors.join(" | ")}`);
  }

  const market = candidates.find(({ market: candidate }) => {
    try {
      expectedOutcomeIndexes(parseArrayField(candidate.outcomes, "outcomes"));
      return candidate.active && !candidate.closed;
    } catch {
      return false;
    }
  })?.market;
  if (market === undefined) throw new Error("Event has no active, open Up/Down market");

  const identityText = `${event.slug} ${event.title} ${market.slug} ${market.question}`;
  if (!/\b(?:btc|bitcoin)\b/iu.test(identityText)) throw new Error("Market asset is not BTC");
  if (!/\bup\b/iu.test(identityText) || !/\bdown\b/iu.test(identityText)) {
    throw new Error("Market title is not an Up/Down contract");
  }

  const opensAt = parseIsoTimestamp(
    [market.eventStartTime, event.startTime, event.startDate, market.startDate],
    "opening",
  );
  const closesAt = parseIsoTimestamp([market.endDate, event.endDate], "closing");
  const opensAtMs = Date.parse(opensAt);
  const closesAtMs = Date.parse(closesAt);
  const durationMs = closesAtMs - opensAtMs;
  const slugDeclaresFiveMinutes =
    /(?:^|-)5m(?:-|$)/u.test(event.slug) || /(?:^|-)5m(?:-|$)/u.test(market.slug);
  const wordingDeclaresFiveMinutes = /\bfive[- ]minute|\b5[- ]minute/iu.test(identityText);
  if (durationMs !== 300_000 || (!slugDeclaresFiveMinutes && !wordingDeclaresFiveMinutes)) {
    throw new Error(`Market is not a supported five-minute interval (duration ${durationMs} ms)`);
  }
  if (nowMs < opensAtMs - 15_000 || nowMs >= closesAtMs) {
    throw new Error(`Market is not currently active (${opensAt} to ${closesAt})`);
  }

  const resolutionText = [
    market.resolutionSource,
    event.resolutionSource,
    market.description,
    event.description,
  ]
    .filter((value): value is string => typeof value === "string" && value.trim() !== "")
    .join(" ");
  if (!/chainlink/iu.test(resolutionText) || !/(?:btc\s*\/\s*usd|bitcoin)/iu.test(resolutionText)) {
    throw new Error("Market rules do not identify a supported Chainlink BTC/USD reference source");
  }

  const outcomes = parseArrayField(market.outcomes, "outcomes");
  const tokenIds = parseArrayField(market.clobTokenIds, "clobTokenIds");
  const indexes = expectedOutcomeIndexes(outcomes);
  if (tokenIds.length !== outcomes.length || tokenIds.some((token) => token.trim() === "")) {
    throw new Error("Both outcome token IDs must be present");
  }

  const rawMetadata = normalizeJson(rawEvent);
  const resolutionSource = [market.resolutionSource, event.resolutionSource].find(
    (value): value is string => typeof value === "string" && value.trim() !== "",
  );
  return {
    schemaVersion: 1,
    eventId: String(event.id),
    marketId: String(market.id),
    conditionId: market.conditionId,
    slug: event.slug,
    question: market.question,
    opensAt,
    closesAt,
    resolutionSource:
      resolutionSource?.trim() ?? "Chainlink BTC/USD as specified in raw market rules",
    referenceSymbol: "btc/usd",
    referenceTopic: "crypto_prices_chainlink",
    upTokenId: tokenIds[indexes.up] ?? "",
    downTokenId: tokenIds[indexes.down] ?? "",
    tickSize:
      market.orderPriceMinTickSize === null || market.orderPriceMinTickSize === undefined
        ? null
        : decimalString(market.orderPriceMinTickSize, "tick size"),
    active: market.active,
    closed: market.closed,
    rawMetadata,
  };
}
