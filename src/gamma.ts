import { PUBLIC_ENDPOINTS } from "./endpoints.js";
import { errorMessage } from "./logger.js";
import { validateBtcFiveMinuteEvent } from "./market.js";
import type { Logger, MarketSpecification } from "./types.js";

export type MarketDiscoveryOptions = {
  marketSlug?: string;
  nowMs?: number;
  gammaBaseUrl?: string;
  fetchImplementation?: typeof fetch;
  logger: Logger;
};

async function fetchJson(
  url: URL,
  fetchImplementation: typeof fetch,
  allowNotFound = false,
): Promise<unknown> {
  const response = await fetchImplementation(url, {
    headers: { Accept: "application/json" },
    signal: AbortSignal.timeout(10_000),
  });
  if (allowNotFound && response.status === 404) return null;
  if (!response.ok)
    throw new Error(`Gamma request failed (${response.status}) for ${url.pathname}`);
  return JSON.parse(await response.text()) as unknown;
}

function candidateSlugs(nowMs: number): string[] {
  const intervalStart = Math.floor(nowMs / 300_000) * 300;
  return [intervalStart, intervalStart - 300, intervalStart + 300].map(
    (epochSeconds) => `btc-updown-5m-${epochSeconds}`,
  );
}

function extractEventList(payload: unknown): unknown[] {
  if (Array.isArray(payload)) return payload;
  if (payload !== null && typeof payload === "object") {
    const events = (payload as Record<string, unknown>).events;
    if (Array.isArray(events)) return events;
  }
  throw new Error("Gamma event-list response has an unexpected shape");
}

export async function discoverBtcFiveMinuteMarket(
  options: MarketDiscoveryOptions,
): Promise<MarketSpecification> {
  const nowMs = options.nowMs ?? Date.now();
  const fetchImplementation = options.fetchImplementation ?? fetch;
  const baseUrl = options.gammaBaseUrl ?? PUBLIC_ENDPOINTS.gamma;

  if (options.marketSlug !== undefined) {
    const url = new URL(`/events/slug/${encodeURIComponent(options.marketSlug)}`, baseUrl);
    const event = await fetchJson(url, fetchImplementation);
    return validateBtcFiveMinuteEvent(event, nowMs);
  }

  const validationErrors: string[] = [];
  for (const slug of candidateSlugs(nowMs)) {
    try {
      const url = new URL(`/events/slug/${encodeURIComponent(slug)}`, baseUrl);
      const event = await fetchJson(url, fetchImplementation, true);
      if (event === null) continue;
      const market = validateBtcFiveMinuteEvent(event, nowMs);
      options.logger.info("Discovered market from interval slug", { slug });
      return market;
    } catch (error) {
      validationErrors.push(`${slug}: ${errorMessage(error)}`);
    }
  }

  const url = new URL("/events/keyset", baseUrl);
  url.searchParams.set("limit", "100");
  url.searchParams.set("order", "endDate");
  url.searchParams.set("ascending", "true");
  url.searchParams.set("closed", "false");
  url.searchParams.set("title_search", "Bitcoin Up or Down");
  url.searchParams.set("end_date_min", new Date(nowMs).toISOString());
  url.searchParams.set("end_date_max", new Date(nowMs + 600_000).toISOString());
  const payload = await fetchJson(url, fetchImplementation);
  const matches: MarketSpecification[] = [];
  for (const event of extractEventList(payload)) {
    try {
      matches.push(validateBtcFiveMinuteEvent(event, nowMs));
    } catch (error) {
      validationErrors.push(errorMessage(error));
    }
  }
  matches.sort((left, right) => Date.parse(left.closesAt) - Date.parse(right.closesAt));
  const match = matches[0];
  if (match !== undefined) {
    options.logger.info("Discovered market from public event listing", { slug: match.slug });
    return match;
  }

  const suffix =
    validationErrors.length > 0 ? ` Validation details: ${validationErrors.join(" | ")}` : "";
  throw new Error(`No unambiguous active BTC five-minute Up/Down market was found.${suffix}`);
}
