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

const MAX_RETRY_ATTEMPTS = 3;
const RETRY_BASE_DELAY_MS = 500;

function isRetryableStatus(status: number): boolean {
  return status === 429 || (status >= 500 && status <= 599);
}

async function fetchJsonWithRetry(
  url: URL,
  fetchImplementation: typeof fetch,
  allowNotFound: boolean,
  random: () => number,
): Promise<{ json: unknown; rawText: string }> {
  let lastError: Error | null = null;
  for (let attempt = 0; attempt < MAX_RETRY_ATTEMPTS; attempt += 1) {
    try {
      const response = await fetchImplementation(url, {
        headers: { Accept: "application/json" },
        signal: AbortSignal.timeout(10_000),
      });
      const rawText = await response.text();
      if (allowNotFound && response.status === 404) {
        return { json: null, rawText };
      }
      if (!response.ok) {
        if (isRetryableStatus(response.status) && attempt < MAX_RETRY_ATTEMPTS - 1) {
          const jitter = 0.8 + Math.min(1, Math.max(0, random())) * 0.4;
          const delay = Math.round(RETRY_BASE_DELAY_MS * 2 ** attempt * jitter);
          await new Promise<void>((resolve) => {
            const timer = setTimeout(resolve, delay);
            timer.unref();
          });
          lastError = new Error(
            `Gamma request returned retryable status ${response.status} for ${url.pathname}`,
          );
          continue;
        }
        throw new Error(`Gamma request failed (${response.status}) for ${url.pathname}`);
      }
      return { json: JSON.parse(rawText) as unknown, rawText };
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      if (
        attempt < MAX_RETRY_ATTEMPTS - 1 &&
        !(error instanceof Error && error.message.includes("Gamma request failed"))
      ) {
        const jitter = 0.8 + Math.min(1, Math.max(0, random())) * 0.4;
        const delay = Math.round(RETRY_BASE_DELAY_MS * 2 ** attempt * jitter);
        await new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, delay);
          timer.unref();
        });
        continue;
      }
      if (error instanceof Error && error.message.includes("Gamma request failed")) throw error;
    }
  }
  throw lastError ?? new Error(`Gamma request exhausted retries for ${url.pathname}`);
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
  const random = Math.random;

  if (options.marketSlug !== undefined) {
    const url = new URL(`/events/slug/${encodeURIComponent(options.marketSlug)}`, baseUrl);
    const { json, rawText } = await fetchJsonWithRetry(url, fetchImplementation, false, random);
    return validateBtcFiveMinuteEvent(json, nowMs, rawText);
  }

  const validationErrors: string[] = [];
  for (const slug of candidateSlugs(nowMs)) {
    try {
      const url = new URL(`/events/slug/${encodeURIComponent(slug)}`, baseUrl);
      const { json, rawText } = await fetchJsonWithRetry(url, fetchImplementation, true, random);
      if (json === null) continue;
      const market = validateBtcFiveMinuteEvent(json, nowMs, rawText);
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
  const { json: payload, rawText } = await fetchJsonWithRetry(
    url,
    fetchImplementation,
    false,
    random,
  );
  const matches: MarketSpecification[] = [];
  for (const event of extractEventList(payload)) {
    try {
      matches.push(validateBtcFiveMinuteEvent(event, nowMs, rawText));
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
