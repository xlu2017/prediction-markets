import { describe, expect, it } from "vitest";

import { discoverBtcFiveMinuteMarket } from "../src/gamma.js";
import type { Logger } from "../src/types.js";
import { FIXED_NOW, validRawEvent } from "./helpers.js";

const logger: Logger = {
  debug: () => undefined,
  error: () => undefined,
  info: () => undefined,
  warn: () => undefined,
};

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json" },
  });
}

describe("Gamma market discovery", () => {
  it("discovers the current interval without relying only on a permanent slug", async () => {
    const requested: string[] = [];
    const implementation = ((input: URL | Request | string) => {
      const url = new URL(input instanceof Request ? input.url : input);
      requested.push(url.pathname);
      if (url.pathname.endsWith("btc-updown-5m-1785909600")) {
        return Promise.resolve(jsonResponse(validRawEvent()));
      }
      return Promise.resolve(jsonResponse({ message: "not found" }, 404));
    }) as typeof fetch;

    const market = await discoverBtcFiveMinuteMarket({
      logger,
      nowMs: FIXED_NOW,
      gammaBaseUrl: "https://gamma.test",
      fetchImplementation: implementation,
    });

    expect(market.slug).toBe("btc-updown-5m-1785909600");
    expect(requested.some((pathname) => pathname.startsWith("/events/slug/"))).toBe(true);
  });

  it("falls back to a filtered public event listing", async () => {
    const requested: URL[] = [];
    const implementation = ((input: URL | Request | string) => {
      const url = new URL(input instanceof Request ? input.url : input);
      requested.push(url);
      if (url.pathname === "/events/keyset") {
        return Promise.resolve(jsonResponse({ events: [validRawEvent()] }));
      }
      return Promise.resolve(jsonResponse({ message: "not found" }, 404));
    }) as typeof fetch;

    const market = await discoverBtcFiveMinuteMarket({
      logger,
      nowMs: FIXED_NOW,
      gammaBaseUrl: "https://gamma.test",
      fetchImplementation: implementation,
    });

    expect(market.marketId).toBe("market-200");
    const listRequest = requested.find((url) => url.pathname === "/events/keyset");
    expect(listRequest?.searchParams.get("closed")).toBe("false");
    expect(listRequest?.searchParams.get("title_search")).toBe("Bitcoin Up or Down");
  });

  it("validates an explicitly supplied slug instead of trusting it", async () => {
    const invalid = validRawEvent();
    invalid.closed = true;
    const implementation = (() => Promise.resolve(jsonResponse(invalid))) as typeof fetch;

    await expect(
      discoverBtcFiveMinuteMarket({
        logger,
        nowMs: FIXED_NOW,
        marketSlug: "btc-updown-5m-1785909600",
        gammaBaseUrl: "https://gamma.test",
        fetchImplementation: implementation,
      }),
    ).rejects.toThrow(/active and open/u);
  });
});
