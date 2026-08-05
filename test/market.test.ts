import { describe, expect, it } from "vitest";

import { validateBtcFiveMinuteEvent } from "../src/market.js";
import { FIXED_NOW, validRawEvent } from "./helpers.js";

describe("BTC five-minute market validation", () => {
  it("extracts Up and Down token IDs and retains raw metadata", () => {
    const market = validateBtcFiveMinuteEvent(validRawEvent(), FIXED_NOW);

    expect(market.upTokenId).toBe("111111");
    expect(market.downTokenId).toBe("222222");
    expect(market.referenceTopic).toBe("crypto_prices_chainlink");
    expect(market.referenceSymbol).toBe("btc/usd");
    expect(market.tickSize).toBe("0.01");
    expect(market.rawMetadata).toMatchObject({ id: "event-100" });
  });

  it("maps tokens by outcome label rather than array position", () => {
    const event = validRawEvent();
    const market = (event.markets as Record<string, unknown>[])[0];
    if (market === undefined) throw new Error("fixture market is missing");
    market.outcomes = '["Down","Up"]';
    market.clobTokenIds = '["down-token","up-token"]';

    const parsed = validateBtcFiveMinuteEvent(event, FIXED_NOW);
    expect(parsed.upTokenId).toBe("up-token");
    expect(parsed.downTokenId).toBe("down-token");
  });

  it("rejects unsupported resolution sources", () => {
    const event = validRawEvent();
    event.description = "Uses a generic exchange price.";
    event.resolutionSource = "https://example.test";
    const market = (event.markets as Record<string, unknown>[])[0];
    if (market === undefined) throw new Error("fixture market is missing");
    market.description = "Uses a generic exchange price.";
    market.resolutionSource = "https://example.test";

    expect(() => validateBtcFiveMinuteEvent(event, FIXED_NOW)).toThrow(/Chainlink BTC\/USD/u);
  });

  it("rejects ambiguous outcomes, closed markets, and inactive time windows", () => {
    const ambiguous = validRawEvent();
    const ambiguousMarket = (ambiguous.markets as Record<string, unknown>[])[0];
    if (ambiguousMarket === undefined) throw new Error("fixture market is missing");
    ambiguousMarket.outcomes = '["Yes","No"]';
    expect(() => validateBtcFiveMinuteEvent(ambiguous, FIXED_NOW)).toThrow(/Up\/Down/u);

    const closed = validRawEvent();
    closed.closed = true;
    expect(() => validateBtcFiveMinuteEvent(closed, FIXED_NOW)).toThrow(/active and open/u);

    expect(() => validateBtcFiveMinuteEvent(validRawEvent(), FIXED_NOW + 600_000)).toThrow(
      /not currently active/u,
    );
  });
});
