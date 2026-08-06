import { describe, expect, it } from "vitest";

import { validateBtcFiveMinuteEvent } from "../src/market.js";
import compatibilityFixture from "./fixtures/real-gamma-compatibility.json" with { type: "json" };

describe("public compatibility fixture", () => {
  const fixtureNow = Date.parse("2026-08-05T17:21:40Z");

  it("validates against the real Gamma response shape with actual field names", () => {
    const market = validateBtcFiveMinuteEvent(
      structuredClone(compatibilityFixture),
      fixtureNow,
      JSON.stringify(compatibilityFixture),
    );

    expect(market.schemaVersion).toBe(2);
    expect(market.conditionId).toBe(compatibilityFixture.markets[0]?.conditionId);
    expect(market.slug).toBe(compatibilityFixture.slug);
    expect(market.referenceSymbol).toBe("btc/usd");
    expect(market.referenceTopic).toBe("crypto_prices_chainlink");
    expect(market.upTokenId).not.toBe("");
    expect(market.downTokenId).not.toBe("");
    expect(market.upTokenId).not.toBe(market.downTokenId);
    expect(market.tickSize).toBe("0.01");
    expect(market.rawGammaText).toBe(JSON.stringify(compatibilityFixture));
  });

  it("preserves actual eventStartTime, startDate, endDate, outcomes and token-field shapes", () => {
    const rawMarket = compatibilityFixture.markets[0];
    if (rawMarket === undefined) throw new Error("fixture market is missing");

    expect(rawMarket.eventStartTime).toBe("2026-08-05T17:20:00Z");
    expect(typeof rawMarket.startDate).toBe("string");
    expect(rawMarket.endDate).toBe("2026-08-05T17:25:00Z");
    expect(rawMarket.outcomes).toBe('["Up", "Down"]');
    expect(typeof rawMarket.clobTokenIds).toBe("string");

    const tokenIds = JSON.parse(rawMarket.clobTokenIds) as string[];
    expect(tokenIds).toHaveLength(2);
    expect(tokenIds[0]?.length).toBeGreaterThan(50);
    expect(tokenIds[1]?.length).toBeGreaterThan(50);

    const market = validateBtcFiveMinuteEvent(
      structuredClone(compatibilityFixture),
      fixtureNow,
      JSON.stringify(compatibilityFixture),
    );
    expect(market.opensAt).toBe("2026-08-05T17:20:00.000Z");
    expect(market.closesAt).toBe("2026-08-05T17:25:00.000Z");
    expect(Date.parse(market.closesAt) - Date.parse(market.opensAt)).toBe(300_000);
  });

  it("rejects the fixture outside its active time window", () => {
    expect(() =>
      validateBtcFiveMinuteEvent(
        structuredClone(compatibilityFixture),
        fixtureNow + 600_000,
        JSON.stringify(compatibilityFixture),
      ),
    ).toThrow(/not currently active/u);
  });
});
