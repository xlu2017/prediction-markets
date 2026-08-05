import eventFixture from "./fixtures/valid-btc-5m-event.json" with { type: "json" };

import { validateBtcFiveMinuteEvent } from "../src/market.js";

export const FIXED_NOW = Date.parse("2026-08-05T06:02:00.000Z");

export function validRawEvent(): Record<string, unknown> {
  return structuredClone(eventFixture);
}

export function validMarket() {
  return validateBtcFiveMinuteEvent(validRawEvent(), FIXED_NOW, JSON.stringify(validRawEvent()));
}
