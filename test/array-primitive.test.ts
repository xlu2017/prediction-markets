import { describe, expect, it } from "vitest";

import { RecorderEventProcessor } from "../src/processor.js";
import { OrderBookTracker } from "../src/book.js";
import { HealthTracker } from "../src/health.js";
import { createLogger } from "../src/logger.js";
import type { RawEventEnvelope } from "../src/types.js";
import { FIXED_NOW, validMarket } from "./helpers.js";

class CapturingStore {
  public envelopes: RawEventEnvelope[] = [];
  public append(envelope: RawEventEnvelope): void {
    this.envelopes.push(envelope);
  }
  public flush(): void {
    void this.envelopes;
  }
  public get count(): number {
    return this.envelopes.length;
  }
}

describe("array slicing with primitive elements", () => {
  it("emits a malformed envelope for every array element including primitives", async () => {
    const market = validMarket();
    const books = new OrderBookTracker([market.upTokenId, market.downTokenId]);
    const health = new HealthTracker(
      "test",
      "2026-08-05T06:02:00.000Z",
      [market.upTokenId, market.downTokenId],
      books,
    );
    const store = new CapturingStore();
    const logger = createLogger("error");
    const noop = (): void => {
      void 0;
    };
    const acceptAll = (): boolean => true;
    const processor = new RecorderEventProcessor(
      "test-run",
      market,
      store as unknown as ConstructorParameters<typeof RecorderEventProcessor>[2],
      books,
      health,
      logger,
      () => FIXED_NOW,
      acceptAll,
      noop,
      noop,
      noop,
    );

    const raw = JSON.stringify([
      {
        event_type: "book",
        asset_id: "111111",
        market: "0xcondition",
        bids: [{ price: ".48", size: "30" }],
        asks: [{ price: ".52", size: "25" }],
        timestamp: String(FIXED_NOW),
      },
      null,
      7,
      "unexpected",
    ]);

    processor.enqueueFrame({
      source: "POLYMARKET_CLOB",
      rawText: raw,
      receivedAt: FIXED_NOW,
      connectionGeneration: 1,
    });
    await processor.drain();

    const clobEvents = store.envelopes.filter((e) => e.source === "POLYMARKET_CLOB");
    expect(clobEvents.length).toBe(4);
    expect(clobEvents[0]?.eventType).toBe("book");
    expect(clobEvents[0]?.messageIndex).toBe(0);
    expect(clobEvents[1]?.eventType).toBe("MALFORMED_EVENT");
    expect(clobEvents[1]?.messageIndex).toBe(1);
    expect(clobEvents[2]?.eventType).toBe("MALFORMED_EVENT");
    expect(clobEvents[2]?.messageIndex).toBe(2);
    expect(clobEvents[3]?.eventType).toBe("MALFORMED_EVENT");
    expect(clobEvents[3]?.messageIndex).toBe(3);
    for (const e of clobEvents) {
      expect(e.messageCount).toBe(4);
    }
  });
});
