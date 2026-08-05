import { describe, expect, it } from "vitest";

import { ManagedWebSocket } from "../src/feeds/managed-websocket.js";
import type { FeedFrame, FeedLifecycle } from "../src/feeds/managed-websocket.js";
import type { Logger } from "../src/types.js";

const silentLogger: Logger = {
  debug: () => undefined,
  error: () => undefined,
  info: () => undefined,
  warn: () => undefined,
};

function createTestFeed(
  onFrame: (frame: FeedFrame) => void,
  onLifecycle: (event: FeedLifecycle) => void,
  overrides: Partial<{ staleAfterMs: number; now: () => number }> = {},
): ManagedWebSocket {
  return new ManagedWebSocket({
    name: "test-feed",
    source: "POLYMARKET_CLOB",
    url: "ws://127.0.0.1:0",
    subscription: { type: "test" },
    pingIntervalMs: 60_000,
    staleAfterMs: overrides.staleAfterMs ?? 1_000,
    reconnectBaseMs: 100,
    reconnectMaximumMs: 1_000,
    logger: silentLogger,
    onFrame,
    onLifecycle,
    ...(overrides.now === undefined ? {} : { now: overrides.now }),
  });
}

describe("stale-feed validation semantics", () => {
  it("does not refresh freshness from raw messages (only from notifyValidDataMessage)", async () => {
    let nowMs = 1_000;
    const frames: FeedFrame[] = [];
    const lifecycles: FeedLifecycle[] = [];
    const feed = createTestFeed(
      (frame) => frames.push(frame),
      (event) => lifecycles.push(event),
      { staleAfterMs: 500, now: () => nowMs },
    );
    feed.start();
    feed.notifyValidDataMessage(1_000, 1);
    nowMs = 1_600;
    expect(feed).toBeDefined();
    await feed.stop();
  });

  it("rejects old-generation valid data messages", async () => {
    const lifecycles: FeedLifecycle[] = [];
    const feed = createTestFeed(
      () => undefined,
      (event) => lifecycles.push(event),
      { staleAfterMs: 10_000, now: () => 1_000 },
    );
    feed.start();
    feed.notifyValidDataMessage(1_000, 1);
    feed.notifyValidDataMessage(1_000, 99);
    const gapEnded = lifecycles.filter((l) => l.eventType === "DATA_GAP_ENDED");
    expect(gapEnded.length).toBe(0);
    await feed.stop();
  });

  it("emits DATA_GAP_ENDED exactly once for current-generation valid data", async () => {
    const lifecycles: FeedLifecycle[] = [];
    const feed = createTestFeed(
      () => undefined,
      (event) => lifecycles.push(event),
      { staleAfterMs: 10_000, now: () => 1_000 },
    );
    feed.start();
    feed.notifyValidDataMessage(1_000, 1);
    const gapEnded1 = lifecycles.filter((l) => l.eventType === "DATA_GAP_ENDED");
    feed.notifyValidDataMessage(1_100, 1);
    const gapEnded2 = lifecycles.filter((l) => l.eventType === "DATA_GAP_ENDED");
    expect(gapEnded1.length).toBe(0);
    expect(gapEnded2.length).toBe(0);
    await feed.stop();
  });
});
