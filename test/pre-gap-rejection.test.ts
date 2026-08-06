import { EventEmitter } from "node:events";

import { describe, expect, it } from "vitest";
import type { WebSocket } from "ws";

import { ManagedWebSocket } from "../src/feeds/managed-websocket.js";
import type { FeedFrame, FeedLifecycle } from "../src/feeds/managed-websocket.js";
import { createLogger } from "../src/logger.js";

const FIXED_NOW = Date.parse("2026-08-05T06:02:00.000Z");

class MockSocket extends EventEmitter {
  public readyState = 0;
  public send(): void {
    void 0;
  }
  public close(): void {
    this.readyState = 3;
    this.emit("close", 1000, Buffer.from(""));
  }
  public terminate(): void {
    this.readyState = 3;
  }
}

function createTestFeed(
  onFrame: (frame: FeedFrame) => void,
  onLifecycle: (event: FeedLifecycle) => void,
  now: () => number,
  socket: MockSocket,
): ManagedWebSocket {
  return new ManagedWebSocket({
    name: "test-feed",
    source: "POLYMARKET_CLOB",
    url: "ws://localhost:0",
    subscription: { type: "test" },
    pingIntervalMs: 10_000,
    staleAfterMs: 15_000,
    reconnectBaseMs: 100_000,
    reconnectMaximumMs: 1_000_000,
    logger: createLogger("error"),
    onFrame,
    onLifecycle,
    now,
    createSocket: (() => socket) as unknown as (url: string) => WebSocket,
  });
}

describe("pre-gap observation rejection", () => {
  it("rejects same-generation frames received before the gap started", async () => {
    let currentTime = FIXED_NOW;
    const now = () => currentTime;
    const frames: FeedFrame[] = [];
    const lifecycles: FeedLifecycle[] = [];
    const socket = new MockSocket();
    const feed = createTestFeed(
      (frame) => frames.push(frame),
      (event) => lifecycles.push(event),
      now,
      socket,
    );

    feed.start();

    const T1 = FIXED_NOW;
    currentTime = T1;
    socket.readyState = 1;
    socket.emit("open");

    expect(feed.isCurrentGeneration(1)).toBe(true);
    expect(feed.isPreGapObservation(T1)).toBe(false);
    feed.commitValidDataMessage(T1, 1);

    const T2 = FIXED_NOW + 5_000;
    currentTime = T2;
    socket.emit("close", 1000, Buffer.from(""));

    const gapStarted = lifecycles.find((e) => e.eventType === "DATA_GAP_STARTED");
    expect(gapStarted).toBeDefined();

    expect(feed.isCurrentGeneration(1)).toBe(true);
    expect(feed.isPreGapObservation(T1)).toBe(true);

    // Processor must NOT call commitValidDataMessage when isPreGapObservation returns true.
    // Simulate the correct processor behavior: skip commit for pre-gap observations.
    // No DATA_GAP_ENDED should have been emitted.
    const gapEnded = lifecycles.filter((e) => e.eventType === "DATA_GAP_ENDED");
    expect(gapEnded.length).toBe(0);

    await feed.stop();
  });

  it("accepts same-generation frames received after the gap started and emits DATA_GAP_ENDED", async () => {
    let currentTime = FIXED_NOW;
    const now = () => currentTime;
    const frames: FeedFrame[] = [];
    const lifecycles: FeedLifecycle[] = [];
    const socket = new MockSocket();
    const feed = createTestFeed(
      (frame) => frames.push(frame),
      (event) => lifecycles.push(event),
      now,
      socket,
    );

    feed.start();

    const T1 = FIXED_NOW;
    currentTime = T1;
    socket.readyState = 1;
    socket.emit("open");

    expect(feed.isCurrentGeneration(1)).toBe(true);
    expect(feed.isPreGapObservation(T1)).toBe(false);
    feed.commitValidDataMessage(T1, 1);

    const T2 = FIXED_NOW + 5_000;
    currentTime = T2;
    socket.emit("close", 1000, Buffer.from(""));

    const T3 = FIXED_NOW + 6_000;
    expect(feed.isCurrentGeneration(1)).toBe(true);
    expect(feed.isPreGapObservation(T3)).toBe(false);
    feed.commitValidDataMessage(T3, 1);

    const gapEnded = lifecycles.filter((e) => e.eventType === "DATA_GAP_ENDED");
    expect(gapEnded.length).toBe(1);

    await feed.stop();
  });

  it("rejects frames from a stale generation", async () => {
    let currentTime = FIXED_NOW;
    const now = () => currentTime;
    const frames: FeedFrame[] = [];
    const lifecycles: FeedLifecycle[] = [];
    const socket = new MockSocket();
    const feed = createTestFeed(
      (frame) => frames.push(frame),
      (event) => lifecycles.push(event),
      now,
      socket,
    );

    feed.start();

    const T1 = FIXED_NOW;
    currentTime = T1;
    socket.readyState = 1;
    socket.emit("open");

    expect(feed.isCurrentGeneration(1)).toBe(true);
    expect(feed.isCurrentGeneration(2)).toBe(false);

    await feed.stop();
  });
});
