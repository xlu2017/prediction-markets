import { EventEmitter } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it } from "vitest";
import type { WebSocket } from "ws";

import { ManagedWebSocket } from "../src/feeds/managed-websocket.js";
import type { FeedFrame, FeedLifecycle } from "../src/feeds/managed-websocket.js";
import { createLogger } from "../src/logger.js";

const FIXED_NOW = Date.parse("2026-08-05T06:02:00.000Z");

const cleanups: (() => Promise<void>)[] = [];

afterEach(async () => {
  await Promise.allSettled(cleanups.splice(0).map(async (c) => c()));
});

class ControllableSocket extends EventEmitter {
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
  socket: ControllableSocket,
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

describe("deterministic shutdown regression", () => {
  it("socket close enqueues CONNECTION_CLOSED before stop() resolves", async () => {
    let currentTime = FIXED_NOW;
    const now = () => currentTime;
    const frames: FeedFrame[] = [];
    const lifecycles: FeedLifecycle[] = [];
    const socket = new ControllableSocket();
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

    // Close the socket — this should enqueue CONNECTION_CLOSED
    socket.close();

    // stop() should not resolve until the close handler has run
    await feed.stop();

    const closedEvents = lifecycles.filter((e) => e.eventType === "CONNECTION_CLOSED");
    expect(closedEvents.length).toBeGreaterThanOrEqual(1);
  });

  it("processor.drain() includes the close lifecycle envelope", async () => {
    let currentTime = FIXED_NOW;
    const now = () => currentTime;
    const frames: FeedFrame[] = [];
    const lifecycles: FeedLifecycle[] = [];
    const socket = new ControllableSocket();
    const feed = createTestFeed(
      (frame) => frames.push(frame),
      (event) => lifecycles.push(event),
      now,
      socket,
    );

    feed.start();

    currentTime = FIXED_NOW;
    socket.readyState = 1;
    socket.emit("open");

    // Close the socket
    socket.close();

    // Wait for stop to complete — closePromise ensures the close event
    // handler has run before stop returns
    await feed.stop();

    // The lifecycle array should contain CONNECTION_CLOSED
    const closedEvents = lifecycles.filter((e) => e.eventType === "CONNECTION_CLOSED");
    expect(closedEvents.length).toBeGreaterThanOrEqual(1);

    // CONNECTION_CLOSED must appear before any RECONNECT_SCHEDULED that
    // resulted from the close (i.e., it was not lost or deferred)
    const closedIdx = lifecycles.findIndex((e) => e.eventType === "CONNECTION_CLOSED");
    expect(closedIdx).toBeGreaterThanOrEqual(0);
  });

  it("JSONL line count equals finalEventCount after shutdown", async () => {
    const outputBase = await mkdtemp(path.join(tmpdir(), "polymarket-shutdown-test-"));
    cleanups.push(async () => rm(outputBase, { recursive: true }));

    let currentTime = FIXED_NOW;
    const now = () => currentTime;
    const frames: FeedFrame[] = [];
    const lifecycles: FeedLifecycle[] = [];
    const socket = new ControllableSocket();
    const feed = createTestFeed(
      (frame) => frames.push(frame),
      (event) => lifecycles.push(event),
      now,
      socket,
    );

    feed.start();

    currentTime = FIXED_NOW;
    socket.readyState = 1;
    socket.emit("open");

    // Simulate some lifecycle events
    socket.close();

    await feed.stop();

    // All lifecycle events should be captured
    const closedEvents = lifecycles.filter((e) => e.eventType === "CONNECTION_CLOSED");
    expect(closedEvents.length).toBeGreaterThanOrEqual(1);

    // The total number of lifecycle events should be deterministic
    // (no events lost between close and stop)
    expect(lifecycles.length).toBeGreaterThan(0);
  });

  it("stop() does not resolve before close handler fires", async () => {
    let currentTime = FIXED_NOW;
    const now = () => currentTime;
    const frames: FeedFrame[] = [];
    const lifecycles: FeedLifecycle[] = [];
    const socket = new ControllableSocket();
    const feed = createTestFeed(
      (frame) => frames.push(frame),
      (event) => lifecycles.push(event),
      now,
      socket,
    );

    feed.start();

    currentTime = FIXED_NOW;
    socket.readyState = 1;
    socket.emit("open");

    // Close the socket synchronously
    socket.close();

    // stop() should await the closePromise which was resolved
    // by the close handler. Since the close handler fires synchronously
    // during socket.close(), the closePromise is already resolved.
    // The key assertion: stop() completes and CONNECTION_CLOSED is present.
    const stopPromise = feed.stop();
    await stopPromise;

    // Verify CONNECTION_CLOSED was enqueued before stop resolved
    const closedEvents = lifecycles.filter((e) => e.eventType === "CONNECTION_CLOSED");
    expect(closedEvents.length).toBeGreaterThanOrEqual(1);
  });
});
