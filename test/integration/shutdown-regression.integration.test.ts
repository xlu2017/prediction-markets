import { createServer, type Server } from "node:http";
import { readFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";

import { afterEach, describe, expect, it } from "vitest";
import { type RawData, WebSocketServer, WebSocket } from "ws";

import { runRecorder } from "../../src/recorder.js";
import type { HealthSummary, RawEventEnvelope, RunManifest } from "../../src/types.js";
import { FIXED_NOW, validRawEvent } from "../helpers.js";

const cleanups: (() => Promise<void>)[] = [];

function messageText(data: RawData): string {
  if (data instanceof ArrayBuffer) return Buffer.from(data).toString("utf8");
  if (Array.isArray(data)) return Buffer.concat(data).toString("utf8");
  return data.toString("utf8");
}

afterEach(async () => {
  await Promise.allSettled(cleanups.splice(0).map(async (c) => c()));
});

async function listen(server: Server): Promise<number> {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return (server.address() as AddressInfo).port;
}

async function closeServer(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error !== undefined) reject(error);
      else resolve();
    });
  });
}

async function closeWebSocketServer(server: WebSocketServer): Promise<void> {
  for (const client of server.clients) client.terminate();
  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error !== undefined) reject(error);
      else resolve();
    });
  });
}

describe("real shutdown regression", () => {
  it("CONNECTION_CLOSED is present in events.jsonl after delayed close", async () => {
    const gammaServer = createServer((_request, response) => {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify(validRawEvent()));
    });
    const gammaPort = await listen(gammaServer);
    cleanups.push(async () => closeServer(gammaServer));

    const clobSocketRef = { current: null as WebSocket | null };
    const clobServer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    await new Promise<void>((resolve) => clobServer.once("listening", resolve));
    const clobPort = (clobServer.address() as AddressInfo).port;
    cleanups.push(async () => closeWebSocketServer(clobServer));
    clobServer.on("connection", (socket) => {
      clobSocketRef.current = socket;
      socket.on("message", (data) => {
        const text = messageText(data);
        if (!text.startsWith("{")) return;
        socket.send(
          JSON.stringify({
            event_type: "book",
            asset_id: "111111",
            market: "0xcondition",
            bids: [{ price: ".48", size: "30" }],
            asks: [{ price: ".52", size: "25" }],
            timestamp: String(FIXED_NOW),
          }),
        );
        socket.send(
          JSON.stringify({
            event_type: "book",
            asset_id: "222222",
            market: "0xcondition",
            bids: [{ price: ".47", size: "20" }],
            asks: [{ price: ".53", size: "22" }],
            timestamp: String(FIXED_NOW),
          }),
        );
      });
    });

    const referenceServer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    await new Promise<void>((resolve) => referenceServer.once("listening", resolve));
    const referencePort = (referenceServer.address() as AddressInfo).port;
    cleanups.push(async () => closeWebSocketServer(referenceServer));
    referenceServer.on("connection", (socket) => {
      socket.on("message", (data) => {
        const text = messageText(data);
        if (!text.startsWith("{")) return;
        socket.send(
          JSON.stringify({
            topic: "crypto_prices_chainlink",
            type: "update",
            timestamp: FIXED_NOW,
            payload: { symbol: "btc/usd", timestamp: FIXED_NOW, value: 67234.5 },
          }),
        );
      });
    });

    const outputBase = await mkdtemp(path.join(tmpdir(), "polymarket-shutdown-real-"));
    cleanups.push(async () => rm(outputBase, { recursive: true }));
    const controller = new AbortController();

    const recorderPromise = runRecorder(
      {
        durationSeconds: 30,
        outputDir: outputBase,
        marketSlug: "btc-updown-5m-1785909600",
        logLevel: "error",
        signal: controller.signal,
      },
      {
        gammaBaseUrl: `http://127.0.0.1:${gammaPort}`,
        clobWebSocketUrl: `ws://127.0.0.1:${clobPort}`,
        referenceWebSocketUrl: `ws://127.0.0.1:${referencePort}`,
        now: () => FIXED_NOW,
        runId: "shutdown-test-run",
        reconnectBaseMs: 10_000,
        reconnectMaximumMs: 20_000,
        clobStaleAfterMs: 500,
        referenceStaleAfterMs: 500,
        random: () => 0.5,
      },
    );

    // Wait for data to flow
    await new Promise((resolve) => setTimeout(resolve, 200));

    // Delay the WebSocket close event until after stop() has begun.
    // We close the CLOB socket, which triggers CONNECTION_CLOSED.
    // The closePromise in ManagedWebSocket ensures the close handler
    // runs before stop() resolves.
    if (clobSocketRef.current !== null && clobSocketRef.current.readyState === WebSocket.OPEN) {
      clobSocketRef.current.close(1000, "shutdown test");
    }

    // Abort the recorder
    controller.abort("shutdown test complete");

    const result = await recorderPromise;

    const manifest = JSON.parse(
      await readFile(path.join(result.runDirectory, "manifest.json"), "utf8"),
    ) as RunManifest;
    const health = JSON.parse(
      await readFile(path.join(result.runDirectory, "health.json"), "utf8"),
    ) as HealthSummary;
    const eventsText = await readFile(path.join(result.runDirectory, "events.jsonl"), "utf8");
    const events = eventsText
      .trim()
      .split("\n")
      .filter((l) => l !== "")
      .map((l) => JSON.parse(l) as RawEventEnvelope);

    // CONNECTION_CLOSED must be present in events.jsonl
    const closedEvents = events.filter((e) => e.eventType === "CONNECTION_CLOSED");
    expect(closedEvents.length).toBeGreaterThanOrEqual(1);

    // events.jsonl line count must equal manifest.finalEventCount
    expect(events.length).toBe(manifest.finalEventCount);

    // events.jsonl line count must equal health.totalEventsReceived
    expect(events.length).toBe(health.totalEventsReceived);

    // No event is appended after finalization — the last event sequence
    // must equal the line count
    const lastEvent = events[events.length - 1];
    expect(lastEvent?.sequence).toBe(events.length);

    // All sequences must be contiguous
    for (let i = 0; i < events.length; i += 1) {
      expect(events[i]?.sequence).toBe(i + 1);
    }
  });
});
