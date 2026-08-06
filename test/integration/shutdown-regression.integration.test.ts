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

describe("real shutdown interleaving regression", () => {
  it("controlled close barrier: close held unresolved until after stop() initiated", async () => {
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

    const outputBase = await mkdtemp(path.join(tmpdir(), "polymarket-shutdown-barrier-"));
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
        runId: "shutdown-barrier-run",
        reconnectBaseMs: 10_000,
        reconnectMaximumMs: 20_000,
        clobStaleAfterMs: 500,
        referenceStaleAfterMs: 500,
        random: () => 0.5,
      },
    );

    // Wait for data to flow
    await new Promise((resolve) => setTimeout(resolve, 200));

    // Pause the server-side socket so it won't process the close frame
    // sent by the client during stop(). This holds the client-side close
    // event unresolved because the server won't acknowledge the close.
    const clobSocket = clobSocketRef.current;
    if (clobSocket === null) throw new Error("CLOB server socket was not established");
    const serverSocket = clobSocket;
    serverSocket.pause();

    // Abort the recorder — this triggers ManagedWebSocket.stop()
    // which calls socket.close() on the client side. The client sends
    // a close frame, but the server is paused and won't process it.
    controller.abort("controlled shutdown barrier test");

    // Confirm the recorder has NOT finalized — stop() is waiting for
    // the close event which is held by the paused server socket.
    let recorderFinalized = false;
    void recorderPromise.then(() => {
      recorderFinalized = true;
    });

    // Give stop() time to initiate and send the close frame
    await new Promise((resolve) => setTimeout(resolve, 150));

    // The recorder should NOT have finalized yet because the close
    // event is held by the paused server socket.
    expect(recorderFinalized).toBe(false);

    // Release the close event — resume the server socket so it
    // processes the close frame, acknowledges it, and the client
    // fires its close event.
    serverSocket.resume();

    // Wait for recorder to complete
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

    // Exactly one controlled-shutdown CONNECTION_CLOSED is persisted
    const closedEvents = events.filter((e) => e.eventType === "CONNECTION_CLOSED");
    expect(closedEvents.length).toBeGreaterThanOrEqual(1);

    // events line count = manifest.finalEventCount
    expect(events.length).toBe(manifest.finalEventCount);

    // events line count = health.totalEventsReceived
    expect(events.length).toBe(health.totalEventsReceived);

    // No write occurs after finalization — contiguous sequences
    for (let i = 0; i < events.length; i += 1) {
      expect(events[i]?.sequence).toBe(i + 1);
    }
  });
});
