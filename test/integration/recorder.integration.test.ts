import { createServer, type Server } from "node:http";
import { readFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";

import { afterEach, describe, expect, it } from "vitest";
import { type RawData, WebSocketServer } from "ws";

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
  await Promise.allSettled(cleanups.splice(0).map(async (cleanup) => cleanup()));
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

describe("read-only recorder integration", () => {
  it("restores subscriptions, records both feeds, and finalizes after a signal", async () => {
    const gammaServer = createServer((_request, response) => {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify(validRawEvent()));
    });
    const gammaPort = await listen(gammaServer);
    cleanups.push(async () => closeServer(gammaServer));

    const clobSubscriptions: string[] = [];
    let clobConnections = 0;
    const clobServer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    await new Promise<void>((resolve) => clobServer.once("listening", resolve));
    const clobPort = (clobServer.address() as AddressInfo).port;
    cleanups.push(async () => closeWebSocketServer(clobServer));
    clobServer.on("connection", (socket) => {
      clobConnections += 1;
      const generation = clobConnections;
      socket.on("message", (data) => {
        const text = messageText(data);
        if (!text.startsWith("{")) return;
        clobSubscriptions.push(text);
        socket.send(
          JSON.stringify({
            event_type: "book",
            asset_id: "111111",
            market: "0xcondition",
            bids: [{ price: ".48", size: "30" }],
            asks: [{ price: ".52", size: "25" }],
            timestamp: String(FIXED_NOW + generation),
          }),
        );
        socket.send(
          JSON.stringify({
            event_type: "book",
            asset_id: "222222",
            market: "0xcondition",
            bids: [{ price: ".47", size: "20" }],
            asks: [{ price: ".53", size: "22" }],
            timestamp: String(FIXED_NOW + generation),
          }),
        );
        const trade = JSON.stringify({
          event_type: "last_trade_price",
          asset_id: "111111",
          market: "0xcondition",
          hash: "stable-trade-id",
          price: ".50",
          size: "2.5",
          timestamp: String(FIXED_NOW + generation),
        });
        socket.send(trade);
        socket.send(trade);
        if (generation === 1) setTimeout(() => socket.close(1011, "test reconnect"), 20);
        else socket.send("{malformed-json");
      });
    });

    const referenceSubscriptions: string[] = [];
    let referenceConnections = 0;
    const referenceServer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    await new Promise<void>((resolve) => referenceServer.once("listening", resolve));
    const referencePort = (referenceServer.address() as AddressInfo).port;
    cleanups.push(async () => closeWebSocketServer(referenceServer));
    referenceServer.on("connection", (socket) => {
      referenceConnections += 1;
      const generation = referenceConnections;
      socket.on("message", (data) => {
        const text = messageText(data);
        if (!text.startsWith("{")) return;
        referenceSubscriptions.push(text);
        socket.send(
          JSON.stringify({
            topic: "crypto_prices_chainlink",
            type: "update",
            timestamp: FIXED_NOW + generation,
            payload: {
              symbol: "btc/usd",
              timestamp: FIXED_NOW + generation,
              value: 67234.5 + generation,
            },
          }),
        );
        if (generation === 1) setTimeout(() => socket.close(1011, "test reconnect"), 20);
      });
    });

    const outputBase = await mkdtemp(path.join(tmpdir(), "polymarket-recorder-integration-"));
    cleanups.push(async () => rm(outputBase, { recursive: true }));
    const controller = new AbortController();
    const abortTimer = setTimeout(() => controller.abort("integration complete"), 350);

    const result = await runRecorder(
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
        runId: "integration-run",
        reconnectBaseMs: 10,
        reconnectMaximumMs: 20,
        clobStaleAfterMs: 500,
        referenceStaleAfterMs: 500,
        random: () => 0.5,
      },
    );
    clearTimeout(abortTimer);

    const manifest = JSON.parse(
      await readFile(path.join(result.runDirectory, "manifest.json"), "utf8"),
    ) as RunManifest;
    const health = JSON.parse(
      await readFile(path.join(result.runDirectory, "health.json"), "utf8"),
    ) as HealthSummary;
    const events = (await readFile(path.join(result.runDirectory, "events.jsonl"), "utf8"))
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line) as RawEventEnvelope);

    expect(manifest.completionStatus).toBe("COMPLETED");
    expect(manifest.finalEventCount).toBe(events.length);
    expect(health.bothTokenStreamsObserved).toBe(true);
    expect(health.referencePriceStreamObserved).toBe(true);
    expect(health.reconnectCount).toBeGreaterThanOrEqual(2);
    expect(health.duplicateEventCount).toBeGreaterThanOrEqual(1);
    expect(health.malformedEventCount).toBeGreaterThanOrEqual(1);
    expect(clobSubscriptions.length).toBeGreaterThanOrEqual(2);
    expect(referenceSubscriptions.length).toBeGreaterThanOrEqual(2);
    expect(events.map((event) => event.sequence)).toEqual(
      Array.from({ length: events.length }, (_, index) => index + 1),
    );
    expect(events.some((event) => event.eventType === "DATA_GAP_STARTED")).toBe(true);
    expect(events.some((event) => event.eventType === "DATA_GAP_ENDED")).toBe(true);
    const reference = events.find((event) => event.eventType === "REFERENCE_PRICE");
    const referencePayload =
      reference?.payload !== null &&
      typeof reference?.payload === "object" &&
      !Array.isArray(reference.payload)
        ? reference.payload.payload
        : null;
    const referenceValue =
      referencePayload !== null &&
      typeof referencePayload === "object" &&
      !Array.isArray(referencePayload)
        ? referencePayload.value
        : null;
    expect(typeof referenceValue).toBe("string");
    expect(reference?.rawText).toContain('"value":67235.5');
  });
});
