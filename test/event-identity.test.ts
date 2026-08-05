import { createServer, type Server } from "node:http";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";

import { afterEach, describe, expect, it } from "vitest";
import { type RawData, WebSocketServer } from "ws";

import { runRecorder } from "../src/recorder.js";
import type { RawEventEnvelope } from "../src/types.js";
import { FIXED_NOW, validRawEvent } from "./helpers.js";

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

describe("event identity contract", () => {
  it("metadata, CLOB, lifecycle and reference envelopes share the same marketId and conditionId", async () => {
    const gammaServer = createServer((_request, response) => {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify(validRawEvent()));
    });
    const gammaPort = await listen(gammaServer);
    cleanups.push(async () => closeServer(gammaServer));

    const clobServer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    await new Promise<void>((resolve) => clobServer.once("listening", resolve));
    const clobPort = (clobServer.address() as AddressInfo).port;
    cleanups.push(async () => closeWebSocketServer(clobServer));
    clobServer.on("connection", (socket) => {
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

    const outputBase = await mkdtemp(path.join(tmpdir(), "polymarket-identity-test-"));
    cleanups.push(async () => rm(outputBase, { recursive: true }));
    const controller = new AbortController();
    const abortTimer = setTimeout(() => controller.abort("test complete"), 200);

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
        runId: "identity-test-run",
        reconnectBaseMs: 10,
        reconnectMaximumMs: 20,
        clobStaleAfterMs: 500,
        referenceStaleAfterMs: 500,
        random: () => 0.5,
      },
    );
    clearTimeout(abortTimer);

    const events = (await readFile(path.join(result.runDirectory, "events.jsonl"), "utf8"))
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line) as RawEventEnvelope);

    const metadata = events.find((event) => event.eventType === "MARKET_DISCOVERED");
    const clobBook = events.find(
      (event) => event.source === "POLYMARKET_CLOB" && event.eventType === "book",
    );
    const reference = events.find((event) => event.eventType === "REFERENCE_PRICE");
    const lifecycle = events.find(
      (event) =>
        event.eventType === "CONNECTION_OPEN" ||
        event.eventType === "SUBSCRIPTION_SENT" ||
        event.eventType === "CONNECTION_ATTEMPT",
    );

    expect(metadata).toBeDefined();
    expect(clobBook).toBeDefined();
    expect(reference).toBeDefined();
    expect(lifecycle).toBeDefined();

    const expectedMarketId = "market-200";
    const expectedConditionId = "0xcondition";

    for (const event of [metadata, clobBook, reference, lifecycle]) {
      expect(event?.marketId).toBe(expectedMarketId);
      expect(event?.conditionId).toBe(expectedConditionId);
    }

    expect(clobBook?.sourceMarketId).toBe("0xcondition");
    expect(metadata?.sourceMarketId).toBeNull();
    expect(reference?.sourceMarketId).toBeNull();
  });

  it("rejects CLOB events whose market does not match the conditionId", async () => {
    const gammaServer = createServer((_request, response) => {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify(validRawEvent()));
    });
    const gammaPort = await listen(gammaServer);
    cleanups.push(async () => closeServer(gammaServer));

    const clobServer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    await new Promise<void>((resolve) => clobServer.once("listening", resolve));
    const clobPort = (clobServer.address() as AddressInfo).port;
    cleanups.push(async () => closeWebSocketServer(clobServer));
    clobServer.on("connection", (socket) => {
      socket.on("message", (data) => {
        const text = messageText(data);
        if (!text.startsWith("{")) return;
        socket.send(
          JSON.stringify({
            event_type: "book",
            asset_id: "111111",
            market: "0xWRONGCONDITION",
            bids: [{ price: ".48", size: "30" }],
            asks: [{ price: ".52", size: "25" }],
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

    const outputBase = await mkdtemp(path.join(tmpdir(), "polymarket-mismatch-test-"));
    cleanups.push(async () => rm(outputBase, { recursive: true }));
    const controller = new AbortController();
    const abortTimer = setTimeout(() => controller.abort("test complete"), 200);

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
        runId: "mismatch-test-run",
        reconnectBaseMs: 10,
        reconnectMaximumMs: 20,
        clobStaleAfterMs: 500,
        referenceStaleAfterMs: 500,
        random: () => 0.5,
      },
    );
    clearTimeout(abortTimer);

    const events = (await readFile(path.join(result.runDirectory, "events.jsonl"), "utf8"))
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line) as RawEventEnvelope);

    const malformed = events.filter((event) => event.eventType === "MALFORMED_EVENT");
    expect(malformed.length).toBeGreaterThanOrEqual(1);
    for (const event of malformed) {
      const payload = event.payload as Record<string, unknown>;
      expect(String(payload.error)).toContain("does not match conditionId");
    }
  });
});
