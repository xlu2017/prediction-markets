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

describe("array-frame processing", () => {
  it("processes two distinct CLOB messages in a single WebSocket frame array", async () => {
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
        const arr = [
          {
            event_type: "book",
            asset_id: "111111",
            market: "0xcondition",
            bids: [{ price: ".48", size: "30" }],
            asks: [{ price: ".52", size: "25" }],
            timestamp: String(FIXED_NOW),
          },
          {
            event_type: "book",
            asset_id: "222222",
            market: "0xcondition",
            bids: [{ price: ".47", size: "20" }],
            asks: [{ price: ".53", size: "22" }],
            timestamp: String(FIXED_NOW),
          },
        ];
        socket.send(JSON.stringify(arr));
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

    const outputBase = await mkdtemp(path.join(tmpdir(), "polymarket-array-test-"));
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
        runId: "array-frame-test",
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

    const bookEvents = events.filter(
      (e) => e.source === "POLYMARKET_CLOB" && e.eventType === "book",
    );
    expect(bookEvents.length).toBe(2);
    expect(bookEvents[0]?.tokenId).toBe("111111");
    expect(bookEvents[1]?.tokenId).toBe("222222");
    expect(bookEvents[0]?.messageIndex).toBe(0);
    expect(bookEvents[1]?.messageIndex).toBe(1);
    expect(bookEvents[0]?.messageCount).toBe(2);
    expect(bookEvents[1]?.messageCount).toBe(2);
    expect(bookEvents[0]?.isDuplicate).toBe(false);
    expect(bookEvents[1]?.isDuplicate).toBe(false);
    expect(bookEvents[0]?.dedupeState).toBe("UNIQUE_APPLIED");
    expect(bookEvents[1]?.dedupeState).toBe("UNIQUE_APPLIED");
    expect(bookEvents[0]?.rawMessageText).not.toBe(bookEvents[1]?.rawMessageText);
  });

  it("processes RTDS array with multiple price observations", async () => {
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
        const arr = [
          {
            topic: "crypto_prices_chainlink",
            type: "update",
            timestamp: FIXED_NOW,
            payload: { symbol: "btc/usd", timestamp: FIXED_NOW, value: 67234.5 },
          },
          {
            topic: "crypto_prices_chainlink",
            type: "update",
            timestamp: FIXED_NOW + 1,
            payload: { symbol: "btc/usd", timestamp: FIXED_NOW + 1, value: 67234.6 },
          },
        ];
        socket.send(JSON.stringify(arr));
      });
    });

    const outputBase = await mkdtemp(path.join(tmpdir(), "polymarket-rtds-array-test-"));
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
        runId: "rtds-array-test",
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

    const refEvents = events.filter((e) => e.eventType === "REFERENCE_PRICE");
    expect(refEvents.length).toBe(2);
    expect(refEvents[0]?.messageIndex).toBe(0);
    expect(refEvents[1]?.messageIndex).toBe(1);
    expect(refEvents[0]?.messageCount).toBe(2);
    expect(refEvents[0]?.isDuplicate).toBe(false);
    expect(refEvents[1]?.isDuplicate).toBe(false);
    const payload0 = refEvents[0]?.payload as { payload: { value: string } };
    const payload1 = refEvents[1]?.payload as { payload: { value: string } };
    expect(payload0.payload.value).toBe("67234.5");
    expect(payload1.payload.value).toBe("67234.6");
  });
});
