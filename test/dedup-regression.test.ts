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

describe("conservative CLOB deduplication", () => {
  it("retains and applies a cancellation with size 0 that shares a hash but has different content", async () => {
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
            bids: [
              { price: ".48", size: "30" },
              { price: ".49", size: "20" },
            ],
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
        socket.send(
          JSON.stringify({
            event_type: "price_change",
            market: "0xcondition",
            hash: "shared-hash-value",
            timestamp: String(FIXED_NOW + 1),
            price_changes: [
              {
                asset_id: "111111",
                side: "BUY",
                price: ".49",
                size: "10",
                hash: "shared-hash-value",
              },
            ],
          }),
        );
        socket.send(
          JSON.stringify({
            event_type: "price_change",
            market: "0xcondition",
            hash: "shared-hash-value",
            timestamp: String(FIXED_NOW + 2),
            price_changes: [
              {
                asset_id: "111111",
                side: "BUY",
                price: ".49",
                size: "0",
                hash: "shared-hash-value",
              },
            ],
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

    const outputBase = await mkdtemp(path.join(tmpdir(), "polymarket-dedup-test-"));
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
        runId: "dedup-test-run",
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

    const priceChanges = events.filter((event) => event.eventType === "price_change");
    expect(priceChanges.length).toBe(2);

    const firstChange = priceChanges[0];
    const secondChange = priceChanges[1];
    expect(firstChange?.isDuplicate).toBe(false);
    expect(secondChange?.isDuplicate).toBe(false);

    const health = JSON.parse(
      await readFile(path.join(result.runDirectory, "health.json"), "utf8"),
    ) as Record<string, unknown>;
    const finalBookHealth = health.finalBookHealth as Record<string, Record<string, unknown>>;
    const upBook = finalBookHealth["111111"];
    expect(upBook?.bestBid).toBe(".48");
    expect(upBook?.bidDepthCount).toBe(1);
  });
});
