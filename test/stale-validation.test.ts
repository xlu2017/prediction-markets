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

describe("stale-feed validation semantics", () => {
  it("emits exactly one DATA_GAP_ENDED after reconnect with valid new-generation data", async () => {
    const startMonotonic = performance.now();
    const advancingNow = () => FIXED_NOW + Math.floor(performance.now() - startMonotonic);

    const gammaServer = createServer((_request, response) => {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify(validRawEvent()));
    });
    const gammaPort = await listen(gammaServer);
    cleanups.push(async () => closeServer(gammaServer));

    let clobConnections = 0;
    const clobServer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    await new Promise<void>((resolve) => clobServer.once("listening", resolve));
    const clobPort = (clobServer.address() as AddressInfo).port;
    cleanups.push(async () => closeWebSocketServer(clobServer));
    clobServer.on("connection", (socket) => {
      clobConnections += 1;
      const generation = clobConnections;
      if (generation === 1) {
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
              timestamp: String(advancingNow()),
            }),
          );
          socket.send(
            JSON.stringify({
              event_type: "book",
              asset_id: "222222",
              market: "0xcondition",
              bids: [{ price: ".47", size: "20" }],
              asks: [{ price: ".53", size: "22" }],
              timestamp: String(advancingNow()),
            }),
          );
          setTimeout(() => socket.close(1011, "test close"), 30);
        });
      } else {
        socket.on("message", (data) => {
          const text = messageText(data);
          if (!text.startsWith("{")) return;
          socket.send(
            JSON.stringify({
              event_type: "book",
              asset_id: "111111",
              market: "0xcondition",
              bids: [{ price: ".49", size: "30" }],
              asks: [{ price: ".51", size: "25" }],
              timestamp: String(advancingNow()),
            }),
          );
          socket.send(
            JSON.stringify({
              event_type: "book",
              asset_id: "222222",
              market: "0xcondition",
              bids: [{ price: ".46", size: "20" }],
              asks: [{ price: ".54", size: "22" }],
              timestamp: String(advancingNow()),
            }),
          );
        });
      }
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
            timestamp: advancingNow(),
            payload: { symbol: "btc/usd", timestamp: advancingNow(), value: 67234.5 },
          }),
        );
      });
    });

    const outputBase = await mkdtemp(path.join(tmpdir(), "polymarket-stale-gap-test-"));
    cleanups.push(async () => rm(outputBase, { recursive: true }));
    const controller = new AbortController();
    const abortTimer = setTimeout(() => controller.abort("test complete"), 1_500);

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
        now: advancingNow,
        runId: "stale-gap-test-run",
        reconnectBaseMs: 10,
        reconnectMaximumMs: 20,
        clobStaleAfterMs: 5_000,
        referenceStaleAfterMs: 5_000,
        random: () => 0.5,
      },
    );
    clearTimeout(abortTimer);

    const events = (await readFile(path.join(result.runDirectory, "events.jsonl"), "utf8"))
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line) as RawEventEnvelope);

    const clobGapStarted = events.filter(
      (e) =>
        e.eventType === "DATA_GAP_STARTED" &&
        (e.payload as Record<string, unknown>).feed === "clob-market",
    );
    const clobGapEnded = events.filter(
      (e) =>
        e.eventType === "DATA_GAP_ENDED" &&
        (e.payload as Record<string, unknown>).feed === "clob-market",
    );

    expect(clobGapStarted.length).toBeGreaterThanOrEqual(1);
    expect(clobGapEnded.length).toBe(1);
    const gapEndedPayload = clobGapEnded[0]?.payload as Record<string, unknown>;
    expect(typeof gapEndedPayload.gapDurationMs).toBe("number");
    expect(gapEndedPayload.gapDurationMs as number).toBeGreaterThanOrEqual(0);
  });

  it("ignores old-generation valid data for gap-ending and freshness", async () => {
    const startMonotonic = performance.now();
    const advancingNow = () => FIXED_NOW + Math.floor(performance.now() - startMonotonic);

    const gammaServer = createServer((_request, response) => {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify(validRawEvent()));
    });
    const gammaPort = await listen(gammaServer);
    cleanups.push(async () => closeServer(gammaServer));

    let clobConnections = 0;
    const clobServer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    await new Promise<void>((resolve) => clobServer.once("listening", resolve));
    const clobPort = (clobServer.address() as AddressInfo).port;
    cleanups.push(async () => closeWebSocketServer(clobServer));
    clobServer.on("connection", (socket) => {
      clobConnections += 1;
      const generation = clobConnections;
      if (generation === 1) {
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
              timestamp: String(advancingNow()),
            }),
          );
          socket.send(
            JSON.stringify({
              event_type: "book",
              asset_id: "222222",
              market: "0xcondition",
              bids: [{ price: ".47", size: "20" }],
              asks: [{ price: ".53", size: "22" }],
              timestamp: String(advancingNow()),
            }),
          );
          setTimeout(() => socket.close(1011, "test close"), 30);
        });
      } else {
        socket.on("message", () => {
          // Second connection: accept subscription but never send data
        });
      }
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
            timestamp: advancingNow(),
            payload: { symbol: "btc/usd", timestamp: advancingNow(), value: 67234.5 },
          }),
        );
      });
    });

    const outputBase = await mkdtemp(path.join(tmpdir(), "polymarket-old-gen-test-"));
    cleanups.push(async () => rm(outputBase, { recursive: true }));
    const controller = new AbortController();
    const abortTimer = setTimeout(() => controller.abort("test complete"), 1_500);

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
        now: advancingNow,
        runId: "old-gen-test-run",
        reconnectBaseMs: 10,
        reconnectMaximumMs: 20,
        clobStaleAfterMs: 5_000,
        referenceStaleAfterMs: 5_000,
        random: () => 0.5,
      },
    );
    clearTimeout(abortTimer);

    const events = (await readFile(path.join(result.runDirectory, "events.jsonl"), "utf8"))
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line) as RawEventEnvelope);

    const clobGapEnded = events.filter(
      (e) =>
        e.eventType === "DATA_GAP_ENDED" &&
        (e.payload as Record<string, unknown>).feed === "clob-market",
    );
    expect(clobGapEnded.length).toBe(0);

    const health = JSON.parse(
      await readFile(path.join(result.runDirectory, "health.json"), "utf8"),
    ) as { openGapsByFeed: Record<string, unknown>; usableForReplay: boolean };
    expect(Object.keys(health.openGapsByFeed)).toContain("clob-market");
    expect(health.usableForReplay).toBe(false);
  });
});
