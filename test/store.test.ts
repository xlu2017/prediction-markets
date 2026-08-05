import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { EventSequencer } from "../src/envelope.js";
import { createManifest, RunStore } from "../src/store.js";
import type { HealthSummary } from "../src/types.js";
import { validMarket } from "./helpers.js";

const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories.splice(0).map(async (directory) => rm(directory, { recursive: true })),
  );
});

describe("run storage", () => {
  it("writes valid JSONL and atomically finalizes manifest and health", async () => {
    const outputBase = await mkdtemp(path.join(tmpdir(), "polymarket-recorder-test-"));
    temporaryDirectories.push(outputBase);
    const market = validMarket();
    const runId = "run-store-test";
    const store = await RunStore.initialize(
      outputBase,
      createManifest({
        runId,
        startedAt: "2026-08-05T06:02:00.000Z",
        market,
        gitCommit: null,
        commandArguments: { durationSeconds: 1 },
        clobStaleAfterMs: 15_000,
        referenceStaleAfterMs: 10_000,
      }),
    );
    const sequencer = new EventSequencer(runId);
    await store.append(
      sequencer.next({
        source: "POLYMARKET_CLOB",
        eventType: "book",
        marketId: market.marketId,
        conditionId: market.conditionId,
        receivedAt: 1,
        processedAt: 2,
        payload: { bids: [{ price: ".48", size: "30" }] },
      }),
    );
    const health: HealthSummary = {
      schemaVersion: 2,
      runId,
      startedAt: "2026-08-05T06:02:00.000Z",
      endedAt: "2026-08-05T06:02:01.000Z",
      totalEventsReceived: 1,
      eventCountsBySource: { POLYMARKET_CLOB: 1 },
      eventCountsByType: { "POLYMARKET_CLOB:book": 1 },
      malformedEventCount: 0,
      duplicateEventCount: 0,
      reconnectCount: 0,
      maximumSourceToReceiveLatencyMs: null,
      maximumReceiveToProcessLatencyMs: 1,
      staleFeedIncidents: 0,
      bothTokenStreamsObserved: false,
      referencePriceStreamObserved: false,
      finalBookHealth: {},
      openGapsByFeed: {},
      captureStatus: "PARTIAL",
      usableForReplay: false,
      qualityFailures: [
        {
          code: "MISSING_UP_SNAPSHOT",
          message: "Missing token snapshot: 111111",
          tokenId: "111111",
        },
        {
          code: "MISSING_DOWN_SNAPSHOT",
          message: "Missing token snapshot: 222222",
          tokenId: "222222",
        },
        { code: "REFERENCE_STREAM_MISSING", message: "Chainlink reference stream not observed" },
      ],
      clobStaleAfterMs: 15_000,
      referenceStaleAfterMs: 10_000,
      lastReferenceReceivedAt: null,
      lastReferenceConnectionGeneration: null,
      finalReferenceAgeMs: null,
      referenceFresh: false,
    };
    await store.finalize({
      status: "COMPLETED",
      endedAt: "2026-08-05T06:02:01.000Z",
      finalEventCount: 1,
      health,
      captureStatus: "PARTIAL",
      usableForReplay: false,
      qualityFailures: [
        {
          code: "MISSING_UP_SNAPSHOT",
          message: "Missing token snapshot: 111111",
          tokenId: "111111",
        },
        {
          code: "MISSING_DOWN_SNAPSHOT",
          message: "Missing token snapshot: 222222",
          tokenId: "222222",
        },
        { code: "REFERENCE_STREAM_MISSING", message: "Chainlink reference stream not observed" },
      ],
    });

    const eventLines = (await readFile(path.join(store.directory, "events.jsonl"), "utf8"))
      .trim()
      .split("\n");
    expect(eventLines).toHaveLength(1);
    expect(() => {
      JSON.parse(eventLines[0] ?? "");
    }).not.toThrow();
    const manifest = JSON.parse(
      await readFile(path.join(store.directory, "manifest.json"), "utf8"),
    ) as Record<string, unknown>;
    expect(manifest).toMatchObject({ completionStatus: "COMPLETED", finalEventCount: 1 });
  });
});
