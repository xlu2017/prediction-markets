#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import path from "node:path";

import { Command } from "commander";

import type { HealthSummary, RawEventEnvelope, RunManifest } from "./types.js";

const program = new Command()
  .name("market:verify-run")
  .description("Verify that a recorded run is usable for deterministic replay")
  .argument("<runDirectory>", "path to the run directory containing manifest.json and health.json")
  .option("--min-reference-observations <number>", "minimum unique Chainlink observations", "5");

program.parse();
const [runDirectory] = program.args;
const opts = program.opts();
const rawMin = Number(opts.minReferenceObservations ?? 5);
if (!Number.isSafeInteger(rawMin) || rawMin < 0) {
  console.error(
    JSON.stringify({
      verified: false,
      runDirectory,
      error: `--min-reference-observations must be a non-negative safe integer, got ${String(opts.minReferenceObservations)}`,
    }),
  );
  process.exitCode = 1;
  throw new Error("invalid argument");
}
const minReferenceObservations = rawMin;

function assertString(value: unknown, label: string): string {
  if (typeof value !== "string") throw new Error(`${label} is not a string`);
  return value;
}

function assertNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`${label} is not a finite number`);
  }
  return value;
}

function assertBoolean(value: unknown, label: string): boolean {
  if (typeof value !== "boolean") throw new Error(`${label} is not a boolean`);
  return value;
}

function runtimeValidateManifest(raw: unknown): RunManifest {
  const record = raw as Record<string, unknown>;
  if (typeof record !== "object") throw new Error("manifest is not an object");
  if (record.schemaVersion !== 2)
    throw new Error(`manifest schemaVersion is ${String(record.schemaVersion)}, expected 2`);
  assertString(record.runId, "manifest.runId");
  assertString(record.startedAt, "manifest.startedAt");
  if (
    record.completionStatus !== "COMPLETED" &&
    record.completionStatus !== "FAILED" &&
    record.completionStatus !== "RUNNING"
  ) {
    throw new Error(`manifest.completionStatus is invalid: ${String(record.completionStatus)}`);
  }
  if (
    record.captureStatus !== "COMPLETE" &&
    record.captureStatus !== "PARTIAL" &&
    record.captureStatus !== "UNUSABLE"
  ) {
    throw new Error(`manifest.captureStatus is invalid: ${String(record.captureStatus)}`);
  }
  assertBoolean(record.usableForReplay, "manifest.usableForReplay");
  if (record.finalEventCount !== null && typeof record.finalEventCount !== "number") {
    throw new Error("manifest.finalEventCount is not a number or null");
  }
  const market = record.market as Record<string, unknown>;
  if (typeof market !== "object") throw new Error("manifest.market is not an object");
  assertString(market.upTokenId, "manifest.market.upTokenId");
  assertString(market.downTokenId, "manifest.market.downTokenId");
  assertString(market.marketId, "manifest.market.marketId");
  assertString(market.conditionId, "manifest.market.conditionId");
  return raw as RunManifest;
}

function runtimeValidateHealth(raw: unknown): HealthSummary {
  const record = raw as Record<string, unknown>;
  if (typeof record !== "object") throw new Error("health is not an object");
  if (record.schemaVersion !== 2)
    throw new Error(`health schemaVersion is ${String(record.schemaVersion)}, expected 2`);
  assertString(record.runId, "health.runId");
  assertNumber(record.totalEventsReceived, "health.totalEventsReceived");
  assertBoolean(record.usableForReplay, "health.usableForReplay");
  assertBoolean(record.referenceFresh, "health.referenceFresh");
  assertNumber(record.referenceClockSkewMs, "health.referenceClockSkewMs");
  return raw as HealthSummary;
}

function runtimeValidateEvent(raw: unknown, index: number): RawEventEnvelope {
  const record = raw as Record<string, unknown>;
  if (typeof record !== "object") {
    throw new Error(`events.jsonl line ${index + 1} is not an object`);
  }
  if (record.schemaVersion !== 2) {
    throw new Error(
      `events.jsonl line ${index + 1}: schemaVersion is ${String(record.schemaVersion)}, expected 2`,
    );
  }
  if (typeof record.sequence !== "number" || !Number.isSafeInteger(record.sequence)) {
    throw new Error(`events.jsonl line ${index + 1}: sequence is not a safe integer`);
  }
  if (typeof record.runId !== "string") {
    throw new Error(`events.jsonl line ${index + 1}: runId is not a string`);
  }
  if (typeof record.eventType !== "string") {
    throw new Error(`events.jsonl line ${index + 1}: eventType is not a string`);
  }
  return raw as RawEventEnvelope;
}

async function verifyRun(directory: string): Promise<void> {
  const manifestPath = path.join(directory, "manifest.json");
  const healthPath = path.join(directory, "health.json");
  const eventsPath = path.join(directory, "events.jsonl");

  let manifestRaw: unknown;
  let healthRaw: unknown;
  try {
    manifestRaw = JSON.parse(await readFile(manifestPath, "utf8"));
  } catch {
    throw new Error(`Cannot read manifest at ${manifestPath}`);
  }
  try {
    healthRaw = JSON.parse(await readFile(healthPath, "utf8"));
  } catch {
    throw new Error(`Cannot read health at ${healthPath}`);
  }

  let manifest: RunManifest;
  let health: HealthSummary;
  try {
    manifest = runtimeValidateManifest(manifestRaw);
  } catch (error) {
    throw new Error(
      `Manifest validation failed: ${error instanceof Error ? error.message : String(error)}`,
      { cause: error },
    );
  }
  try {
    health = runtimeValidateHealth(healthRaw);
  } catch (error) {
    throw new Error(
      `Health validation failed: ${error instanceof Error ? error.message : String(error)}`,
      { cause: error },
    );
  }

  const errors: string[] = [];

  if (manifest.completionStatus !== "COMPLETED") {
    errors.push(`completionStatus is ${manifest.completionStatus}, expected COMPLETED`);
  }
  if (manifest.captureStatus !== "COMPLETE") {
    errors.push(`captureStatus is ${manifest.captureStatus}, expected COMPLETE`);
  }
  if (!manifest.usableForReplay) {
    errors.push("usableForReplay is false in manifest");
  }
  if (manifest.failureReason !== null) {
    errors.push(`failureReason is non-null: ${manifest.failureReason}`);
  }
  if (manifest.finalEventCount === null || manifest.finalEventCount <= 0) {
    errors.push(`finalEventCount is ${manifest.finalEventCount}, expected a positive number`);
  }

  if (health.runId !== manifest.runId) {
    errors.push(`health.runId ${health.runId} does not match manifest.runId ${manifest.runId}`);
  }
  if (health.captureStatus !== "COMPLETE") {
    errors.push(`health captureStatus is ${health.captureStatus}, expected COMPLETE`);
  }
  if (!health.usableForReplay) {
    errors.push("usableForReplay is false in health");
  }
  if (!health.bothTokenStreamsObserved) {
    errors.push("bothTokenStreamsObserved is false");
  }
  if (!health.referencePriceStreamObserved) {
    errors.push("referencePriceStreamObserved is false");
  }
  if (!health.referenceFresh) {
    errors.push("referenceFresh is false");
  }
  if (health.malformedEventCount > 0) {
    errors.push(`malformedEventCount is ${health.malformedEventCount}, expected 0`);
  }
  if (health.staleFeedIncidents > 0) {
    errors.push(`staleFeedIncidents is ${health.staleFeedIncidents}, expected 0`);
  }
  if (Object.keys(health.openGapsByFeed).length > 0) {
    errors.push(`openGapsByFeed is non-empty: ${Object.keys(health.openGapsByFeed).join(", ")}`);
  }
  if (health.dataGapCount > 0) {
    errors.push(`dataGapCount is ${health.dataGapCount}, expected 0`);
  }
  if (health.closedGapCount > 0) {
    errors.push(`closedGapCount is ${health.closedGapCount}, expected 0`);
  }
  if (health.qualityFailures.length > 0) {
    const codes = health.qualityFailures.map((f) => f.code).join(", ");
    errors.push(`qualityFailures is non-empty: ${codes}`);
  }
  const expectedTokenIds = new Set([manifest.market.upTokenId, manifest.market.downTokenId]);
  for (const [tokenId, book] of Object.entries(health.finalBookHealth)) {
    if (book.state !== "SYNCHRONIZED") {
      errors.push(`book for token ${tokenId} state is ${book.state}, expected SYNCHRONIZED`);
    }
    if (!book.healthy) {
      errors.push(`book for token ${tokenId} is not healthy`);
    }
    if (book.integrity === "CROSSED") {
      errors.push(
        `book for token ${tokenId} is CROSSED (bid ${book.bestBid} > ask ${book.bestAsk})`,
      );
    }
    if (book.crossedIncidentCount > 0) {
      errors.push(`book for token ${tokenId} had ${book.crossedIncidentCount} crossed incident(s)`);
    }
    if (!expectedTokenIds.has(tokenId)) {
      errors.push(`book health has unexpected token ID ${tokenId}`);
    }
  }
  for (const expected of expectedTokenIds) {
    if (!(expected in health.finalBookHealth)) {
      errors.push(`missing book health for expected token ID ${expected}`);
    }
  }

  // Validate events.jsonl
  let eventsText: string;
  try {
    eventsText = await readFile(eventsPath, "utf8");
  } catch {
    throw new Error(`Cannot read events at ${eventsPath}`);
  }
  const lines = eventsText.trim().split("\n");
  if (lines.length === 0 || (lines.length === 1 && lines[0] === "")) {
    errors.push("events.jsonl is empty");
  } else {
    const events: RawEventEnvelope[] = [];
    let parseErrors = 0;
    for (let i = 0; i < lines.length; i += 1) {
      const line = lines[i];
      if (line === undefined || line === "") {
        errors.push(`events.jsonl line ${i + 1} is empty`);
        parseErrors += 1;
        continue;
      }
      try {
        const parsed = JSON.parse(line) as unknown;
        events.push(runtimeValidateEvent(parsed, i));
      } catch (error) {
        errors.push(
          `events.jsonl line ${i + 1}: ${error instanceof Error ? error.message : "parse error"}`,
        );
        parseErrors += 1;
      }
    }
    if (parseErrors === 0) {
      if (events.length !== manifest.finalEventCount) {
        errors.push(
          `events.jsonl line count ${events.length} does not match manifest.finalEventCount ${manifest.finalEventCount}`,
        );
      }
      if (events.length !== health.totalEventsReceived) {
        errors.push(
          `events.jsonl line count ${events.length} does not match health.totalEventsReceived ${health.totalEventsReceived}`,
        );
      }
      let expectedSeq = 1;
      for (let i = 0; i < events.length; i += 1) {
        const event = events[i];
        if (event === undefined) continue;
        if (event.sequence !== expectedSeq) {
          errors.push(
            `events.jsonl line ${i + 1}: sequence is ${event.sequence}, expected ${expectedSeq}`,
          );
          break;
        }
        expectedSeq += 1;
      }
      if (events.length > 0) {
        const lastEvent = events[events.length - 1];
        if (lastEvent !== undefined && lastEvent.sequence !== events.length) {
          errors.push(
            `final sequence ${lastEvent.sequence} does not match line count ${events.length}`,
          );
        }
        for (const event of events) {
          if (event.runId !== manifest.runId) {
            errors.push(
              `event sequence ${event.sequence} runId ${event.runId} does not match manifest runId ${manifest.runId}`,
            );
            break;
          }
          if (event.marketId !== manifest.market.marketId) {
            errors.push(
              `event sequence ${event.sequence} marketId ${event.marketId} does not match manifest marketId ${manifest.market.marketId}`,
            );
            break;
          }
          if (event.conditionId !== manifest.market.conditionId) {
            errors.push(
              `event sequence ${event.sequence} conditionId ${event.conditionId} does not match manifest conditionId ${manifest.market.conditionId}`,
            );
            break;
          }
        }
      }
      // Recompute reference freshness from events rather than trusting health.referenceFresh
      const referenceObs = new Set<number>();
      let lastRefReceivedAt: number | null = null;
      let lastRefSourceTimestamp: number | null = null;
      for (const event of events) {
        if (event.eventType === "REFERENCE_PRICE" && !event.isDuplicate) {
          if (event.sourceTimestamp !== null) referenceObs.add(event.sourceTimestamp);
          lastRefReceivedAt = event.receivedAt;
          if (event.sourceTimestamp !== null) lastRefSourceTimestamp = event.sourceTimestamp;
        }
      }
      if (referenceObs.size < minReferenceObservations) {
        errors.push(
          `unique Chainlink reference observations ${referenceObs.size} < minimum ${minReferenceObservations}`,
        );
      }
      // Recompute freshness using the endedAt from health
      const endedAtMs = health.endedAt !== null ? Date.parse(health.endedAt) : null;
      if (endedAtMs !== null && lastRefReceivedAt !== null && lastRefSourceTimestamp !== null) {
        const receiptAge = endedAtMs - lastRefReceivedAt;
        const sourceAge = endedAtMs - lastRefSourceTimestamp;
        const staleAfter = health.referenceStaleAfterMs;
        const skewTolerance = health.referenceClockSkewMs;
        if (receiptAge < 0 || receiptAge > staleAfter) {
          errors.push(
            `recomputed reference receipt age ${receiptAge}ms is outside [0, ${staleAfter}ms]`,
          );
        }
        if (sourceAge < -skewTolerance || sourceAge > staleAfter) {
          errors.push(
            `recomputed reference source age ${sourceAge}ms is outside [-${skewTolerance}ms, ${staleAfter}ms]`,
          );
        }
      }
    }
  }

  if (errors.length > 0) {
    const message = errors.map((e) => `  - ${e}`).join("\n");
    throw new Error(`Run verification failed with ${errors.length} error(s):\n${message}`);
  }

  console.log(
    JSON.stringify({
      verified: true,
      runDirectory: directory,
      runId: manifest.runId,
      captureStatus: manifest.captureStatus,
      usableForReplay: manifest.usableForReplay,
      finalEventCount: manifest.finalEventCount,
    }),
  );
}

try {
  if (runDirectory === undefined) throw new Error("runDirectory argument is required");
  await verifyRun(runDirectory);
} catch (error) {
  if (!process.exitCode) {
    console.error(
      JSON.stringify({
        verified: false,
        runDirectory,
        error: error instanceof Error ? error.message : String(error),
      }),
    );
    process.exitCode = 1;
  }
}
