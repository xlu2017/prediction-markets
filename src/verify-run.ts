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
const minReferenceObservations = Number(opts.minReferenceObservations ?? 5);

async function verifyRun(directory: string): Promise<void> {
  const manifestPath = path.join(directory, "manifest.json");
  const healthPath = path.join(directory, "health.json");
  const eventsPath = path.join(directory, "events.jsonl");

  let manifest: RunManifest;
  let health: HealthSummary;
  try {
    manifest = JSON.parse(await readFile(manifestPath, "utf8")) as RunManifest;
  } catch {
    throw new Error(`Cannot read manifest at ${manifestPath}`);
  }
  try {
    health = JSON.parse(await readFile(healthPath, "utf8")) as HealthSummary;
  } catch {
    throw new Error(`Cannot read health at ${healthPath}`);
  }

  const errors: string[] = [];
  const manifestSchemaVersion: unknown = (manifest as { schemaVersion: unknown }).schemaVersion;
  const healthSchemaVersion: unknown = (health as { schemaVersion: unknown }).schemaVersion;

  if (manifestSchemaVersion !== 2) {
    errors.push(`manifest schemaVersion is ${String(manifestSchemaVersion)}, expected 2`);
  }
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

  if (healthSchemaVersion !== 2) {
    errors.push(`health schemaVersion is ${String(healthSchemaVersion)}, expected 2`);
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
        const parsed = JSON.parse(line) as RawEventEnvelope;
        events.push(parsed);
      } catch {
        errors.push(`events.jsonl line ${i + 1} is not valid JSON`);
        parseErrors += 1;
      }
    }
    if (parseErrors === 0) {
      if (events.length !== manifest.finalEventCount) {
        errors.push(
          `events.jsonl line count ${events.length} does not match manifest.finalEventCount ${manifest.finalEventCount}`,
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
      const referenceObs = new Set<string | number>();
      for (const event of events) {
        if (event.eventType === "REFERENCE_PRICE" && !event.isDuplicate) {
          if (event.sourceTimestamp !== null) referenceObs.add(event.sourceTimestamp);
        }
      }
      if (referenceObs.size < minReferenceObservations) {
        errors.push(
          `unique Chainlink reference observations ${referenceObs.size} < minimum ${minReferenceObservations}`,
        );
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
  console.error(
    JSON.stringify({
      verified: false,
      runDirectory,
      error: error instanceof Error ? error.message : String(error),
    }),
  );
  process.exitCode = 1;
}
