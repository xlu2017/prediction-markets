#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import path from "node:path";

import { Command } from "commander";

import type { HealthSummary, RunManifest } from "./types.js";

const program = new Command()
  .name("market:verify-run")
  .description("Verify that a recorded run is usable for deterministic replay")
  .argument("<runDirectory>", "path to the run directory containing manifest.json and health.json");

program.parse();
const [runDirectory] = program.args;

async function verifyRun(directory: string): Promise<void> {
  const manifestPath = path.join(directory, "manifest.json");
  const healthPath = path.join(directory, "health.json");

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
  if (health.qualityFailures.length > 0) {
    const codes = health.qualityFailures.map((f) => f.code).join(", ");
    errors.push(`qualityFailures is non-empty: ${codes}`);
  }
  for (const [tokenId, book] of Object.entries(health.finalBookHealth)) {
    if (book.state !== "SYNCHRONIZED") {
      errors.push(`book for token ${tokenId} state is ${book.state}, expected SYNCHRONIZED`);
    }
    if (!book.healthy) {
      errors.push(`book for token ${tokenId} is not healthy`);
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
