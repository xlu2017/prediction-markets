import { readFile } from "node:fs/promises";
import path from "node:path";

import { describe, expect, it } from "vitest";

import type { HealthSummary, RawEventEnvelope, RunManifest } from "../src/types.js";

const fixtureDirectory = path.resolve("fixtures/sample-run");

describe("committed offline run", () => {
  it("is self-contained, line-valid, and sequence-complete", async () => {
    const manifest = JSON.parse(
      await readFile(path.join(fixtureDirectory, "manifest.json"), "utf8"),
    ) as RunManifest;
    const health = JSON.parse(
      await readFile(path.join(fixtureDirectory, "health.json"), "utf8"),
    ) as HealthSummary;
    const events = (await readFile(path.join(fixtureDirectory, "events.jsonl"), "utf8"))
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line) as RawEventEnvelope);

    expect(manifest.completionStatus).toBe("COMPLETED");
    expect(manifest.outputFiles).toEqual([
      "manifest.json",
      "market.json",
      "events.jsonl",
      "health.json",
    ]);
    expect(events.map((event) => event.sequence)).toEqual([1, 2, 3, 4]);
    expect(manifest.finalEventCount).toBe(events.length);
    expect(health.bothTokenStreamsObserved).toBe(true);
    expect(health.referencePriceStreamObserved).toBe(true);
    const reference = events.find((event) => event.eventType === "REFERENCE_PRICE");
    expect(reference?.rawText).toContain("67234.5000000000000001");
  });
});
