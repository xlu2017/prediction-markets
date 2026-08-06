import { mkdtemp, mkdir, writeFile, rm, copyFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { verifyRun } from "../src/verify-run.js";

const FIXTURE_DIR = path.resolve(__dirname, "..", "fixtures", "sample-run");
const cleanups: (() => Promise<void>)[] = [];

afterEach(async () => {
  await Promise.allSettled(cleanups.splice(0).map(async (cleanup) => cleanup()));
});

async function createTempRunDir(): Promise<string> {
  const dir = await mkdtemp(path.join(tmpdir(), "verify-corrupt-"));
  cleanups.push(async () => {
    await rm(dir, { recursive: true, force: true });
  });
  return dir;
}

async function copyFixture(dest: string): Promise<void> {
  await mkdir(dest, { recursive: true });
  await copyFile(path.join(FIXTURE_DIR, "manifest.json"), path.join(dest, "manifest.json"));
  await copyFile(path.join(FIXTURE_DIR, "health.json"), path.join(dest, "health.json"));
  await copyFile(path.join(FIXTURE_DIR, "events.jsonl"), path.join(dest, "events.jsonl"));
}

async function readJson(filePath: string): Promise<Record<string, unknown>> {
  const text = await import("node:fs/promises").then((m) => m.readFile(filePath, "utf8"));
  return JSON.parse(text) as Record<string, unknown>;
}

async function writeJson(filePath: string, data: unknown): Promise<void> {
  await writeFile(filePath, JSON.stringify(data, null, 2));
}

async function readLines(filePath: string): Promise<string[]> {
  const text = await import("node:fs/promises").then((m) => m.readFile(filePath, "utf8"));
  return text.trim().split("\n");
}

async function writeLines(filePath: string, lines: string[]): Promise<void> {
  await writeFile(filePath, lines.join("\n") + "\n");
}

function requireLine(lines: string[], index: number): string {
  const line = lines[index];
  if (line === undefined) throw new Error(`fixture missing line ${index}`);
  return line;
}

describe("verify-run corrupted artifact detection", () => {
  it("passes for the valid fixture", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    await expect(verifyRun(dir, 1)).resolves.toBeUndefined();
  });

  it("fails when manifest.endedAt is null on a completed run", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const manifest = await readJson(path.join(dir, "manifest.json"));
    manifest.endedAt = null;
    await writeJson(path.join(dir, "manifest.json"), manifest);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/endedAt.*null/);
  });

  it("fails when manifest.endedAt is invalid", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const manifest = await readJson(path.join(dir, "manifest.json"));
    manifest.endedAt = "not-a-timestamp";
    await writeJson(path.join(dir, "manifest.json"), manifest);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/endedAt.*timestamp/);
  });

  it("fails when health.endedAt is null on a completed run", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const health = await readJson(path.join(dir, "health.json"));
    health.endedAt = null;
    await writeJson(path.join(dir, "health.json"), health);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/endedAt.*null/);
  });

  it("fails when manifest and health endedAt mismatch", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const manifest = await readJson(path.join(dir, "manifest.json"));
    manifest.endedAt = "2026-08-05T06:02:02.000Z";
    await writeJson(path.join(dir, "manifest.json"), manifest);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/endedAt.*does not match/);
  });

  it("fails when health.endedAt is before manifest.startedAt", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const health = await readJson(path.join(dir, "health.json"));
    health.endedAt = "2026-08-05T06:01:59.000Z";
    await writeJson(path.join(dir, "health.json"), health);
    const manifest = await readJson(path.join(dir, "manifest.json"));
    manifest.endedAt = "2026-08-05T06:01:59.000Z";
    await writeJson(path.join(dir, "manifest.json"), manifest);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/before.*startedAt/);
  });

  it("fails when gap counters are negative", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const health = await readJson(path.join(dir, "health.json"));
    health.dataGapCount = -1;
    await writeJson(path.join(dir, "health.json"), health);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/dataGapCount.*non-negative/);
  });

  it("fails when generation is fractional", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const health = await readJson(path.join(dir, "health.json"));
    health.lastReferenceConnectionGeneration = 1.5;
    await writeJson(path.join(dir, "health.json"), health);
    await expect(verifyRun(dir, 1)).rejects.toThrow(
      /lastReferenceConnectionGeneration.*non-negative safe integer/,
    );
  });

  it("fails when qualityFailures is malformed", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const health = await readJson(path.join(dir, "health.json"));
    health.qualityFailures = [{ code: "INVALID_CODE", message: "bad" }];
    await writeJson(path.join(dir, "health.json"), health);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/qualityFailures.*INVALID_CODE/);
  });

  it("fails when book state is invalid", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const health = await readJson(path.join(dir, "health.json"));
    const bookHealth = health.finalBookHealth as Record<string, Record<string, unknown>>;
    const upBook = bookHealth["111111"];
    if (upBook === undefined) throw new Error("fixture missing book 111111");
    upBook.state = "BAD_STATE";
    await writeJson(path.join(dir, "health.json"), health);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/state.*BAD_STATE/);
  });

  it("fails when book integrity is invalid", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const health = await readJson(path.join(dir, "health.json"));
    const bookHealth = health.finalBookHealth as Record<string, Record<string, unknown>>;
    const upBook = bookHealth["111111"];
    if (upBook === undefined) throw new Error("fixture missing book 111111");
    upBook.integrity = "BAD_INTEGRITY";
    await writeJson(path.join(dir, "health.json"), health);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/integrity.*BAD_INTEGRITY/);
  });

  it("fails when event source is invalid", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const first = JSON.parse(requireLine(lines, 0)) as Record<string, unknown>;
    first.source = "BAD_SOURCE";
    lines[0] = JSON.stringify(first);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/source.*BAD_SOURCE/);
  });

  it("fails when event dedupeState is invalid", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const second = JSON.parse(requireLine(lines, 1)) as Record<string, unknown>;
    second.dedupeState = "BAD_STATE";
    lines[1] = JSON.stringify(second);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/dedupeState.*BAD_STATE/);
  });

  it("fails when event identity fields are missing", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const second = JSON.parse(requireLine(lines, 1)) as Record<string, unknown>;
    delete second.runId;
    lines[1] = JSON.stringify(second);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/runId.*string/);
  });

  it("fails when token arrays are malformed", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const second = JSON.parse(requireLine(lines, 1)) as Record<string, unknown>;
    second.observedTokenIds = "not-an-array";
    lines[1] = JSON.stringify(second);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/observedTokenIds.*array/);
  });

  it("fails when anomaly counters are mismatched", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const health = await readJson(path.join(dir, "health.json"));
    health.referenceTimestampMissingCount = 5;
    await writeJson(path.join(dir, "health.json"), health);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/missing count.*does not match/);
  });

  it("fails when manifest and health thresholds mismatch", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const manifest = await readJson(path.join(dir, "manifest.json"));
    manifest.clobStaleAfterMs = 99_999;
    await writeJson(path.join(dir, "manifest.json"), manifest);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/clobStaleAfterMs.*does not match/);
  });

  it("fails when manifest and health captureStatus mismatch", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const manifest = await readJson(path.join(dir, "manifest.json"));
    manifest.captureStatus = "PARTIAL";
    await writeJson(path.join(dir, "manifest.json"), manifest);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/captureStatus.*PARTIAL/);
  });

  it("fails when manifest and health usableForReplay mismatch", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const manifest = await readJson(path.join(dir, "manifest.json"));
    manifest.usableForReplay = false;
    await writeJson(path.join(dir, "manifest.json"), manifest);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/usableForReplay.*false/);
  });

  it("fails when event count does not match finalEventCount", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const manifest = await readJson(path.join(dir, "manifest.json"));
    manifest.finalEventCount = 99;
    await writeJson(path.join(dir, "manifest.json"), manifest);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/line count.*does not match.*finalEventCount/);
  });

  it("fails when recomputed malformed count does not match health", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const health = await readJson(path.join(dir, "health.json"));
    health.malformedEventCount = 5;
    await writeJson(path.join(dir, "health.json"), health);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/malformed count.*does not match/);
  });

  it("fails when all reference events are RECEIVED_NOT_APPLIED", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const refLine = requireLine(lines, 3);
    const ref = JSON.parse(refLine) as Record<string, unknown>;
    ref.dedupeState = "RECEIVED_NOT_APPLIED";
    ref.dedupeReason = "frame from inactive connection generation";
    lines[3] = JSON.stringify(ref);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(
      /unique Chainlink reference observations.*minimum/,
    );
  });

  it("fails when all book snapshots are RECEIVED_NOT_APPLIED", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const book1 = JSON.parse(requireLine(lines, 1)) as Record<string, unknown>;
    book1.dedupeState = "RECEIVED_NOT_APPLIED";
    book1.dedupeReason = "frame from inactive connection generation";
    lines[1] = JSON.stringify(book1);
    const book2 = JSON.parse(requireLine(lines, 2)) as Record<string, unknown>;
    book2.dedupeState = "RECEIVED_NOT_APPLIED";
    book2.dedupeReason = "frame from inactive connection generation";
    lines[2] = JSON.stringify(book2);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    const health = await readJson(path.join(dir, "health.json"));
    const bookHealth = health.finalBookHealth as Record<string, Record<string, unknown>>;
    const upBook = bookHealth["111111"];
    if (upBook === undefined) throw new Error("fixture missing book 111111");
    upBook.state = "UNINITIALIZED";
    const downBook = bookHealth["222222"];
    if (downBook === undefined) throw new Error("fixture missing book 222222");
    downBook.state = "UNINITIALIZED";
    await writeJson(path.join(dir, "health.json"), health);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/book for token.*SYNCHRONIZED/);
  });

  it("fails when isDuplicate contradicts dedupeState (UNIQUE_APPLIED with isDuplicate=true)", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const first = JSON.parse(requireLine(lines, 0)) as Record<string, unknown>;
    first.isDuplicate = true;
    first.dedupeState = "UNIQUE_APPLIED";
    lines[0] = JSON.stringify(first);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/UNIQUE_APPLIED requires isDuplicate=false/);
  });

  it("fails when DUPLICATE_APPLIED has null duplicateOfSequence", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const first = JSON.parse(requireLine(lines, 0)) as Record<string, unknown>;
    first.isDuplicate = true;
    first.dedupeState = "DUPLICATE_APPLIED";
    first.duplicateOfSequence = null;
    lines[0] = JSON.stringify(first);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(
      /DUPLICATE_APPLIED requires duplicateOfSequence to be a positive prior sequence/,
    );
  });

  it("fails when duplicateOfSequence points forward", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const first = JSON.parse(requireLine(lines, 0)) as Record<string, unknown>;
    first.isDuplicate = true;
    first.dedupeState = "DUPLICATE_APPLIED";
    first.duplicateOfSequence = 999;
    lines[0] = JSON.stringify(first);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(
      /duplicateOfSequence.*must be less than sequence/,
    );
  });

  it("fails when duplicateOfSequence points to itself", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const first = JSON.parse(requireLine(lines, 0)) as Record<string, unknown>;
    first.isDuplicate = true;
    first.dedupeState = "DUPLICATE_APPLIED";
    first.duplicateOfSequence = 1;
    lines[0] = JSON.stringify(first);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(
      /duplicateOfSequence.*must be less than sequence/,
    );
  });

  it("fails when messageIndex >= messageCount", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const second = JSON.parse(requireLine(lines, 1)) as Record<string, unknown>;
    second.messageIndex = 5;
    second.messageCount = 1;
    lines[1] = JSON.stringify(second);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/messageIndex.*must be less than messageCount/);
  });

  it("fails when payload contains a non-finite number", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const first = JSON.parse(requireLine(lines, 0)) as Record<string, unknown>;
    first.payload = { value: "__PLACEHOLDER__" };
    const line = JSON.stringify(first).replace('"__PLACEHOLDER__"', "1e500");
    lines[0] = line;
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/payload.*non-finite number/);
  });

  it("fails when minReferenceObservations is not a positive integer", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    await expect(verifyRun(dir, 0)).rejects.toThrow(
      /minReferenceObservations must be a positive safe integer/,
    );
    await expect(verifyRun(dir, -1)).rejects.toThrow(
      /minReferenceObservations must be a positive safe integer/,
    );
    await expect(verifyRun(dir, 1.5)).rejects.toThrow(
      /minReferenceObservations must be a positive safe integer/,
    );
  });
});
