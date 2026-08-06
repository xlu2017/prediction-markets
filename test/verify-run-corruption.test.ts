import { mkdtemp, mkdir, writeFile, rm, copyFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { verifyRun, runtimeValidateManifest } from "../src/verify-run.js";

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
    upBook.snapshotConnectionGeneration = null;
    upBook.lastSuccessfulUpdateTime = null;
    upBook.bestBid = null;
    upBook.bestAsk = null;
    upBook.bidDepthCount = 0;
    upBook.askDepthCount = 0;
    upBook.healthy = false;
    const downBook = bookHealth["222222"];
    if (downBook === undefined) throw new Error("fixture missing book 222222");
    downBook.state = "UNINITIALIZED";
    downBook.snapshotConnectionGeneration = null;
    downBook.lastSuccessfulUpdateTime = null;
    downBook.bestBid = null;
    downBook.bestAsk = null;
    downBook.bidDepthCount = 0;
    downBook.askDepthCount = 0;
    downBook.healthy = false;
    await writeJson(path.join(dir, "health.json"), health);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/book for token.*SYNCHRONIZED/);
  });

  it("fails when all book snapshots are RECEIVED_NOT_APPLIED but health is unchanged", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    for (let i = 0; i < lines.length; i += 1) {
      const parsed = JSON.parse(requireLine(lines, i)) as Record<string, unknown>;
      if (
        parsed.source === "POLYMARKET_CLOB" &&
        parsed.eventType === "book" &&
        parsed.dedupeState === "UNIQUE_APPLIED"
      ) {
        parsed.dedupeState = "RECEIVED_NOT_APPLIED";
        parsed.dedupeReason = "frame from inactive connection generation";
        lines[i] = JSON.stringify(parsed);
      }
    }
    await writeLines(path.join(dir, "events.jsonl"), lines);
    // health.json is left completely unchanged
    await expect(verifyRun(dir, 1)).rejects.toThrow(
      /no UNIQUE_APPLIED book snapshot for expected token/,
    );
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

  it("accepts negative maximumSourceToReceiveLatencyMs within clock-skew tolerance", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    // Null out book sourceTimestamps so only the reference event contributes to max latency
    for (let i = 0; i < 3; i += 1) {
      const parsed = JSON.parse(requireLine(lines, i)) as Record<string, unknown>;
      parsed.sourceTimestamp = null;
      lines[i] = JSON.stringify(parsed);
    }
    const ref = JSON.parse(requireLine(lines, 3)) as Record<string, unknown>;
    // receivedAt - sourceTimestamp = -1000ms (source timestamp is 1s ahead)
    const newSourceTs = (ref.receivedAt as number) + 1000;
    ref.sourceTimestamp = newSourceTs;
    ref.topLevelSourceTimestamp = newSourceTs;
    ref.payloadSourceTimestamp = newSourceTs;
    lines[3] = JSON.stringify(ref);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    const health = await readJson(path.join(dir, "health.json"));
    // Recomputed: maxSourceLatency = -1000, lastRefSourceTimestamp = newSourceTs
    health.maximumSourceToReceiveLatencyMs = -1000;
    health.lastReferenceSourceTimestamp = newSourceTs;
    health.finalReferenceSourceAgeMs = 1785909720126 - newSourceTs;
    health.referenceClockSkewMs = 5000;
    await writeJson(path.join(dir, "health.json"), health);
    const manifest = await readJson(path.join(dir, "manifest.json"));
    manifest.referenceStaleAfterMs = health.referenceStaleAfterMs;
    await writeJson(path.join(dir, "manifest.json"), manifest);
    // The verifier must accept the negative latency metric
    await expect(verifyRun(dir, 1)).resolves.toBeUndefined();
  });

  it("fails when manifest.market.referenceSymbol is not btc/usd", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const manifest = await readJson(path.join(dir, "manifest.json"));
    (manifest.market as Record<string, unknown>).referenceSymbol = "eth/usd";
    await writeJson(path.join(dir, "manifest.json"), manifest);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/referenceSymbol.*expected btc\/usd/);
  });

  it("fails when manifest.market.referenceTopic is not crypto_prices_chainlink", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const manifest = await readJson(path.join(dir, "manifest.json"));
    (manifest.market as Record<string, unknown>).referenceTopic = "wrong_topic";
    await writeJson(path.join(dir, "manifest.json"), manifest);
    await expect(verifyRun(dir, 1)).rejects.toThrow(
      /referenceTopic.*expected crypto_prices_chainlink/,
    );
  });

  it("fails when manifest.market.rawMetadata is not a JsonValue", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const manifest = await readJson(path.join(dir, "manifest.json"));
    (manifest.market as Record<string, unknown>).rawMetadata = { bad: Number.NaN };
    expect(() => runtimeValidateManifest(manifest)).toThrow(/rawMetadata/);
  });

  it("fails when manifest.market.rawGammaText is not a string", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const manifest = await readJson(path.join(dir, "manifest.json"));
    (manifest.market as Record<string, unknown>).rawGammaText = 123;
    await writeJson(path.join(dir, "manifest.json"), manifest);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/rawGammaText.*string/);
  });

  it("fails when manifest.commandArguments is not a JsonValue", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const manifest = await readJson(path.join(dir, "manifest.json"));
    manifest.commandArguments = { bad: Number.NaN };
    expect(() => runtimeValidateManifest(manifest)).toThrow(/commandArguments/);
  });

  it("fails when finalBookHealth map key does not match book.tokenId", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const health = await readJson(path.join(dir, "health.json"));
    const bookHealth = health.finalBookHealth as Record<string, Record<string, unknown>>;
    const upBook = bookHealth["111111"];
    if (upBook === undefined) throw new Error("fixture missing book 111111");
    upBook.tokenId = "999999";
    await writeJson(path.join(dir, "health.json"), health);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/map key.*does not match book.tokenId/);
  });

  it("fails when frameId/messageIndex/messageCount nullability is inconsistent", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const book = JSON.parse(requireLine(lines, 1)) as Record<string, unknown>;
    book.frameId = "frame-1";
    book.messageIndex = null;
    book.messageCount = 1;
    lines[1] = JSON.stringify(book);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(
      /frameId, messageIndex, messageCount must be all null or all non-null/,
    );
  });

  it("fails when messageCount is 0 for a framed message", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const book = JSON.parse(requireLine(lines, 1)) as Record<string, unknown>;
    book.frameId = "frame-1";
    book.messageIndex = 0;
    book.messageCount = 0;
    lines[1] = JSON.stringify(book);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/messageCount must be > 0/);
  });

  it("fails when feed-derived event has connectionGeneration <= 0", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const book = JSON.parse(requireLine(lines, 1)) as Record<string, unknown>;
    book.connectionGeneration = 0;
    lines[1] = JSON.stringify(book);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(
      /connectionGeneration must be > 0 for feed-derived events/,
    );
  });

  it("fails when processedAt < receivedAt", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const book = JSON.parse(requireLine(lines, 1)) as Record<string, unknown>;
    book.processedAt = (book.receivedAt as number) - 1;
    lines[1] = JSON.stringify(book);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/processedAt.*before receivedAt/);
  });

  it("fails when up and down snapshots have different generations but health matches both", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const book2 = JSON.parse(requireLine(lines, 2)) as Record<string, unknown>;
    book2.connectionGeneration = 2;
    lines[2] = JSON.stringify(book2);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    const health = await readJson(path.join(dir, "health.json"));
    const bookHealth = health.finalBookHealth as Record<string, Record<string, unknown>>;
    const downHealth = bookHealth["222222"];
    if (downHealth === undefined) throw new Error("fixture missing book 222222");
    downHealth.snapshotConnectionGeneration = 2;
    await writeJson(path.join(dir, "health.json"), health);
    await expect(verifyRun(dir, 1)).rejects.toThrow(
      /does not equal latest accepted CLOB generation/,
    );
  });

  it("fails when a synchronized book has null snapshotConnectionGeneration", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const health = await readJson(path.join(dir, "health.json"));
    const bookHealth = health.finalBookHealth as Record<string, Record<string, unknown>>;
    const upHealth = bookHealth["111111"];
    if (upHealth === undefined) throw new Error("fixture missing book 111111");
    upHealth.snapshotConnectionGeneration = null;
    await writeJson(path.join(dir, "health.json"), health);
    await expect(verifyRun(dir, 1)).rejects.toThrow(
      /snapshotConnectionGeneration must be > 0 for SYNCHRONIZED/,
    );
  });

  it("fails when book envelope has tokenId=null with observedTokenIds=[expected]", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const book1 = JSON.parse(requireLine(lines, 1)) as Record<string, unknown>;
    book1.tokenId = null;
    book1.observedTokenIds = ["111111"];
    lines[1] = JSON.stringify(book1);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/tokenId null is not an expected token/);
  });

  it("fails when book envelope has observedTokenIds non-null", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const book1 = JSON.parse(requireLine(lines, 1)) as Record<string, unknown>;
    book1.observedTokenIds = ["111111"];
    lines[1] = JSON.stringify(book1);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(
      /observedTokenIds must be null for a full snapshot/,
    );
  });

  it("fails when book payload.asset_id does not match tokenId", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const book1 = JSON.parse(requireLine(lines, 1)) as Record<string, unknown>;
    const payload = book1.payload as Record<string, unknown>;
    payload.asset_id = "222222";
    lines[1] = JSON.stringify(book1);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/payload\.asset_id.*does not match tokenId/);
  });

  it("fails when book payload.market does not match conditionId", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const book1 = JSON.parse(requireLine(lines, 1)) as Record<string, unknown>;
    const payload = book1.payload as Record<string, unknown>;
    payload.market = "0xwrong";
    lines[1] = JSON.stringify(book1);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(
      /payload\.market.*does not match envelope\.sourceMarketId/,
    );
  });

  it("fails when final bestBid is changed in health", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const health = await readJson(path.join(dir, "health.json"));
    const bookHealth = health.finalBookHealth as Record<string, Record<string, unknown>>;
    const upHealth = bookHealth["111111"];
    if (upHealth === undefined) throw new Error("fixture missing book 111111");
    upHealth.bestBid = ".99";
    await writeJson(path.join(dir, "health.json"), health);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/replayed bestBid.*does not match health/);
  });

  it("fails when depth count is changed in health", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const health = await readJson(path.join(dir, "health.json"));
    const bookHealth = health.finalBookHealth as Record<string, Record<string, unknown>>;
    const upHealth = bookHealth["111111"];
    if (upHealth === undefined) throw new Error("fixture missing book 111111");
    upHealth.bidDepthCount = 99;
    await writeJson(path.join(dir, "health.json"), health);
    await expect(verifyRun(dir, 1)).rejects.toThrow(
      /replayed bidDepthCount.*does not match health/,
    );
  });

  it("fails when integrity is changed in health", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const health = await readJson(path.join(dir, "health.json"));
    const bookHealth = health.finalBookHealth as Record<string, Record<string, unknown>>;
    const upHealth = bookHealth["111111"];
    if (upHealth === undefined) throw new Error("fixture missing book 111111");
    upHealth.integrity = "CROSSED";
    await writeJson(path.join(dir, "health.json"), health);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/replayed integrity.*does not match health/);
  });

  it("fails when crossedIncidentCount is changed in health", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const health = await readJson(path.join(dir, "health.json"));
    const bookHealth = health.finalBookHealth as Record<string, Record<string, unknown>>;
    const upHealth = bookHealth["111111"];
    if (upHealth === undefined) throw new Error("fixture missing book 111111");
    upHealth.crossedIncidentCount = 99;
    await writeJson(path.join(dir, "health.json"), health);
    await expect(verifyRun(dir, 1)).rejects.toThrow(
      /replayed crossedIncidentCount.*does not match health/,
    );
  });

  it("fails when health.referenceClockSkewMs differs from recorder policy (5000)", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const health = await readJson(path.join(dir, "health.json"));
    health.referenceClockSkewMs = 10_000;
    await writeJson(path.join(dir, "health.json"), health);
    await expect(verifyRun(dir, 1)).rejects.toThrow(/referenceClockSkewMs is 10000, expected 5000/);
  });

  it("fails when price_change payload.market is changed to another condition", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const book1 = JSON.parse(requireLine(lines, 1)) as Record<string, unknown>;
    const priceChange = {
      ...book1,
      sequence: 5,
      eventType: "price_change",
      tokenId: "111111",
      observedTokenIds: null,
      payload: {
        event_type: "price_change",
        asset_id: "111111",
        market: "0xwrong",
        price_changes: [{ asset_id: "111111", price: ".49", size: "10", side: "BUY" }],
        timestamp: String(1785909720100),
      },
    };
    lines.push(JSON.stringify(priceChange));
    const manifest = await readJson(path.join(dir, "manifest.json"));
    manifest.finalEventCount = 5;
    await writeJson(path.join(dir, "manifest.json"), manifest);
    const health = await readJson(path.join(dir, "health.json"));
    health.totalEventsReceived = 5;
    (health.eventCountsByType as Record<string, number>)["POLYMARKET_CLOB:price_change"] = 1;
    (health.eventCountsBySource as Record<string, number>).POLYMARKET_CLOB = 3;
    await writeJson(path.join(dir, "health.json"), health);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(
      /price_change event sequence 5: payload\.market.*does not match envelope\.sourceMarketId/,
    );
  });

  it("fails when price_change payload.event_type is changed", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const book1 = JSON.parse(requireLine(lines, 1)) as Record<string, unknown>;
    const priceChange = {
      ...book1,
      sequence: 5,
      eventType: "price_change",
      tokenId: "111111",
      observedTokenIds: null,
      payload: {
        event_type: "book",
        asset_id: "111111",
        market: "0xcondition",
        price_changes: [{ asset_id: "111111", price: ".49", size: "10", side: "BUY" }],
        timestamp: String(1785909720100),
      },
    };
    lines.push(JSON.stringify(priceChange));
    const manifest = await readJson(path.join(dir, "manifest.json"));
    manifest.finalEventCount = 5;
    await writeJson(path.join(dir, "manifest.json"), manifest);
    const health = await readJson(path.join(dir, "health.json"));
    health.totalEventsReceived = 5;
    (health.eventCountsByType as Record<string, number>)["POLYMARKET_CLOB:price_change"] = 1;
    (health.eventCountsBySource as Record<string, number>).POLYMARKET_CLOB = 3;
    await writeJson(path.join(dir, "health.json"), health);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(
      /price_change event sequence 5: payload\.event_type.*does not match envelope\.eventType/,
    );
  });

  it("fails when price_change observedTokenIds is missing a payload token", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const book1 = JSON.parse(requireLine(lines, 1)) as Record<string, unknown>;
    const priceChange = {
      ...book1,
      sequence: 5,
      eventType: "price_change",
      tokenId: null,
      observedTokenIds: ["111111"],
      payload: {
        event_type: "price_change",
        asset_id: "111111",
        market: "0xcondition",
        price_changes: [
          { asset_id: "111111", price: ".49", size: "10", side: "BUY" },
          { asset_id: "222222", price: ".51", size: "10", side: "SELL" },
        ],
        timestamp: String(1785909720100),
      },
    };
    lines.push(JSON.stringify(priceChange));
    const manifest = await readJson(path.join(dir, "manifest.json"));
    manifest.finalEventCount = 5;
    await writeJson(path.join(dir, "manifest.json"), manifest);
    const health = await readJson(path.join(dir, "health.json"));
    health.totalEventsReceived = 5;
    (health.eventCountsByType as Record<string, number>)["POLYMARKET_CLOB:price_change"] = 1;
    (health.eventCountsBySource as Record<string, number>).POLYMARKET_CLOB = 3;
    await writeJson(path.join(dir, "health.json"), health);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(
      /price_change event sequence 5: observedTokenIds.*does not match payload token set/,
    );
  });

  it("fails when price_change observedTokenIds contains an extra token", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const book1 = JSON.parse(requireLine(lines, 1)) as Record<string, unknown>;
    const priceChange = {
      ...book1,
      sequence: 5,
      eventType: "price_change",
      tokenId: null,
      observedTokenIds: ["111111", "222222", "999999"],
      payload: {
        event_type: "price_change",
        asset_id: "111111",
        market: "0xcondition",
        price_changes: [
          { asset_id: "111111", price: ".49", size: "10", side: "BUY" },
          { asset_id: "222222", price: ".51", size: "10", side: "SELL" },
        ],
        timestamp: String(1785909720100),
      },
    };
    lines.push(JSON.stringify(priceChange));
    const manifest = await readJson(path.join(dir, "manifest.json"));
    manifest.finalEventCount = 5;
    await writeJson(path.join(dir, "manifest.json"), manifest);
    const health = await readJson(path.join(dir, "health.json"));
    health.totalEventsReceived = 5;
    (health.eventCountsByType as Record<string, number>)["POLYMARKET_CLOB:price_change"] = 1;
    (health.eventCountsBySource as Record<string, number>).POLYMARKET_CLOB = 3;
    await writeJson(path.join(dir, "health.json"), health);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(
      /price_change event sequence 5: observedTokenIds.*does not match payload token set/,
    );
  });

  it("fails when price_change tokenId is inconsistent with payload asset_id", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const book1 = JSON.parse(requireLine(lines, 1)) as Record<string, unknown>;
    const priceChange = {
      ...book1,
      sequence: 5,
      eventType: "price_change",
      tokenId: "222222",
      observedTokenIds: null,
      payload: {
        event_type: "price_change",
        asset_id: "111111",
        market: "0xcondition",
        price_changes: [{ asset_id: "111111", price: ".49", size: "10", side: "BUY" }],
        timestamp: String(1785909720100),
      },
    };
    lines.push(JSON.stringify(priceChange));
    const manifest = await readJson(path.join(dir, "manifest.json"));
    manifest.finalEventCount = 5;
    await writeJson(path.join(dir, "manifest.json"), manifest);
    const health = await readJson(path.join(dir, "health.json"));
    health.totalEventsReceived = 5;
    (health.eventCountsByType as Record<string, number>)["POLYMARKET_CLOB:price_change"] = 1;
    (health.eventCountsBySource as Record<string, number>).POLYMARKET_CLOB = 3;
    await writeJson(path.join(dir, "health.json"), health);
    await writeLines(path.join(dir, "events.jsonl"), lines);
    await expect(verifyRun(dir, 1)).rejects.toThrow(
      /price_change event sequence 5: envelope\.tokenId.*does not match single payload token/,
    );
  });

  it("replay resync guard: epoch advances exactly once for STALE_STREAM + DATA_GAP_STARTED", async () => {
    const dir = await createTempRunDir();
    await copyFixture(dir);
    const lines = await readLines(path.join(dir, "events.jsonl"));
    const market = JSON.parse(requireLine(lines, 0)) as Record<string, unknown>;
    const book1 = JSON.parse(requireLine(lines, 1)) as Record<string, unknown>;
    const book2 = JSON.parse(requireLine(lines, 2)) as Record<string, unknown>;
    const ref = JSON.parse(requireLine(lines, 3)) as Record<string, unknown>;

    const staleStream = {
      ...book1,
      sequence: 5,
      eventType: "STALE_STREAM",
      tokenId: null,
      observedTokenIds: null,
      sourceMarketId: null,
      sourceTimestamp: null,
      payload: { feed: "clob-market", reason: "stale" },
    };
    const gapStarted = {
      ...book1,
      sequence: 6,
      eventType: "DATA_GAP_STARTED",
      tokenId: null,
      observedTokenIds: null,
      sourceMarketId: null,
      sourceTimestamp: null,
      payload: { feed: "clob-market" },
    };
    const book1Gen2 = {
      ...book1,
      sequence: 7,
      connectionGeneration: 2,
      processedAt: 1785909720120,
      receivedAt: 1785909720119,
      sourceTimestamp: 1785909720118,
    };
    const book2Gen2 = {
      ...book2,
      sequence: 8,
      connectionGeneration: 2,
      processedAt: 1785909720121,
      receivedAt: 1785909720120,
      sourceTimestamp: 1785909720119,
    };
    const refGen2 = {
      ...ref,
      sequence: 9,
      connectionGeneration: 2,
      receivedAt: 1785909720123,
      processedAt: 1785909720124,
      sourceTimestamp: 1785909720122,
      topLevelSourceTimestamp: 1785909720122,
      payloadSourceTimestamp: 1785909720122,
    };
    const closed = {
      ...book1,
      sequence: 10,
      eventType: "CONNECTION_CLOSED",
      tokenId: null,
      observedTokenIds: null,
      sourceMarketId: null,
      sourceTimestamp: null,
      payload: { code: 1000, reason: "controlled shutdown" },
    };

    const newLines = [
      JSON.stringify(market),
      JSON.stringify(book1),
      JSON.stringify(book2),
      JSON.stringify(ref),
      JSON.stringify(staleStream),
      JSON.stringify(gapStarted),
      JSON.stringify(book1Gen2),
      JSON.stringify(book2Gen2),
      JSON.stringify(refGen2),
      JSON.stringify(closed),
    ];
    await writeLines(path.join(dir, "events.jsonl"), newLines);

    const health = await readJson(path.join(dir, "health.json"));
    health.totalEventsReceived = 10;
    (health.eventCountsBySource as Record<string, number>).POLYMARKET_CLOB = 7;
    (health.eventCountsBySource as Record<string, number>).POLYMARKET_REFERENCE_PRICE = 2;
    (health.eventCountsByType as Record<string, number>)["POLYMARKET_CLOB:book"] = 4;
    (health.eventCountsByType as Record<string, number>)["POLYMARKET_CLOB:STALE_STREAM"] = 1;
    (health.eventCountsByType as Record<string, number>)["POLYMARKET_CLOB:DATA_GAP_STARTED"] = 1;
    (health.eventCountsByType as Record<string, number>)["POLYMARKET_CLOB:CONNECTION_CLOSED"] = 1;
    (health.eventCountsByType as Record<string, number>)[
      "POLYMARKET_REFERENCE_PRICE:REFERENCE_PRICE"
    ] = 2;
    health.staleFeedIncidents = 1;
    health.dataGapCount = 1;
    health.closedGapCount = 0;
    health.reconnectCount = 1;
    health.maximumSourceToReceiveLatencyMs = 5;
    health.maximumReceiveToProcessLatencyMs = 1;
    health.lastReferenceReceivedAt = 1785909720123;
    health.lastReferenceSourceTimestamp = 1785909720122;
    health.lastReferenceConnectionGeneration = 2;
    health.finalReferenceAgeMs = 1785909720126 - 1785909720123;
    health.finalReferenceSourceAgeMs = 1785909720126 - 1785909720122;
    const bookHealth = health.finalBookHealth as Record<string, Record<string, unknown>>;
    const upHealth = bookHealth["111111"];
    if (upHealth === undefined) throw new Error("fixture missing book 111111");
    const downHealth = bookHealth["222222"];
    if (downHealth === undefined) throw new Error("fixture missing book 222222");
    upHealth.synchronizationEpoch = 1;
    upHealth.snapshotConnectionGeneration = 2;
    upHealth.lastSuccessfulUpdateTime = 1785909720120;
    downHealth.synchronizationEpoch = 1;
    downHealth.snapshotConnectionGeneration = 2;
    downHealth.lastSuccessfulUpdateTime = 1785909720121;
    await writeJson(path.join(dir, "health.json"), health);

    const manifest = await readJson(path.join(dir, "manifest.json"));
    manifest.finalEventCount = 10;
    await writeJson(path.join(dir, "manifest.json"), manifest);

    // The verifier should pass — epoch advanced exactly once despite
    // both STALE_STREAM and DATA_GAP_STARTED being present
    await expect(verifyRun(dir, 1)).resolves.toBeUndefined();
  });
});
