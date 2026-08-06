import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, open, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";

import type {
  CaptureStatus,
  CompletionStatus,
  HealthSummary,
  JsonValue,
  MarketSpecification,
  QualityFailure,
  RawEventEnvelope,
  RunManifest,
} from "./types.js";

const execFileAsync = promisify(execFile);
export const RECORDER_VERSION = "0.2.0";
export const OUTPUT_FILES = ["manifest.json", "market.json", "events.jsonl", "health.json"];

export function createRunId(nowMs = Date.now(), uuid = randomUUID()): string {
  return `${new Date(nowMs).toISOString().replace(/[-:.]/gu, "")}-${uuid}`;
}

export async function currentGitCommit(cwd: string): Promise<string | null> {
  try {
    const { stdout } = await execFileAsync("git", ["rev-parse", "HEAD"], { cwd });
    const commit = stdout.trim();
    return /^[0-9a-f]{40}$/u.test(commit) ? commit : null;
  } catch {
    return null;
  }
}

async function atomicJson(location: string, value: JsonValue): Promise<void> {
  const temporary = `${location}.${randomUUID()}.tmp`;
  await writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, {
    encoding: "utf8",
    flag: "wx",
  });
  await rename(temporary, location);
}

export function createManifest(input: {
  runId: string;
  startedAt: string;
  market: MarketSpecification;
  gitCommit: string | null;
  commandArguments: Record<string, JsonValue>;
  clobStaleAfterMs: number;
  referenceStaleAfterMs: number;
}): RunManifest {
  return {
    schemaVersion: 2,
    runId: input.runId,
    startedAt: input.startedAt,
    endedAt: null,
    recorderVersion: RECORDER_VERSION,
    gitCommit: input.gitCommit,
    market: input.market,
    commandArguments: input.commandArguments,
    outputFiles: [...OUTPUT_FILES],
    completionStatus: "RUNNING",
    finalEventCount: null,
    failureReason: null,
    captureStatus: "UNUSABLE",
    usableForReplay: false,
    qualityFailures: [],
    clobStaleAfterMs: input.clobStaleAfterMs,
    referenceStaleAfterMs: input.referenceStaleAfterMs,
  };
}

export class RunStore {
  private constructor(
    public readonly directory: string,
    private manifest: RunManifest,
    private readonly eventsFile: Awaited<ReturnType<typeof open>>,
  ) {}

  public static async initialize(outputBase: string, manifest: RunManifest): Promise<RunStore> {
    const directory = path.resolve(outputBase, manifest.runId);
    await mkdir(directory, { recursive: true });
    const eventsFile = await open(path.join(directory, "events.jsonl"), "wx");
    const store = new RunStore(directory, manifest, eventsFile);
    await atomicJson(path.join(directory, "market.json"), {
      schemaVersion: 2,
      market: manifest.market,
    });
    await store.writeManifest();
    return store;
  }

  public async append(envelope: RawEventEnvelope): Promise<void> {
    await this.eventsFile.write(`${JSON.stringify(envelope)}\n`, undefined, "utf8");
  }

  public async flush(): Promise<void> {
    await this.eventsFile.sync();
  }

  public async finalize(input: {
    status: Exclude<CompletionStatus, "RUNNING">;
    endedAt: string;
    finalEventCount: number;
    health: HealthSummary;
    captureStatus: CaptureStatus;
    usableForReplay: boolean;
    qualityFailures: QualityFailure[];
    failureReason?: string | null;
  }): Promise<void> {
    await this.flush();
    this.manifest = {
      ...this.manifest,
      endedAt: input.endedAt,
      completionStatus: input.status,
      finalEventCount: input.finalEventCount,
      failureReason: input.failureReason ?? null,
      captureStatus: input.captureStatus,
      usableForReplay: input.usableForReplay,
      qualityFailures: input.qualityFailures,
    };
    await atomicJson(path.join(this.directory, "health.json"), input.health);
    await this.writeManifest();
    await this.eventsFile.close();
  }

  private async writeManifest(): Promise<void> {
    await atomicJson(path.join(this.directory, "manifest.json"), this.manifest);
  }
}
