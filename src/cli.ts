#!/usr/bin/env node
import { Command, Option } from "commander";

import { createLogger, errorMessage } from "./logger.js";
import { runRecorder } from "./recorder.js";
import type { LogLevel } from "./types.js";

function positiveInteger(value: string): number {
  if (!/^\d+$/u.test(value)) throw new Error("must be a positive integer");
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) throw new Error("must be a positive integer");
  return parsed;
}

const program = new Command()
  .name("market:record")
  .description("Record public Polymarket BTC five-minute market data without authentication")
  .addOption(
    new Option("--duration-seconds <seconds>", "recording duration in seconds")
      .default(120)
      .argParser(positiveInteger),
  )
  .option("--output-dir <path>", "base output directory", "data/polymarket-runs")
  .option("--market-slug <slug>", "explicit event slug; still validated")
  .addOption(
    new Option("--log-level <level>", "minimum structured-log level")
      .choices(["debug", "info", "warn", "error"])
      .default("info"),
  );

program.parse();
const values = program.opts<{
  durationSeconds: number;
  outputDir: string;
  marketSlug?: string;
  logLevel: LogLevel;
}>();

const controller = new AbortController();
const logger = createLogger(values.logLevel);
const handleSignal = (signal: NodeJS.Signals): void => {
  logger.info("Controlled shutdown requested", { signal });
  controller.abort(signal);
};
process.once("SIGINT", handleSignal);
process.once("SIGTERM", handleSignal);

try {
  await runRecorder(
    {
      durationSeconds: values.durationSeconds,
      outputDir: values.outputDir,
      logLevel: values.logLevel,
      signal: controller.signal,
      ...(values.marketSlug === undefined ? {} : { marketSlug: values.marketSlug }),
    },
    { logger },
  );
} catch (error) {
  logger.error("Recorder failed", { message: errorMessage(error) });
  process.exitCode = 1;
} finally {
  process.removeListener("SIGINT", handleSignal);
  process.removeListener("SIGTERM", handleSignal);
}
