import type { JsonValue, Logger, LogLevel } from "./types.js";

const priorities: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

export function createLogger(minimumLevel: LogLevel): Logger {
  const write = (
    level: LogLevel,
    message: string,
    context: Record<string, JsonValue> = {},
  ): void => {
    if (priorities[level] < priorities[minimumLevel]) return;
    const line = JSON.stringify({
      timestamp: new Date().toISOString(),
      level,
      message,
      ...context,
    });
    if (level === "error") process.stderr.write(`${line}\n`);
    else process.stdout.write(`${line}\n`);
  };

  return {
    debug: (message, context) => write("debug", message, context),
    error: (message, context) => write("error", message, context),
    info: (message, context) => write("info", message, context),
    warn: (message, context) => write("warn", message, context),
  };
}

export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
