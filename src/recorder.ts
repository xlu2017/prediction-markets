import { performance } from "node:perf_hooks";

import type WebSocket from "ws";

import { OrderBookTracker } from "./book.js";
import { PUBLIC_ENDPOINTS } from "./endpoints.js";
import { type FeedFrame, type FeedLifecycle, ManagedWebSocket } from "./feeds/managed-websocket.js";
import { discoverBtcFiveMinuteMarket } from "./gamma.js";
import { HealthTracker } from "./health.js";
import { createLogger, errorMessage } from "./logger.js";
import { RecorderEventProcessor } from "./processor.js";
import { createManifest, createRunId, currentGitCommit, RunStore } from "./store.js";
import type { JsonValue, LogLevel, Logger, MarketSpecification } from "./types.js";

export type RecorderOptions = {
  durationSeconds: number;
  outputDir: string;
  marketSlug?: string;
  logLevel: LogLevel;
  signal?: AbortSignal;
};

export type RecorderDependencies = {
  gammaBaseUrl?: string;
  clobWebSocketUrl?: string;
  referenceWebSocketUrl?: string;
  fetchImplementation?: typeof fetch;
  createSocket?: (url: string) => WebSocket;
  now?: () => number;
  monotonicNow?: () => number;
  random?: () => number;
  runId?: string;
  cwd?: string;
  logger?: Logger;
  reconnectBaseMs?: number;
  reconnectMaximumMs?: number;
  clobStaleAfterMs?: number;
  referenceStaleAfterMs?: number;
};

export type RecorderResult = {
  runId: string;
  runDirectory: string;
  eventCount: number;
  market: MarketSpecification;
};

function assertOptions(options: RecorderOptions): void {
  if (!Number.isSafeInteger(options.durationSeconds) || options.durationSeconds <= 0) {
    throw new Error("durationSeconds must be a positive integer");
  }
  if (options.outputDir.trim() === "") throw new Error("outputDir must not be empty");
}

async function waitForDuration(
  durationMs: number,
  signal: AbortSignal | undefined,
  monotonicNow: () => number,
): Promise<void> {
  const deadline = monotonicNow() + durationMs;
  while (monotonicNow() < deadline && signal?.aborted !== true) {
    const remaining = Math.max(1, Math.ceil(deadline - monotonicNow()));
    await new Promise<void>((resolve) => {
      let finished = false;
      const finish = (): void => {
        if (finished) return;
        finished = true;
        clearTimeout(timeout);
        signal?.removeEventListener("abort", finish);
        resolve();
      };
      const timeout = setTimeout(finish, Math.min(remaining, 1_000));
      signal?.addEventListener("abort", finish, { once: true });
      timeout.unref();
    });
  }
}

function createFeeds(input: {
  market: MarketSpecification;
  dependencies: RecorderDependencies;
  logger: Logger;
  onFrame: (frame: FeedFrame) => void;
  onLifecycle: (event: FeedLifecycle) => void;
}): [ManagedWebSocket, ManagedWebSocket] {
  const shared = {
    pingIntervalMs: 5_000,
    reconnectBaseMs: input.dependencies.reconnectBaseMs ?? 500,
    reconnectMaximumMs: input.dependencies.reconnectMaximumMs ?? 15_000,
    logger: input.logger,
    onFrame: input.onFrame,
    onLifecycle: input.onLifecycle,
    ...(input.dependencies.now === undefined ? {} : { now: input.dependencies.now }),
    ...(input.dependencies.random === undefined ? {} : { random: input.dependencies.random }),
    ...(input.dependencies.createSocket === undefined
      ? {}
      : { createSocket: input.dependencies.createSocket }),
  };

  return [
    new ManagedWebSocket({
      ...shared,
      name: "clob-market",
      source: "POLYMARKET_CLOB",
      url: input.dependencies.clobWebSocketUrl ?? PUBLIC_ENDPOINTS.clobMarketWebSocket,
      staleAfterMs: input.dependencies.clobStaleAfterMs ?? 15_000,
      subscription: {
        assets_ids: [input.market.upTokenId, input.market.downTokenId],
        type: "market",
        custom_feature_enabled: true,
      },
    }),
    new ManagedWebSocket({
      ...shared,
      name: "chainlink-reference",
      source: "POLYMARKET_REFERENCE_PRICE",
      url: input.dependencies.referenceWebSocketUrl ?? PUBLIC_ENDPOINTS.referencePriceWebSocket,
      staleAfterMs: input.dependencies.referenceStaleAfterMs ?? 10_000,
      subscription: {
        action: "subscribe",
        subscriptions: [
          {
            topic: input.market.referenceTopic,
            type: "*",
            filters: JSON.stringify({ symbol: input.market.referenceSymbol }),
          },
        ],
      },
    }),
  ];
}

export async function runRecorder(
  options: RecorderOptions,
  dependencies: RecorderDependencies = {},
): Promise<RecorderResult> {
  assertOptions(options);
  const now = dependencies.now ?? Date.now;
  const monotonicNow = dependencies.monotonicNow ?? performance.now.bind(performance);
  const logger = dependencies.logger ?? createLogger(options.logLevel);
  const cwd = dependencies.cwd ?? process.cwd();

  const market = await discoverBtcFiveMinuteMarket({
    logger,
    ...(options.marketSlug === undefined ? {} : { marketSlug: options.marketSlug }),
    ...(dependencies.gammaBaseUrl === undefined ? {} : { gammaBaseUrl: dependencies.gammaBaseUrl }),
    ...(dependencies.fetchImplementation === undefined
      ? {}
      : { fetchImplementation: dependencies.fetchImplementation }),
    nowMs: now(),
  });

  const startedAtMs = now();
  const startedAt = new Date(startedAtMs).toISOString();
  const runId = dependencies.runId ?? createRunId(startedAtMs);
  const gitCommit = await currentGitCommit(cwd);
  const clobStaleAfterMs = dependencies.clobStaleAfterMs ?? 15_000;
  const referenceStaleAfterMs = dependencies.referenceStaleAfterMs ?? 10_000;
  const commandArguments: Record<string, JsonValue> = {
    durationSeconds: options.durationSeconds,
    outputDir: options.outputDir,
    marketSlug: options.marketSlug ?? null,
    logLevel: options.logLevel,
  };
  const store = await RunStore.initialize(
    options.outputDir,
    createManifest({
      runId,
      startedAt,
      market,
      gitCommit,
      commandArguments,
      clobStaleAfterMs,
      referenceStaleAfterMs,
    }),
  );
  const books = new OrderBookTracker([market.upTokenId, market.downTokenId], market.tickSize);
  const health = new HealthTracker(runId, startedAt, [market.upTokenId, market.downTokenId], books);
  const fatalController = new AbortController();
  const feedsBySource = new Map<string, ManagedWebSocket>();
  let currentClobGeneration: number | null = null;
  const onValidData = (frame: FeedFrame): void => {
    const feed = feedsBySource.get(frame.source);
    if (feed === undefined) return;
    if (frame.source === "POLYMARKET_CLOB") {
      currentClobGeneration = frame.connectionGeneration;
    }
    feed.notifyValidDataMessage(frame.receivedAt, frame.connectionGeneration);
  };
  const onClobGap = (): void => {
    currentClobGeneration = null;
  };
  const onFatalError = (): void => {
    logger.error("Fatal processing error detected, stopping feeds");
    fatalController.abort("fatal processing error");
  };
  const processor = new RecorderEventProcessor(
    runId,
    market,
    store,
    books,
    health,
    logger,
    now,
    onValidData,
    onFatalError,
    onClobGap,
  );
  const feeds = createFeeds({
    market,
    dependencies,
    logger,
    onFrame: (frame) => processor.enqueueFrame(frame),
    onLifecycle: (event) => processor.enqueueLifecycle(event),
  });
  for (const feed of feeds) feedsBySource.set(feed.source, feed);

  processor.enqueueMarketMetadata(startedAtMs);
  try {
    for (const feed of feeds) feed.start();
    logger.info("Recorder started", {
      runId,
      slug: market.slug,
      durationSeconds: options.durationSeconds,
      outputDirectory: store.directory,
    });
    const combinedSignal = options.signal;
    if (combinedSignal !== undefined) {
      combinedSignal.addEventListener("abort", () => fatalController.abort(combinedSignal.reason), {
        once: true,
      });
    }
    await waitForDuration(options.durationSeconds * 1_000, fatalController.signal, monotonicNow);
    await Promise.all(feeds.map(async (feed) => feed.stop()));
    const eventCount = await processor.drain();
    const endedAtMs = now();
    const endedAt = new Date(endedAtMs).toISOString();
    const healthSummary = health.summary(
      endedAt,
      endedAtMs,
      clobStaleAfterMs,
      referenceStaleAfterMs,
      currentClobGeneration,
    );
    await store.finalize({
      status: "COMPLETED",
      endedAt,
      finalEventCount: eventCount,
      health: healthSummary,
      captureStatus: healthSummary.captureStatus,
      usableForReplay: healthSummary.usableForReplay,
      qualityFailures: healthSummary.qualityFailures,
    });
    logger.info("Recorder completed", {
      runId,
      eventCount,
      outputDirectory: store.directory,
      captureStatus: healthSummary.captureStatus,
      usableForReplay: healthSummary.usableForReplay,
    });
    return { runId, runDirectory: store.directory, eventCount, market };
  } catch (error) {
    await Promise.allSettled(feeds.map(async (feed) => feed.stop()));
    await processor.drain().catch(() => undefined);
    const endedAtMs = now();
    const endedAt = new Date(endedAtMs).toISOString();
    const reason = errorMessage(error);
    const healthSummary = health.summary(
      endedAt,
      endedAtMs,
      clobStaleAfterMs,
      referenceStaleAfterMs,
      currentClobGeneration,
    );
    await store
      .finalize({
        status: "FAILED",
        endedAt,
        finalEventCount: processor.count,
        health: healthSummary,
        captureStatus: healthSummary.captureStatus,
        usableForReplay: healthSummary.usableForReplay,
        qualityFailures: healthSummary.qualityFailures,
        failureReason: reason,
      })
      .catch((finalizationError: unknown) => {
        logger.error("Failed to finalize unsuccessful run", {
          message: errorMessage(finalizationError),
        });
      });
    throw error;
  }
}
