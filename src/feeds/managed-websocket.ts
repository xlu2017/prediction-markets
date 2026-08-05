import WebSocket, { type RawData } from "ws";

import { reconnectDelayMs, StaleDetector } from "../reliability.js";
import type { EventSource, JsonValue, Logger } from "../types.js";

export type FeedFrame = {
  source: EventSource;
  rawText: string;
  receivedAt: number;
  connectionGeneration: number;
};

export type FeedLifecycle = {
  source: EventSource;
  eventType: string;
  receivedAt: number;
  connectionGeneration: number;
  payload: Record<string, JsonValue>;
};

export type ManagedWebSocketOptions = {
  name: string;
  source: EventSource;
  url: string;
  subscription: JsonValue;
  pingIntervalMs: number;
  staleAfterMs: number;
  reconnectBaseMs: number;
  reconnectMaximumMs: number;
  logger: Logger;
  onFrame: (frame: FeedFrame) => void;
  onLifecycle: (event: FeedLifecycle) => void;
  now?: () => number;
  random?: () => number;
  createSocket?: (url: string) => WebSocket;
};

function rawDataToString(data: RawData): string {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return Buffer.from(data).toString("utf8");
  if (Array.isArray(data)) return Buffer.concat(data).toString("utf8");
  return data.toString("utf8");
}

export class ManagedWebSocket {
  private socket: WebSocket | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private pingTimer: NodeJS.Timeout | null = null;
  private staleTimer: NodeJS.Timeout | null = null;
  private stopping = false;
  private reconnectAttempt = 0;
  private generation = 0;
  private gapStartedAt: number | null = null;
  private readonly staleDetector: StaleDetector;
  private readonly now: () => number;
  private readonly random: () => number;
  private readonly createSocket: (url: string) => WebSocket;

  public constructor(private readonly options: ManagedWebSocketOptions) {
    this.staleDetector = new StaleDetector(options.staleAfterMs);
    this.now = options.now ?? Date.now;
    this.random = options.random ?? Math.random;
    this.createSocket = options.createSocket ?? ((url) => new WebSocket(url));
  }

  public start(): void {
    if (this.socket !== null || this.reconnectTimer !== null) {
      throw new Error(`${this.options.name} feed is already started`);
    }
    this.stopping = false;
    this.connect();
  }

  public async stop(): Promise<void> {
    this.stopping = true;
    this.clearTimers();
    const socket = this.socket;
    this.socket = null;
    if (socket === null || socket.readyState === WebSocket.CLOSED) return;

    await new Promise<void>((resolve) => {
      const timeout = setTimeout(() => {
        socket.terminate();
        resolve();
      }, 1_000);
      timeout.unref();
      socket.once("close", () => {
        clearTimeout(timeout);
        resolve();
      });
      if (socket.readyState === WebSocket.CONNECTING) socket.terminate();
      else socket.close(1000, "controlled shutdown");
    });
  }

  private connect(): void {
    if (this.stopping) return;
    this.generation += 1;
    const currentGeneration = this.generation;
    const socket = this.createSocket(this.options.url);
    this.socket = socket;

    this.emitLifecycle("CONNECTION_ATTEMPT", currentGeneration, { url: this.options.url });

    socket.on("open", () => {
      if (this.stopping || socket !== this.socket) return;
      const openedAt = this.now();
      this.reconnectAttempt = 0;
      this.staleDetector.observe(openedAt);
      this.emitLifecycle("CONNECTION_OPEN", currentGeneration, {});
      socket.send(JSON.stringify(this.options.subscription));
      this.emitLifecycle("SUBSCRIPTION_SENT", currentGeneration, {
        subscription: this.options.subscription,
      });
      if (this.gapStartedAt !== null) {
        this.emitLifecycle("DATA_GAP_ENDED", currentGeneration, {
          gapStartedAt: this.gapStartedAt,
          gapDurationMs: Math.max(0, openedAt - this.gapStartedAt),
        });
        this.gapStartedAt = null;
      }
      this.startTimers(socket, currentGeneration);
    });

    socket.on("message", (data) => {
      if (this.stopping || socket !== this.socket) return;
      const receivedAt = this.now();
      const rawText = rawDataToString(data);
      if (rawText !== "PONG" && rawText !== "PING") this.staleDetector.observe(receivedAt);
      this.options.onFrame({
        source: this.options.source,
        rawText,
        receivedAt,
        connectionGeneration: currentGeneration,
      });
    });

    socket.on("error", (error) => {
      this.options.logger.warn(`${this.options.name} WebSocket error`, { message: error.message });
      this.emitLifecycle("CONNECTION_ERROR", currentGeneration, { message: error.message });
    });

    socket.on("close", (code, reasonBuffer) => {
      this.stopConnectionTimers();
      if (socket === this.socket) this.socket = null;
      const closedAt = this.now();
      const reason = reasonBuffer.toString("utf8");
      this.emitLifecycle("CONNECTION_CLOSED", currentGeneration, { code, reason });
      if (this.stopping) return;
      if (this.gapStartedAt === null) {
        this.gapStartedAt = closedAt;
        this.emitLifecycle("DATA_GAP_STARTED", currentGeneration, { gapStartedAt: closedAt });
      }
      const delayMs = reconnectDelayMs(
        this.reconnectAttempt,
        this.options.reconnectBaseMs,
        this.options.reconnectMaximumMs,
        this.random,
      );
      this.reconnectAttempt += 1;
      this.emitLifecycle("RECONNECT_SCHEDULED", currentGeneration, {
        delayMs,
        attempt: this.reconnectAttempt,
      });
      this.reconnectTimer = setTimeout(() => {
        this.reconnectTimer = null;
        this.connect();
      }, delayMs);
    });
  }

  private startTimers(socket: WebSocket, generation: number): void {
    this.stopConnectionTimers();
    this.pingTimer = setInterval(() => {
      if (socket.readyState === WebSocket.OPEN) socket.send("PING");
    }, this.options.pingIntervalMs);
    this.pingTimer.unref();

    this.staleTimer = setInterval(
      () => {
        const now = this.now();
        if (this.staleDetector.check(now)) {
          this.emitLifecycle("STALE_STREAM", generation, {
            staleAfterMs: this.options.staleAfterMs,
          });
        }
      },
      Math.max(250, Math.floor(this.options.staleAfterMs / 4)),
    );
    this.staleTimer.unref();
  }

  private emitLifecycle(
    eventType: string,
    connectionGeneration: number,
    payload: Record<string, JsonValue>,
  ): void {
    this.options.onLifecycle({
      source: this.options.source,
      eventType,
      receivedAt: this.now(),
      connectionGeneration,
      payload: { feed: this.options.name, ...payload },
    });
  }

  private stopConnectionTimers(): void {
    if (this.pingTimer !== null) clearInterval(this.pingTimer);
    if (this.staleTimer !== null) clearInterval(this.staleTimer);
    this.pingTimer = null;
    this.staleTimer = null;
  }

  private clearTimers(): void {
    this.stopConnectionTimers();
    if (this.reconnectTimer !== null) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
  }
}
