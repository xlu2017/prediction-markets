import {
  compareDecimalStrings,
  exactWireDecimalString,
  isDecimalString,
  isMultipleOfTick,
} from "./decimal.js";
import type { BookHealth, BookState } from "./types.js";

type BookSide = Map<string, string>;
type TokenBook = {
  state: BookState;
  synchronizationEpoch: number;
  snapshotConnectionGeneration: number | null;
  bids: BookSide;
  asks: BookSide;
  lastSuccessfulUpdateTime: number | null;
};

function recordValue(record: Record<string, unknown>, key: string): unknown {
  return Object.hasOwn(record, key) ? record[key] : undefined;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function validatePriceBounds(price: string, label: string): void {
  const cmpZero = compareDecimalStrings(price, "0");
  if (cmpZero <= 0) throw new Error(`${label} price must be > 0, got ${price}`);
  const cmpOne = compareDecimalStrings(price, "1");
  if (cmpOne >= 0) throw new Error(`${label} price must be < 1, got ${price}`);
}

function validateTickSize(price: string, tickSize: string, label: string): void {
  if (tickSize === "0" || !isDecimalString(tickSize)) return;
  if (!isMultipleOfTick(price, tickSize)) {
    throw new Error(`${label} price ${price} is not a multiple of tick size ${tickSize}`);
  }
}

function validateLevel(
  rawLevel: unknown,
  label: string,
  tickSize: string | null,
): { price: string; size: string } {
  const level = asRecord(rawLevel);
  if (level === null) throw new Error(`${label} must be an object`);
  const price = exactWireDecimalString(recordValue(level, "price"), `${label} price`);
  const size = exactWireDecimalString(recordValue(level, "size"), `${label} size`);
  if (compareDecimalStrings(size, "0") < 0) throw new Error(`${label} size must not be negative`);
  validatePriceBounds(price, label);
  if (tickSize !== null) validateTickSize(price, tickSize, label);
  return { price, size };
}

function buildSide(levels: unknown, label: string, tickSize: string | null): BookSide {
  if (!Array.isArray(levels)) throw new Error(`${label} must be an array`);
  const side: BookSide = new Map();
  for (const rawLevel of levels) {
    const { price, size } = validateLevel(rawLevel, label, tickSize);
    if (compareDecimalStrings(size, "0") > 0) {
      if (side.has(price)) throw new Error(`${label} has duplicate price level ${price}`);
      side.set(price, size);
    }
  }
  return side;
}

function best(side: BookSide, direction: "highest" | "lowest"): string | null {
  let candidate: string | null = null;
  for (const price of side.keys()) {
    if (candidate === null) candidate = price;
    else {
      const comparison = compareDecimalStrings(price, candidate);
      if (
        (direction === "highest" && comparison > 0) ||
        (direction === "lowest" && comparison < 0)
      ) {
        candidate = price;
      }
    }
  }
  return candidate;
}

function cloneSide(side: BookSide): BookSide {
  return new Map(side);
}

export class OrderBookTracker {
  private readonly books = new Map<string, TokenBook>();
  private currentSynchronizationEpoch = 0;

  public constructor(
    tokenIds: string[],
    private readonly tickSize: string | null = null,
  ) {
    for (const tokenId of tokenIds) {
      this.books.set(tokenId, {
        state: "UNINITIALIZED",
        synchronizationEpoch: 0,
        snapshotConnectionGeneration: null,
        bids: new Map(),
        asks: new Map(),
        lastSuccessfulUpdateTime: null,
      });
    }
  }

  public apply(event: unknown, processedAt: number, connectionGeneration: number): string[] {
    const record = asRecord(event);
    if (record === null) throw new Error("CLOB event must be an object");
    const eventType = recordValue(record, "event_type");
    if (typeof eventType !== "string") throw new Error("CLOB event is missing event_type");

    if (eventType === "book") return this.applyBook(record, processedAt, connectionGeneration);
    if (eventType === "price_change") return this.applyPriceChange(record, processedAt);
    return this.applyNonBookEvent(record);
  }

  public requireResync(): void {
    this.currentSynchronizationEpoch += 1;
    for (const book of this.books.values()) {
      if (book.state === "SYNCHRONIZED" || book.state === "STALE") {
        book.state = "RESYNC_REQUIRED";
      }
    }
  }

  private applyBook(
    record: Record<string, unknown>,
    processedAt: number,
    connectionGeneration: number,
  ): string[] {
    const tokenId = recordValue(record, "asset_id");
    if (typeof tokenId !== "string") throw new Error("Book event is missing asset_id");
    const book = this.books.get(tokenId);
    if (book === undefined) throw new Error(`Unknown token ID in book event: ${tokenId}`);
    const bids = buildSide(recordValue(record, "bids"), "book bid", this.tickSize);
    const asks = buildSide(recordValue(record, "asks"), "book ask", this.tickSize);
    book.bids = bids;
    book.asks = asks;
    book.state = "SYNCHRONIZED";
    book.snapshotConnectionGeneration = connectionGeneration;
    book.synchronizationEpoch = this.currentSynchronizationEpoch;
    book.lastSuccessfulUpdateTime = processedAt;
    return [tokenId];
  }

  private applyPriceChange(record: Record<string, unknown>, processedAt: number): string[] {
    const changes = recordValue(record, "price_changes");
    if (!Array.isArray(changes)) throw new Error("price_change event is missing price_changes");
    if (changes.length === 0) return [];

    const validated: {
      tokenId: string;
      side: "BUY" | "SELL";
      price: string;
      size: string;
      book: TokenBook;
    }[] = [];
    const updated = new Set<string>();

    for (const rawChange of changes) {
      const change = asRecord(rawChange);
      if (change === null) throw new Error("Price change must be an object");
      const tokenId = recordValue(change, "asset_id");
      const sideName = recordValue(change, "side");
      if (typeof tokenId !== "string") throw new Error("Price change is missing asset_id");
      if (typeof sideName !== "string") throw new Error("Price change is missing side");
      const normalizedSide = sideName.toUpperCase();
      if (normalizedSide !== "BUY" && normalizedSide !== "SELL") {
        throw new Error(`Invalid side "${sideName}"; only BUY and SELL are accepted`);
      }
      const book = this.books.get(tokenId);
      if (book === undefined) throw new Error(`Unknown token ID in price_change: ${tokenId}`);
      if (book.state === "UNINITIALIZED") {
        throw new Error(
          `Cannot apply price_change before initial book snapshot for token ${tokenId}`,
        );
      }
      if (book.state === "INVALID") {
        throw new Error(`Cannot apply price_change to invalid book for token ${tokenId}`);
      }
      if (book.state === "RESYNC_REQUIRED") {
        throw new Error(
          `Cannot apply price_change to resync-required book for token ${tokenId}; fresh snapshot needed`,
        );
      }
      if (book.state === "STALE") {
        throw new Error(
          `Cannot apply price_change to stale book for token ${tokenId}; fresh snapshot needed`,
        );
      }
      const price = exactWireDecimalString(recordValue(change, "price"), "price change price");
      const size = exactWireDecimalString(recordValue(change, "size"), "price change size");
      if (compareDecimalStrings(size, "0") < 0)
        throw new Error("Price change size must not be negative");
      validatePriceBounds(price, "price change");
      if (this.tickSize !== null) validateTickSize(price, this.tickSize, "price change");
      validated.push({ tokenId, side: normalizedSide, price, size, book });
    }

    const backups = new Map<TokenBook, { bids: BookSide; asks: BookSide; state: BookState }>();
    for (const entry of validated) {
      if (!backups.has(entry.book)) {
        backups.set(entry.book, {
          bids: cloneSide(entry.book.bids),
          asks: cloneSide(entry.book.asks),
          state: entry.book.state,
        });
      }
    }

    try {
      for (const entry of validated) {
        const side = entry.side === "BUY" ? entry.book.bids : entry.book.asks;
        if (compareDecimalStrings(entry.size, "0") === 0) side.delete(entry.price);
        else side.set(entry.price, entry.size);
        entry.book.lastSuccessfulUpdateTime = processedAt;
        entry.book.state = "SYNCHRONIZED";
        updated.add(entry.tokenId);
      }
    } catch (error) {
      for (const [book, backup] of backups) {
        book.bids = backup.bids;
        book.asks = backup.asks;
        book.state = "INVALID";
        book.lastSuccessfulUpdateTime = null;
      }
      throw error;
    }
    return [...updated];
  }

  private applyNonBookEvent(record: Record<string, unknown>): string[] {
    const tokenId = recordValue(record, "asset_id");
    if (typeof tokenId === "string" && this.books.has(tokenId)) return [tokenId];
    return [];
  }

  public markStale(nowMs: number, staleAfterMs: number): void {
    for (const book of this.books.values()) {
      if (
        book.state === "SYNCHRONIZED" &&
        book.lastSuccessfulUpdateTime !== null &&
        nowMs - book.lastSuccessfulUpdateTime > staleAfterMs
      ) {
        book.state = "STALE";
      }
    }
  }

  public snapshot(nowMs: number, staleAfterMs: number): Record<string, BookHealth> {
    const result: Record<string, BookHealth> = {};
    for (const [tokenId, book] of this.books) {
      const lastUpdate = book.lastSuccessfulUpdateTime;
      let state = book.state;
      if (state === "SYNCHRONIZED" && lastUpdate !== null && nowMs - lastUpdate > staleAfterMs) {
        state = "STALE";
      }
      result[tokenId] = {
        tokenId,
        state,
        synchronizationEpoch: book.synchronizationEpoch,
        snapshotConnectionGeneration: book.snapshotConnectionGeneration,
        bestBid: best(book.bids, "highest"),
        bestAsk: best(book.asks, "lowest"),
        bidDepthCount: book.bids.size,
        askDepthCount: book.asks.size,
        lastSuccessfulUpdateTime: lastUpdate,
        healthy: state === "SYNCHRONIZED",
      };
    }
    return result;
  }
}
