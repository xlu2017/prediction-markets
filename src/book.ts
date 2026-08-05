import { compareDecimalStrings, exactWireDecimalString } from "./decimal.js";
import type { BookHealth } from "./types.js";

type BookSide = Map<string, string>;
type TokenBook = {
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

function applyLevels(side: BookSide, levels: unknown): void {
  if (!Array.isArray(levels)) throw new Error("Book levels must be an array");
  side.clear();
  for (const rawLevel of levels) {
    const level = asRecord(rawLevel);
    if (level === null) throw new Error("Book level must be an object");
    const price = exactWireDecimalString(recordValue(level, "price"), "book price");
    const size = exactWireDecimalString(recordValue(level, "size"), "book size");
    if (compareDecimalStrings(size, "0") > 0) side.set(price, size);
  }
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

export class OrderBookTracker {
  private readonly books = new Map<string, TokenBook>();

  public constructor(tokenIds: string[]) {
    for (const tokenId of tokenIds) {
      this.books.set(tokenId, { bids: new Map(), asks: new Map(), lastSuccessfulUpdateTime: null });
    }
  }

  public apply(event: unknown, processedAt: number): string[] {
    const record = asRecord(event);
    if (record === null) throw new Error("CLOB event must be an object");
    const eventType = recordValue(record, "event_type");
    if (typeof eventType !== "string") throw new Error("CLOB event is missing event_type");

    if (eventType === "book") {
      const tokenId = recordValue(record, "asset_id");
      if (typeof tokenId !== "string") throw new Error("Book event is missing asset_id");
      const book = this.books.get(tokenId);
      if (book === undefined) return [];
      applyLevels(book.bids, recordValue(record, "bids"));
      applyLevels(book.asks, recordValue(record, "asks"));
      book.lastSuccessfulUpdateTime = processedAt;
      return [tokenId];
    }

    if (eventType === "price_change") {
      const changes = recordValue(record, "price_changes");
      if (!Array.isArray(changes)) throw new Error("price_change event is missing price_changes");
      const updated = new Set<string>();
      for (const rawChange of changes) {
        const change = asRecord(rawChange);
        if (change === null) throw new Error("Price change must be an object");
        const tokenId = recordValue(change, "asset_id");
        const sideName = recordValue(change, "side");
        if (typeof tokenId !== "string" || typeof sideName !== "string") {
          throw new Error("Price change is missing asset_id or side");
        }
        const book = this.books.get(tokenId);
        if (book === undefined) continue;
        const price = exactWireDecimalString(recordValue(change, "price"), "price change price");
        const size = exactWireDecimalString(recordValue(change, "size"), "price change size");
        const side = sideName.toUpperCase() === "BUY" ? book.bids : book.asks;
        if (compareDecimalStrings(size, "0") === 0) side.delete(price);
        else side.set(price, size);
        book.lastSuccessfulUpdateTime = processedAt;
        updated.add(tokenId);
      }
      return [...updated];
    }

    const tokenId = recordValue(record, "asset_id");
    if (typeof tokenId === "string" && this.books.has(tokenId)) {
      const book = this.books.get(tokenId);
      if (book !== undefined) book.lastSuccessfulUpdateTime = processedAt;
      return [tokenId];
    }
    return [];
  }

  public snapshot(nowMs: number, staleAfterMs: number): Record<string, BookHealth> {
    const result: Record<string, BookHealth> = {};
    for (const [tokenId, book] of this.books) {
      const lastUpdate = book.lastSuccessfulUpdateTime;
      result[tokenId] = {
        tokenId,
        bestBid: best(book.bids, "highest"),
        bestAsk: best(book.asks, "lowest"),
        bidDepthCount: book.bids.size,
        askDepthCount: book.asks.size,
        lastSuccessfulUpdateTime: lastUpdate,
        healthy: lastUpdate !== null && nowMs - lastUpdate <= staleAfterMs,
      };
    }
    return result;
  }
}
