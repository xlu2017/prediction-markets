import { describe, expect, it } from "vitest";

import { MessageDeduplicator, type MessageDedupeKey } from "../src/envelope.js";

function makeKey(
  gen: number,
  frameHash: string,
  msgIndex: number,
  msgHash: string,
  ts: number | null,
): MessageDedupeKey {
  return {
    connectionGeneration: gen,
    frameHash,
    messageIndex: msgIndex,
    messageHash: msgHash,
    sourceTimestamp: ts,
  };
}

describe("message-level deduplication", () => {
  it("classifies first occurrence as unique, second as duplicate", () => {
    const d = new MessageDeduplicator();
    const key = makeKey(1, "fh", 0, "mh", 100);
    expect(d.classify(key).isDuplicate).toBe(false);
    d.registerApplied(key, 1);
    const result = d.classify(key);
    expect(result.isDuplicate).toBe(true);
    expect(result.duplicateOfSequence).toBe(1);
  });

  it("does not register as applied until registerApplied is called", () => {
    const d = new MessageDeduplicator();
    const key = makeKey(1, "fh", 0, "mh", 100);
    expect(d.classify(key).isDuplicate).toBe(false);
    expect(d.classify(key).isDuplicate).toBe(false);
    d.registerApplied(key, 1);
    expect(d.classify(key).isDuplicate).toBe(true);
  });

  it("distinguishes messages with same frame hash but different message index", () => {
    const d = new MessageDeduplicator();
    const key0 = makeKey(1, "fh", 0, "mh0", 100);
    const key1 = makeKey(1, "fh", 1, "mh1", 100);
    expect(d.classify(key0).isDuplicate).toBe(false);
    d.registerApplied(key0, 1);
    expect(d.classify(key1).isDuplicate).toBe(false);
  });

  it("distinguishes messages with same frame hash and index but different message hash", () => {
    const d = new MessageDeduplicator();
    const keyA = makeKey(1, "fh", 0, "mhA", 100);
    const keyB = makeKey(1, "fh", 0, "mhB", 100);
    expect(d.classify(keyA).isDuplicate).toBe(false);
    d.registerApplied(keyA, 1);
    expect(d.classify(keyB).isDuplicate).toBe(false);
  });

  it("distinguishes duplicates across connection generations", () => {
    const d = new MessageDeduplicator();
    const key1 = makeKey(1, "fh", 0, "mh", 100);
    const key2 = makeKey(2, "fh", 0, "mh", 100);
    expect(d.classify(key1).isDuplicate).toBe(false);
    d.registerApplied(key1, 1);
    expect(d.classify(key2).isDuplicate).toBe(false);
  });

  it("distinguishes duplicates with different source timestamps", () => {
    const d = new MessageDeduplicator();
    const key1 = makeKey(1, "fh", 0, "mh", 100);
    const key2 = makeKey(1, "fh", 0, "mh", 200);
    expect(d.classify(key1).isDuplicate).toBe(false);
    d.registerApplied(key1, 1);
    expect(d.classify(key2).isDuplicate).toBe(false);
  });

  it("treats null key as never duplicate", () => {
    const d = new MessageDeduplicator();
    expect(d.classify(null).isDuplicate).toBe(false);
    d.registerApplied(null, 1);
    expect(d.classify(null).isDuplicate).toBe(false);
  });

  it("evicts oldest entries when capacity is exceeded", () => {
    const d = new MessageDeduplicator(3);
    for (let i = 0; i < 4; i++) {
      const key = makeKey(1, `fh${i}`, 0, `mh${i}`, 100);
      d.classify(key);
      d.registerApplied(key, i + 1);
    }
    const key0 = makeKey(1, "fh0", 0, "mh0", 100);
    expect(d.classify(key0).isDuplicate).toBe(false);
  });
});
