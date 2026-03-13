import createHandler from "./redis-strings-cache-components";
import type { CacheComponentsEntry } from "./cache-components-handler";

function createMockRedisClient() {
  return {
    isReady: true,
    get: jest.fn<Promise<string | null>, [string]>().mockResolvedValue(null),
    set: jest
      .fn<Promise<string | null>, [string, string, object?]>()
      .mockResolvedValue("OK"),
    unlink: jest
      .fn<Promise<number>, [string | string[]]>()
      .mockResolvedValue(1),
    hmGet: jest
      .fn<Promise<(string | null)[]>, [string, string[]]>()
      .mockResolvedValue([]),
    hSet: jest
      .fn<Promise<number>, [string, ...unknown[]]>()
      .mockResolvedValue(1),
    hDel: jest
      .fn<Promise<number>, [string, string | string[]]>()
      .mockResolvedValue(1),
    hScan: jest.fn().mockResolvedValue({
      cursor: "0",
      entries: [],
    }),
  };
}

function createReadableStream(data: Uint8Array): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(data);
      controller.close();
    },
  });
}

async function readStream(
  stream: ReadableStream<Uint8Array>,
): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    if (value) chunks.push(value);
  }
  if (chunks.length === 1 && chunks[0]) return chunks[0];
  const total = chunks.reduce((sum, c) => sum + c.byteLength, 0);
  const result = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    result.set(c, offset);
    offset += c.byteLength;
  }
  return result;
}

function makeStoredEntry(
  overrides: Partial<{
    value: string;
    tags: string[];
    stale: number;
    timestamp: number;
    expire: number;
    revalidate: number;
  }> = {},
) {
  const data = new Uint8Array([72, 101, 108, 108, 111]); // "Hello"
  return {
    value: overrides.value ?? Buffer.from(data).toString("base64"),
    tags: overrides.tags ?? ["tag1"],
    stale: overrides.stale ?? 0,
    timestamp: overrides.timestamp ?? Date.now(),
    expire: overrides.expire ?? 3600,
    revalidate: overrides.revalidate ?? 60,
  };
}

describe("redis-strings-cache-components", () => {
  let mockClient: ReturnType<typeof createMockRedisClient>;

  beforeEach(() => {
    mockClient = createMockRedisClient();
    jest.clearAllMocks();
  });

  describe("get()", () => {
    it("returns undefined on cache miss", async () => {
      const handler = createHandler({ client: mockClient as never });
      mockClient.get.mockResolvedValue(null);

      const result = await handler.get("missing-key", []);

      expect(result).toBeUndefined();
      expect(mockClient.get).toHaveBeenCalledTimes(1);
    });

    it("returns reconstructed entry on cache hit", async () => {
      const handler = createHandler({ client: mockClient as never });
      const stored = makeStoredEntry({ timestamp: Date.now() });
      mockClient.get.mockResolvedValue(JSON.stringify(stored));
      mockClient.hmGet.mockResolvedValue([null]); // no revalidated tags

      const result = await handler.get("hit-key", []);

      expect(result).toBeDefined();
      expect(result!.tags).toEqual(["tag1"]);
      expect(result!.stale).toBe(stored.stale);
      expect(result!.timestamp).toBe(stored.timestamp);
      expect(result!.expire).toBe(stored.expire);
      expect(result!.revalidate).toBe(stored.revalidate);

      // Verify the stream contains the original data
      const data = await readStream(result!.value);
      expect(Buffer.from(data).toString()).toBe("Hello");
    });

    it("returns undefined for expired entry and unlinks key", async () => {
      const handler = createHandler({ client: mockClient as never });
      const stored = makeStoredEntry({
        timestamp: Date.now() - 7200_000, // 2 hours ago
        expire: 3600, // 1 hour TTL — expired
      });
      mockClient.get.mockResolvedValue(JSON.stringify(stored));

      const result = await handler.get("expired-key", []);

      expect(result).toBeUndefined();
      expect(mockClient.unlink).toHaveBeenCalledWith("expired-key");
    });

    it("returns undefined when tags have been revalidated after entry creation", async () => {
      const handler = createHandler({ client: mockClient as never });
      const entryTimestamp = Date.now() - 60_000; // 1 min ago
      const stored = makeStoredEntry({
        timestamp: entryTimestamp,
        tags: ["tag-a"],
      });
      mockClient.get.mockResolvedValue(JSON.stringify(stored));
      // Tag was revalidated AFTER the entry was created
      mockClient.hmGet.mockResolvedValue([String(entryTimestamp + 30_000)]);

      const result = await handler.get("revalidated-key", []);

      expect(result).toBeUndefined();
      expect(mockClient.unlink).toHaveBeenCalledWith("revalidated-key");
    });

    it("returns entry when tags were revalidated BEFORE entry creation", async () => {
      const handler = createHandler({ client: mockClient as never });
      const entryTimestamp = Date.now();
      const stored = makeStoredEntry({
        timestamp: entryTimestamp,
        tags: ["tag-a"],
      });
      mockClient.get.mockResolvedValue(JSON.stringify(stored));
      // Tag was revalidated BEFORE the entry
      mockClient.hmGet.mockResolvedValue([String(entryTimestamp - 30_000)]);

      const result = await handler.get("still-valid-key", []);

      expect(result).toBeDefined();
    });

    it("checks both stored tags and soft tags for revalidation", async () => {
      const handler = createHandler({ client: mockClient as never });
      const stored = makeStoredEntry({
        timestamp: Date.now(),
        tags: ["stored-tag"],
      });
      mockClient.get.mockResolvedValue(JSON.stringify(stored));
      mockClient.hmGet.mockResolvedValue([null, null]);

      await handler.get("key", ["soft-tag"]);

      // Should query both stored and soft tags
      expect(mockClient.hmGet).toHaveBeenCalledWith("__cc_revalidatedTags__", [
        "stored-tag",
        "soft-tag",
      ]);
    });

    it("does not check revalidation when no tags exist", async () => {
      const handler = createHandler({ client: mockClient as never });
      const stored = makeStoredEntry({ tags: [], timestamp: Date.now() });
      mockClient.get.mockResolvedValue(JSON.stringify(stored));

      const result = await handler.get("no-tags-key", []);

      expect(result).toBeDefined();
      expect(mockClient.hmGet).not.toHaveBeenCalled();
    });

    it("throws when client is not ready", async () => {
      mockClient.isReady = false;
      const handler = createHandler({ client: mockClient as never });

      await expect(handler.get("key", [])).rejects.toThrow(
        "Redis client is not ready",
      );
    });

    it("respects keyPrefix", async () => {
      const handler = createHandler({
        client: mockClient as never,
        keyPrefix: "app:",
      });
      mockClient.get.mockResolvedValue(null);

      await handler.get("my-key", []);

      expect(mockClient.get).toHaveBeenCalledWith("app:my-key");
    });
  });

  describe("set()", () => {
    it("stores entry with base64-encoded value", async () => {
      const handler = createHandler({ client: mockClient as never });
      const data = new Uint8Array([1, 2, 3, 4, 5]);
      const entry: CacheComponentsEntry = {
        value: createReadableStream(data),
        tags: ["t1"],
        stale: 0,
        timestamp: Date.now(),
        expire: 300,
        revalidate: 60,
      };

      await handler.set("set-key", Promise.resolve(entry));

      expect(mockClient.set).toHaveBeenCalledTimes(1);
      const [key, serialized, options] = mockClient.set.mock.calls[0]!;
      expect(key).toBe("set-key");
      const parsed = JSON.parse(serialized);
      expect(parsed.value).toBe(Buffer.from(data).toString("base64"));
      expect(parsed.tags).toEqual(["t1"]);
      expect(options).toEqual({ EX: 300 });
    });

    it("awaits pending entry promise before storing", async () => {
      const handler = createHandler({ client: mockClient as never });
      let resolveEntry!: (entry: CacheComponentsEntry) => void;
      const pendingEntry = new Promise<CacheComponentsEntry>((resolve) => {
        resolveEntry = resolve;
      });

      const setPromise = handler.set("deferred-key", pendingEntry);

      // set should not have been called yet
      expect(mockClient.set).not.toHaveBeenCalled();

      resolveEntry({
        value: createReadableStream(new Uint8Array([42])),
        tags: [],
        stale: 0,
        timestamp: Date.now(),
        expire: 0,
        revalidate: 0,
      });

      await setPromise;
      expect(mockClient.set).toHaveBeenCalledTimes(1);
    });

    it("stores tags in shared tags hash", async () => {
      const handler = createHandler({ client: mockClient as never });
      const entry: CacheComponentsEntry = {
        value: createReadableStream(new Uint8Array([1])),
        tags: ["tag-x", "tag-y"],
        stale: 0,
        timestamp: Date.now(),
        expire: 100,
        revalidate: 10,
      };

      await handler.set("tagged-key", Promise.resolve(entry));

      expect(mockClient.hSet).toHaveBeenCalledWith(
        "__cc_sharedTags__",
        "tagged-key",
        JSON.stringify(["tag-x", "tag-y"]),
      );
    });

    it("does not store tags when entry has no tags", async () => {
      const handler = createHandler({ client: mockClient as never });
      const entry: CacheComponentsEntry = {
        value: createReadableStream(new Uint8Array([1])),
        tags: [],
        stale: 0,
        timestamp: Date.now(),
        expire: 100,
        revalidate: 10,
      };

      await handler.set("no-tags-key", Promise.resolve(entry));

      expect(mockClient.hSet).not.toHaveBeenCalled();
    });

    it("omits TTL when expire is 0", async () => {
      const handler = createHandler({ client: mockClient as never });
      const entry: CacheComponentsEntry = {
        value: createReadableStream(new Uint8Array([1])),
        tags: [],
        stale: 0,
        timestamp: Date.now(),
        expire: 0,
        revalidate: 0,
      };

      await handler.set("no-ttl-key", Promise.resolve(entry));

      const [, , options] = mockClient.set.mock.calls[0]!;
      expect(options).toBeUndefined();
    });
  });

  describe("refreshTags()", () => {
    it("does not throw when client is ready", async () => {
      const handler = createHandler({ client: mockClient as never });

      await expect(handler.refreshTags()).resolves.toBeUndefined();
    });

    it("throws when client is not ready", async () => {
      mockClient.isReady = false;
      const handler = createHandler({ client: mockClient as never });

      await expect(handler.refreshTags()).rejects.toThrow(
        "Redis client is not ready",
      );
    });
  });

  describe("getExpiration()", () => {
    it("returns 0 for empty tags array", async () => {
      const handler = createHandler({ client: mockClient as never });

      const result = await handler.getExpiration([]);

      expect(result).toBe(0);
      expect(mockClient.hmGet).not.toHaveBeenCalled();
    });

    it("returns 0 when no tags have been revalidated", async () => {
      const handler = createHandler({ client: mockClient as never });
      mockClient.hmGet.mockResolvedValue([null, null]);

      const result = await handler.getExpiration(["a", "b"]);

      expect(result).toBe(0);
    });

    it("returns max timestamp across revalidated tags", async () => {
      const handler = createHandler({ client: mockClient as never });
      mockClient.hmGet.mockResolvedValue(["1000", "3000", "2000"]);

      const result = await handler.getExpiration(["a", "b", "c"]);

      expect(result).toBe(3000);
    });
  });

  describe("updateTags()", () => {
    it("does nothing for empty tags array", async () => {
      const handler = createHandler({ client: mockClient as never });

      await handler.updateTags([]);

      expect(mockClient.hSet).not.toHaveBeenCalled();
      expect(mockClient.hScan).not.toHaveBeenCalled();
    });

    it("marks tags as revalidated with current timestamp", async () => {
      const handler = createHandler({ client: mockClient as never });
      const before = Date.now();

      await handler.updateTags(["tag1", "tag2"]);

      const after = Date.now();
      expect(mockClient.hSet).toHaveBeenCalledTimes(1);
      const [key, rawFields] = mockClient.hSet.mock.calls[0]!;
      const fields = rawFields as Record<string, string>;
      expect(key).toBe("__cc_revalidatedTags__");
      const ts1 = Number.parseInt(fields["tag1"]!, 10);
      const ts2 = Number.parseInt(fields["tag2"]!, 10);
      expect(ts1).toBeGreaterThanOrEqual(before);
      expect(ts1).toBeLessThanOrEqual(after);
      expect(ts2).toBeGreaterThanOrEqual(before);
      expect(ts2).toBeLessThanOrEqual(after);
    });

    it("scans and deletes matching cache entries", async () => {
      const handler = createHandler({ client: mockClient as never });

      // Simulate hScan returning entries where some match the tag
      mockClient.hScan.mockResolvedValue({
        cursor: "0",
        entries: [
          { field: "key1", value: JSON.stringify(["tag1", "tag2"]) },
          { field: "key2", value: JSON.stringify(["tag3"]) },
          { field: "key3", value: JSON.stringify(["tag1"]) },
        ],
      });

      await handler.updateTags(["tag1"]);

      // Should unlink keys matching tag1 (key1 and key3, with prefix)
      expect(mockClient.unlink).toHaveBeenCalledWith(["key1", "key3"]);
      // Should delete tag entries for matching keys
      expect(mockClient.hDel).toHaveBeenCalledWith("__cc_sharedTags__", [
        "key1",
        "key3",
      ]);
    });

    it("does not delete when no entries match tags", async () => {
      const handler = createHandler({ client: mockClient as never });

      mockClient.hScan.mockResolvedValue({
        cursor: "0",
        entries: [{ field: "key1", value: JSON.stringify(["other-tag"]) }],
      });

      await handler.updateTags(["tag1"]);

      expect(mockClient.unlink).not.toHaveBeenCalled();
      expect(mockClient.hDel).not.toHaveBeenCalled();
    });

    it("handles multi-page hScan cursor pagination", async () => {
      const handler = createHandler({ client: mockClient as never });

      // First page returns cursor "42", second page returns "0" (done)
      mockClient.hScan
        .mockResolvedValueOnce({
          cursor: "42",
          entries: [{ field: "key1", value: JSON.stringify(["tag1"]) }],
        })
        .mockResolvedValueOnce({
          cursor: "0",
          entries: [{ field: "key2", value: JSON.stringify(["tag1"]) }],
        });

      await handler.updateTags(["tag1"]);

      expect(mockClient.hScan).toHaveBeenCalledTimes(2);
      expect(mockClient.unlink).toHaveBeenCalledWith(["key1", "key2"]);
    });

    it("applies keyPrefix to deleted cache keys but not tag fields", async () => {
      const handler = createHandler({
        client: mockClient as never,
        keyPrefix: "myapp:",
      });

      mockClient.hScan.mockResolvedValue({
        cursor: "0",
        entries: [{ field: "key1", value: JSON.stringify(["tag1"]) }],
      });

      await handler.updateTags(["tag1"]);

      // Cache keys get prefix, but hDel field names don't
      expect(mockClient.unlink).toHaveBeenCalledWith(["myapp:key1"]);
      expect(mockClient.hDel).toHaveBeenCalledWith("myapp:__cc_sharedTags__", [
        "key1",
      ]);
    });
  });
});
