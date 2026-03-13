import type {
  CacheComponentsEntry,
  CacheComponentsHandler,
} from "./cache-components-handler";
import type { RedisClientType } from "@redis/client";
import type { RedisClusterCacheAdapter } from "../helpers/redisClusterAdapter";
import { withAbortSignalProxy } from "../helpers/withAbortSignalProxy";

/**
 * Options for creating a Redis-based CacheComponentsHandler.
 */
export type CreateRedisCacheComponentsHandlerOptions<
  T = RedisClientType | RedisClusterCacheAdapter,
> = {
  /**
   * The Redis client instance.
   */
  client: T;
  /**
   * Optional. Prefix for all keys, useful for namespacing.
   *
   * @default '' // empty string
   */
  keyPrefix?: string;
  /**
   * Optional. Timeout in milliseconds for Redis operations.
   *
   * @default 5000 // 5000 ms
   *
   * @remarks
   * To disable timeout of Redis operations, set this option to 0.
   */
  timeoutMs?: number;
  /**
   * Optional. The number of tags in a single query retrieved from Redis when scanning for tags.
   *
   * @default 10_000 // 10,000 tags
   */
  revalidateTagQuerySize?: number;
  /**
   * Key for storing cache-component tag associations.
   *
   * @default '__cc_sharedTags__'
   */
  sharedTagsKey?: string;
  /**
   * Key for storing revalidated tag timestamps.
   *
   * @default '__cc_revalidatedTags__'
   */
  revalidatedTagsKey?: string;
};

type StoredCacheComponentsEntry = Omit<CacheComponentsEntry, "value"> & {
  value: string;
};

async function streamToUint8Array(
  stream: ReadableStream<Uint8Array>,
): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];

  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    if (value) {
      chunks.push(value);
    }
  }

  if (chunks.length === 1 && chunks[0]) {
    return chunks[0];
  }

  const totalLength = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return result;
}

function uint8ArrayToReadableStream(
  data: Uint8Array,
): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(data);
      controller.close();
    },
  });
}

/**
 * Creates a CacheComponentsHandler for handling Next.js 16+ `"use cache"` operations using Redis strings.
 *
 * This handler stores cache component entries (RSC Flight payloads) in Redis as base64-encoded strings,
 * with tag-based invalidation support via Redis hash maps.
 *
 * @param options - The configuration options. See {@link CreateRedisCacheComponentsHandlerOptions}.
 *
 * @returns An object implementing the CacheComponentsHandler interface.
 *
 * @remarks
 * - The `get` method retrieves and reconstructs a `ReadableStream<Uint8Array>` from the stored base64 data.
 * - The `set` method awaits the pending entry promise, buffers the stream, and stores the result in Redis.
 * - The `updateTags` method marks tags as revalidated and deletes cache entries associated with those tags.
 * - The `getExpiration` method returns the most recent revalidation timestamp across the given tags.
 * - The `refreshTags` method is a no-op since revalidation state is queried directly from Redis.
 */
export default function createHandler({
  client: innerClient,
  keyPrefix = "",
  timeoutMs = 5_000,
  revalidateTagQuerySize = 10_000,
  sharedTagsKey = "__cc_sharedTags__",
  revalidatedTagsKey = "__cc_revalidatedTags__",
}: CreateRedisCacheComponentsHandlerOptions<
  RedisClientType | RedisClusterCacheAdapter
>): CacheComponentsHandler {
  const client = withAbortSignalProxy(innerClient);

  function assertClientIsReady(): void {
    if (!client.isReady) {
      throw new Error(
        "Redis client is not ready yet or connection is lost. Keep trying...",
      );
    }
  }

  return {
    async get(cacheKey, softTags) {
      assertClientIsReady();

      const result = await client
        .withAbortSignal(AbortSignal.timeout(timeoutMs))
        .get(keyPrefix + cacheKey);

      if (!result) {
        return undefined;
      }

      const stored = JSON.parse(result) as StoredCacheComponentsEntry;

      // Check if entry has expired (expire is a duration in seconds, timestamp is in ms)
      const now = Date.now();
      if (
        Number.isFinite(stored.expire) &&
        stored.expire > 0 &&
        now > stored.timestamp + stored.expire * 1000
      ) {
        await client
          .withAbortSignal(AbortSignal.timeout(timeoutMs))
          .unlink(keyPrefix + cacheKey);

        return undefined;
      }

      // Check if any tags have been revalidated after this entry was created
      const combinedTags = [...(stored.tags ?? []), ...(softTags ?? [])];

      if (combinedTags.length > 0) {
        const revalidationTimes = await client
          .withAbortSignal(AbortSignal.timeout(timeoutMs))
          .hmGet(keyPrefix + revalidatedTagsKey, combinedTags);

        for (const timeString of revalidationTimes) {
          if (
            timeString &&
            Number.parseInt(timeString, 10) > stored.timestamp
          ) {
            await client
              .withAbortSignal(AbortSignal.timeout(timeoutMs))
              .unlink(keyPrefix + cacheKey);

            return undefined;
          }
        }
      }

      // Decode base64 value to Uint8Array and wrap in a ReadableStream
      const valueBuffer = new Uint8Array(Buffer.from(stored.value, "base64"));

      return {
        value: uint8ArrayToReadableStream(valueBuffer),
        tags: stored.tags,
        stale: stored.stale,
        timestamp: stored.timestamp,
        expire: stored.expire,
        revalidate: stored.revalidate,
      };
    },

    async set(cacheKey, pendingEntry) {
      assertClientIsReady();

      const entry = await pendingEntry;

      // Read the entire stream into a buffer for Redis storage
      const valueBuffer = await streamToUint8Array(entry.value);

      const stored: StoredCacheComponentsEntry = {
        value: Buffer.from(valueBuffer).toString("base64"),
        tags: entry.tags ?? [],
        stale: entry.stale,
        timestamp: entry.timestamp,
        expire: entry.expire,
        revalidate: entry.revalidate,
      };

      const serialized = JSON.stringify(stored);

      // Use expire duration as Redis TTL so Redis auto-evicts expired entries
      const ttlSeconds =
        Number.isFinite(stored.expire) && stored.expire > 0
          ? Math.ceil(stored.expire)
          : undefined;

      const setOperation = client
        .withAbortSignal(AbortSignal.timeout(timeoutMs))
        .set(
          keyPrefix + cacheKey,
          serialized,
          ttlSeconds ? { EX: ttlSeconds } : undefined,
        );

      const setTagsOperation =
        stored.tags.length > 0
          ? client
              .withAbortSignal(AbortSignal.timeout(timeoutMs))
              .hSet(
                keyPrefix + sharedTagsKey,
                cacheKey,
                JSON.stringify(stored.tags),
              )
          : undefined;

      await Promise.all([setOperation, setTagsOperation].filter(Boolean));
    },

    async refreshTags() {
      assertClientIsReady();
      // No-op — revalidation state is queried directly from Redis on each get/getExpiration call.
    },

    async getExpiration(tags) {
      assertClientIsReady();

      if (!tags || tags.length === 0) {
        return 0;
      }

      const revalidationTimes = await client
        .withAbortSignal(AbortSignal.timeout(timeoutMs))
        .hmGet(keyPrefix + revalidatedTagsKey, tags);

      let max = 0;
      for (const timeString of revalidationTimes) {
        if (timeString) {
          const ts = Number.parseInt(timeString, 10);
          if (ts > max) {
            max = ts;
          }
        }
      }

      return max;
    },

    async updateTags(tags) {
      assertClientIsReady();

      if (!tags || tags.length === 0) {
        return;
      }

      const now = Date.now();

      // Mark all tags as revalidated
      const revalidateFields: Record<string, string> = {};
      for (const tag of tags) {
        revalidateFields[tag] = String(now);
      }

      const markRevalidatedOperation = client
        .withAbortSignal(AbortSignal.timeout(timeoutMs))
        .hSet(keyPrefix + revalidatedTagsKey, revalidateFields);

      // Scan shared tags to find cache keys associated with the revalidated tags
      const tagsSet = new Set(tags);
      const keysToDelete: string[] = [];
      const tagKeysToDelete: string[] = [];

      let cursor = "0";
      const hScanOptions = { COUNT: revalidateTagQuerySize };

      do {
        const portion = await client
          .withAbortSignal(AbortSignal.timeout(timeoutMs))
          .hScan(keyPrefix + sharedTagsKey, cursor, hScanOptions);

        for (const { field, value } of portion.entries) {
          const entryTags: string[] = JSON.parse(value);
          if (entryTags.some((t) => tagsSet.has(t))) {
            keysToDelete.push(keyPrefix + field);
            tagKeysToDelete.push(field);
          }
        }

        cursor = portion.cursor;
      } while (cursor !== "0");

      await markRevalidatedOperation;

      if (keysToDelete.length === 0) {
        return;
      }

      const deleteKeysOperation = client
        .withAbortSignal(AbortSignal.timeout(timeoutMs))
        .unlink(keysToDelete);

      const deleteTagsOperation = client
        .withAbortSignal(AbortSignal.timeout(timeoutMs))
        .hDel(keyPrefix + sharedTagsKey, tagKeysToDelete);

      await Promise.all([deleteKeysOperation, deleteTagsOperation]);
    },
  };
}
