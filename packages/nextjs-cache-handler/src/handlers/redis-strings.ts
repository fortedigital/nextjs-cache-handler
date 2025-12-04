import { REVALIDATED_TAGS_KEY } from "../constants";
import { isImplicitTag } from "../helpers/isImplicitTag";
import { CacheHandlerValue, Handler } from "./cache-handler.types";
import { CreateRedisStringsHandlerOptions } from "./redis-strings.types";
import {
  convertStringsToBuffers,
  parseBuffersToStrings,
} from "../helpers/buffer";
import type { RedisClientType } from "@redis/client";
import { RedisClusterCacheAdapter } from "../helpers/redisClusterAdapter";
import { withAbortSignalProxy } from "../helpers/withAbortSignalProxy";

/**
 * Creates a Handler for handling cache operations using Redis strings.
 *
 * This function initializes a Handler for managing cache operations using Redis.
 * It supports Redis Client and includes methods for on-demand revalidation of cache values.
 *
 * @param options - The configuration options for the Redis Handler. See {@link CreateRedisStringsHandlerOptions}.
 *
 * @returns An object representing the Redis-based cache handler, with methods for cache operations.
 *
 * @remarks
 * - The `get` method retrieves a value from the cache, automatically converting `Buffer` types when necessary.
 * - The `set` method stores a value in the cache, using the configured expiration strategy.
 * - The `revalidateTag` and `delete` methods handle cache revalidation and deletion.
 */
export default function createHandler({
  client: innerClient,
  keyPrefix = "",
  sharedTagsKey = "__sharedTags__",
  sharedTagsTtlKey = "__sharedTagsTtl__",
  timeoutMs = 5_000,
  keyExpirationStrategy = "EXPIREAT",
  revalidateTagQuerySize = 10_000,
}: CreateRedisStringsHandlerOptions<
  RedisClientType | RedisClusterCacheAdapter
>): Handler {
  const client = withAbortSignalProxy(innerClient);
  const revalidatedTagsKey = keyPrefix + REVALIDATED_TAGS_KEY;

  function assertClientIsReady(): void {
    if (!client.isReady) {
      throw new Error(
        "Redis client is not ready yet or connection is lost. Keep trying...",
      );
    }
  }

  async function revalidateTag(tag: string) {
    assertClientIsReady();

    if (isImplicitTag(tag)) {
      await client
        .withAbortSignal(AbortSignal.timeout(timeoutMs))
        .hSet(revalidatedTagsKey, tag, Date.now());
    }

    const tagsMap: Map<string, string[]> = new Map();

    let cursor = "0";

    const hScanOptions = { COUNT: revalidateTagQuerySize };

    do {
      const remoteTagsPortion = await client.hScan(
        keyPrefix + sharedTagsKey,
        cursor,
        hScanOptions,
      );

      for (const { field, value } of remoteTagsPortion.entries) {
        tagsMap.set(field, JSON.parse(value));
      }

      cursor = remoteTagsPortion.cursor;
    } while (cursor !== "0");

    const keysToDelete: string[] = [];
    const tagsToDelete: string[] = [];

    for (const [key, tags] of tagsMap) {
      if (tags.includes(tag)) {
        keysToDelete.push(keyPrefix + key);
        tagsToDelete.push(key);
      }
    }

    if (keysToDelete.length === 0) {
      return;
    }

    const deleteKeysOperation = client
      .withAbortSignal(AbortSignal.timeout(timeoutMs))
      .unlink(keysToDelete);

    const updateTagsOperation = client
      .withAbortSignal(AbortSignal.timeout(timeoutMs))
      .hDel(keyPrefix + sharedTagsKey, tagsToDelete);

    const updateTtlOperation = client
      .withAbortSignal(AbortSignal.timeout(timeoutMs))
      .hDel(keyPrefix + sharedTagsTtlKey, tagsToDelete);

    await Promise.all([
      deleteKeysOperation,
      updateTtlOperation,
      updateTagsOperation,
    ]);
  }

  async function revalidateSharedKeys() {
    assertClientIsReady();

    const ttlMap = new Map();

    let cursor = "0";

    const hScanOptions = { COUNT: revalidateTagQuerySize };

    do {
      const remoteTagsPortion = await client
        .withAbortSignal(AbortSignal.timeout(timeoutMs))
        .hScan(keyPrefix + sharedTagsTtlKey, cursor, hScanOptions);

      for (const { field, value } of remoteTagsPortion.entries) {
        ttlMap.set(field, Number(value));
      }

      cursor = remoteTagsPortion.cursor;
    } while (cursor !== "0");

    const tagsAndTtlToDelete: string[] = [];
    const keysToDelete: string[] = [];

    for (const [key, ttlInSeconds] of ttlMap) {
      if (new Date().getTime() > ttlInSeconds * 1000) {
        tagsAndTtlToDelete.push(key);
        keysToDelete.push(keyPrefix + key);
      }
    }

    if (tagsAndTtlToDelete.length === 0) {
      return;
    }

    const deleteKeysOperation = client
      .withAbortSignal(AbortSignal.timeout(timeoutMs))
      .unlink(keysToDelete);

    const updateTtlOperation = client
      .withAbortSignal(AbortSignal.timeout(timeoutMs))
      .hDel(keyPrefix + sharedTagsTtlKey, tagsAndTtlToDelete);

    const updateTagsOperation = client
      .withAbortSignal(AbortSignal.timeout(timeoutMs))
      .hDel(keyPrefix + sharedTagsKey, tagsAndTtlToDelete);

    await Promise.all([
      deleteKeysOperation,
      updateTagsOperation,
      updateTtlOperation,
    ]);
  }

  return {
    name: "redis-strings",
    async get(key, { implicitTags }) {
      assertClientIsReady();

      const result = await client
        .withAbortSignal(AbortSignal.timeout(timeoutMs))
        .get(keyPrefix + key);

      if (!result) {
        return null;
      }

      const cacheValue = JSON.parse(result) as CacheHandlerValue | null;

      if (!cacheValue) {
        return null;
      }

      convertStringsToBuffers(cacheValue);

      const sharedTagKeyExists = await client
        .withAbortSignal(AbortSignal.timeout(timeoutMs))
        .hExists(keyPrefix + sharedTagsKey, key);

      if (!sharedTagKeyExists) {
        await client
          .withAbortSignal(AbortSignal.timeout(timeoutMs))
          .unlink(keyPrefix + key);

        return null;
      }

      const combinedTags = new Set([...cacheValue.tags, ...implicitTags]);

      if (combinedTags.size === 0) {
        return cacheValue;
      }

      const revalidationTimes = await client
        .withAbortSignal(AbortSignal.timeout(timeoutMs))
        .hmGet(revalidatedTagsKey, Array.from(combinedTags));

      for (const timeString of revalidationTimes) {
        if (
          timeString &&
          Number.parseInt(timeString, 10) > cacheValue.lastModified
        ) {
          await client
            .withAbortSignal(AbortSignal.timeout(timeoutMs))
            .unlink(keyPrefix + key);

          return null;
        }
      }

      return cacheValue;
    },
    async set(key, cacheHandlerValue) {
      assertClientIsReady();

      let setOperation: Promise<string | null>;
      let expireOperation: Promise<number> | undefined;
      const lifespan = cacheHandlerValue.lifespan;

      // Clone only the value object to avoid mutating Next.js's original
      const valueForStorage = cacheHandlerValue.value
        ? { ...cacheHandlerValue.value }
        : null;

      if (valueForStorage) {
        parseBuffersToStrings({ ...cacheHandlerValue, value: valueForStorage });
      }

      const setTagsOperation = client
        .withAbortSignal(AbortSignal.timeout(timeoutMs))
        .hSet(
          keyPrefix + sharedTagsKey,
          key,
          JSON.stringify(cacheHandlerValue.tags ?? []),
        );

      const setSharedTtlOperation = lifespan
        ? client
            .withAbortSignal(AbortSignal.timeout(timeoutMs))
            .hSet(keyPrefix + sharedTagsTtlKey, key, lifespan.expireAt)
        : undefined;

      await Promise.all([setTagsOperation, setSharedTtlOperation]);

      const serializedValue = JSON.stringify({
        ...cacheHandlerValue,
        value: valueForStorage,
      });

      switch (keyExpirationStrategy) {
        case "EXAT": {
          setOperation = client
            .withAbortSignal(AbortSignal.timeout(timeoutMs))
            .set(
              keyPrefix + key,
              serializedValue,
              typeof lifespan?.expireAt === "number"
                ? {
                    EXAT: lifespan.expireAt,
                  }
                : undefined,
            );
          break;
        }
        case "EXPIREAT": {
          setOperation = client
            .withAbortSignal(AbortSignal.timeout(timeoutMs))
            .set(keyPrefix + key, serializedValue);

          expireOperation = lifespan
            ? client
                .withAbortSignal(AbortSignal.timeout(timeoutMs))
                .expireAt(keyPrefix + key, lifespan.expireAt)
            : undefined;
          break;
        }
        default: {
          throw new Error(
            `Invalid keyExpirationStrategy: ${keyExpirationStrategy}`,
          );
        }
      }

      await Promise.all([setOperation, expireOperation]);
    },
    async revalidateTag(tag) {
      assertClientIsReady();

      /*
       * If the tag is an implicit tag, we need to mark it as revalidated.
       * The revalidation process is done by the CacheHandler class on the next get operation.
       */
      if (isImplicitTag(tag)) {
        await client
          .withAbortSignal(AbortSignal.timeout(timeoutMs))
          .hSet(revalidatedTagsKey, tag, Date.now());
      }

      await Promise.all([revalidateTag(tag), revalidateSharedKeys()]);
    },
    async delete(key) {
      await client
        .withAbortSignal(AbortSignal.timeout(timeoutMs))
        .unlink(keyPrefix + key);

      await Promise.all([
        client
          .withAbortSignal(AbortSignal.timeout(timeoutMs))
          .hDel(keyPrefix + sharedTagsKey, key),
        client
          .withAbortSignal(AbortSignal.timeout(timeoutMs))
          .hDel(keyPrefix + sharedTagsTtlKey, key),
      ]);
    },
  };
}
