import type { CacheHandlerValue, Handler } from "@neshca/cache-handler";
import {
  getTimeoutRedisCommandOptions,
  isImplicitTag,
} from "@neshca/cache-handler/helpers";
import type { CreateRedisStackHandlerOptions } from "@neshca/cache-handler/redis-stack";

import { REVALIDATED_TAGS_KEY } from "../constants";

export type CreateRedisStringsHandlerOptions =
  CreateRedisStackHandlerOptions & {
    /**
     * Key for storing cache tags.
     *
     * @default '__sharedTags__'
     */
    sharedTagsKey?: string;
    /**
     * Key for storing cache tags TTL.
     *
     * @default '__sharedTagsTtl__'
     */
    sharedTagsTtlKey?: string;
    /**
     * Determines the expiration strategy for cache keys.
     *
     * - `'EXAT'`: Uses the `EXAT` option of the `SET` command to set expiration time.
     * - `'EXPIREAT'`: Uses the `EXPIREAT` command to set expiration time.
     *
     * By default, it uses `'EXPIREAT'` for compatibility with older versions.
     *
     * @default 'EXPIREAT'
     */
    keyExpirationStrategy?: "EXAT" | "EXPIREAT";
  };

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
  client,
  keyPrefix = "",
  sharedTagsKey = "__sharedTags__",
  sharedTagsTtlKey = "__sharedTagsTtl__",
  timeoutMs = 5_000,
  keyExpirationStrategy = "EXPIREAT",
  revalidateTagQuerySize = 10_000,
}: CreateRedisStringsHandlerOptions): Handler {
  function assertClientIsReady(): void {
    if (!client.isReady) {
      throw new Error(
        "Redis client is not ready yet or connection is lost. Keep trying...",
      );
    }
  }

  async function revalidateTags(tag: string) {
    const tagsMap: Map<string, string[]> = new Map();

    let cursor = 0;

    const hScanOptions = { COUNT: revalidateTagQuerySize };

    do {
      const remoteTagsPortion = await client.hScan(
        getTimeoutRedisCommandOptions(timeoutMs),
        keyPrefix + sharedTagsKey,
        cursor,
        hScanOptions,
      );

      for (const { field, value } of remoteTagsPortion.tuples) {
        tagsMap.set(field, JSON.parse(value));
      }

      cursor = remoteTagsPortion.cursor;
    } while (cursor !== 0);

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

    const deleteKeysOperation = client.unlink(
      getTimeoutRedisCommandOptions(timeoutMs),
      keysToDelete,
    );

    const updateTagsOperation = client.hDel(
      { isolated: true, ...getTimeoutRedisCommandOptions(timeoutMs) },
      keyPrefix + sharedTagsKey,
      tagsToDelete,
    );

    const updateTtlOperation = client.hDel(
      { isolated: true, ...getTimeoutRedisCommandOptions(timeoutMs) },
      keyPrefix + sharedTagsTtlKey,
      tagsToDelete,
    );

    await Promise.all([
      deleteKeysOperation,
      updateTtlOperation,
      updateTagsOperation,
    ]);
  }

  async function revalidateSharedKeys() {
    const ttlMap = new Map();

    let cursor = 0;

    const hScanOptions = { COUNT: revalidateTagQuerySize };

    do {
      const remoteTagsPortion = await client.hScan(
        getTimeoutRedisCommandOptions(timeoutMs),
        keyPrefix + sharedTagsTtlKey,
        cursor,
        hScanOptions,
      );

      for (const { field, value } of remoteTagsPortion.tuples) {
        ttlMap.set(field, Number(value));
      }

      cursor = remoteTagsPortion.cursor;
    } while (cursor !== 0);

    const tagsAndTtlToDelete = [];
    const keysToDelete = [];

    for (const [key, ttlInSeconds] of ttlMap) {
      if (new Date().getTime() > ttlInSeconds * 1000) {
        tagsAndTtlToDelete.push(key);
        keysToDelete.push(keyPrefix + key);
      }
    }

    if (tagsAndTtlToDelete.length === 0) {
      return;
    }

    const updateTtlOperation = client.hDel(
      {
        isolated: true,
        ...getTimeoutRedisCommandOptions(timeoutMs),
      },
      keyPrefix + sharedTagsTtlKey,
      tagsAndTtlToDelete,
    );

    const updateTagsOperation = client.hDel(
      {
        isolated: true,
        ...getTimeoutRedisCommandOptions(timeoutMs),
      },
      keyPrefix + sharedTagsKey,
      tagsAndTtlToDelete,
    );

    const deleteKeysOperation = client.unlink(
      getTimeoutRedisCommandOptions(timeoutMs),
      keysToDelete,
    );

    await Promise.all([
      deleteKeysOperation,
      updateTagsOperation,
      updateTtlOperation,
    ]);
  }

  const revalidatedTagsKey = keyPrefix + REVALIDATED_TAGS_KEY;

  return {
    name: "apopro-redis-strings",
    async get(key, { implicitTags }) {
      assertClientIsReady();

      const result = await client.get(
        getTimeoutRedisCommandOptions(timeoutMs),
        keyPrefix + key,
      );

      if (!result) {
        return null;
      }

      const cacheValue = JSON.parse(result) as CacheHandlerValue | null;

      if (!cacheValue) {
        return null;
      }

      const combinedTags = new Set([...cacheValue.tags, ...implicitTags]);

      if (combinedTags.size === 0) {
        return cacheValue;
      }

      const revalidationTimes = await client.hmGet(
        getTimeoutRedisCommandOptions(timeoutMs),
        revalidatedTagsKey,
        Array.from(combinedTags),
      );

      for (const timeString of revalidationTimes) {
        if (
          timeString &&
          Number.parseInt(timeString, 10) > cacheValue.lastModified
        ) {
          await client.unlink(
            getTimeoutRedisCommandOptions(timeoutMs),
            keyPrefix + key,
          );

          return null;
        }
      }

      return cacheValue;
    },
    async set(key, cacheHandlerValue) {
      assertClientIsReady();

      const options = getTimeoutRedisCommandOptions(timeoutMs);

      let setOperation: Promise<string | null>;
      let expireOperation: Promise<boolean> | undefined;
      const lifespan = cacheHandlerValue.lifespan;

      switch (keyExpirationStrategy) {
        case "EXAT": {
          setOperation = client.set(
            options,
            keyPrefix + key,
            JSON.stringify(cacheHandlerValue),
            typeof lifespan?.expireAt === "number"
              ? {
                  EXAT: lifespan.expireAt,
                }
              : undefined,
          );
          break;
        }
        case "EXPIREAT": {
          setOperation = client.set(
            options,
            keyPrefix + key,
            JSON.stringify(cacheHandlerValue),
          );

          expireOperation = lifespan
            ? client.expireAt(options, keyPrefix + key, lifespan.expireAt)
            : undefined;
          break;
        }
        default: {
          throw new Error(
            `Invalid keyExpirationStrategy: ${keyExpirationStrategy}`,
          );
        }
      }

      const setTagsOperation =
        cacheHandlerValue.tags.length > 0
          ? client.hSet(
              options,
              keyPrefix + sharedTagsKey,
              key,
              JSON.stringify(cacheHandlerValue.tags),
            )
          : undefined;

      const setSharedTtlOperation = lifespan
        ? client.hSet(
            options,
            keyPrefix + sharedTagsTtlKey,
            key,
            lifespan.expireAt,
          )
        : undefined;

      await Promise.all([
        setOperation,
        expireOperation,
        setTagsOperation,
        setSharedTtlOperation,
      ]);
    },
    async revalidateTag(tag) {
      assertClientIsReady();

      /*
       * If the tag is an implicit tag, we need to mark it as revalidated.
       * The revalidation process is done by the CacheHandler class on the next get operation.
       */
      if (isImplicitTag(tag)) {
        await client.hSet(
          getTimeoutRedisCommandOptions(timeoutMs),
          revalidatedTagsKey,
          tag,
          Date.now(),
        );
      }

      await Promise.all([revalidateTags(tag), revalidateSharedKeys()]);
    },
    async delete(key) {
      await Promise.all([
        client.unlink(
          getTimeoutRedisCommandOptions(timeoutMs),
          keyPrefix + key,
        ),
        client.hDel(keyPrefix + sharedTagsKey, key),
        client.hDel(keyPrefix + sharedTagsTtlKey, key),
      ]);
    },
  };
}
