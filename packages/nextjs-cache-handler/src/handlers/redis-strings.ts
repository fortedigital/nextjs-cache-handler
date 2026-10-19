import { REVALIDATED_TAGS_KEY } from "../constants";
import { isImplicitTag } from "../helpers/isImplicitTag";
import { CacheHandlerValue, Handler } from "./cache-handler.types";
import {
  type CacheValueSerializer,
  type CreateRedisStringsHandlerOptions,
} from "./redis-strings.types";
import {
  convertStringsToBuffers,
  parseBuffersToStrings,
} from "../helpers/buffer";
import type { RedisClientType } from "@redis/client";
import { RedisClusterCacheAdapter } from "../helpers/redisClusterAdapter";
import { withAbortSignalProxy } from "../helpers/withAbortSignalProxy";

export type { CacheValueSerializer } from "./redis-strings.types";

export const jsonCacheValueSerializer: CacheValueSerializer = {
  serialize(value) {
    return JSON.stringify(value);
  },
  deserialize(stored) {
    return JSON.parse(
      typeof stored === "string" ? stored : String(stored),
    ) as CacheHandlerValue | null;
  },
};

const HSET_WITH_EXPIRATION_SCRIPT = `
local result = redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return result
`;

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
  valueSerializer = jsonCacheValueSerializer,
  metadataKeysExpirationSeconds,
}: CreateRedisStringsHandlerOptions<
  RedisClientType | RedisClusterCacheAdapter
>): Handler {
  const client =
    "withAbortSignal" in innerClient &&
    typeof innerClient.withAbortSignal === "function"
      ? innerClient
      : withAbortSignalProxy(innerClient);
  const revalidatedTagsKey = keyPrefix + REVALIDATED_TAGS_KEY;

  if (
    metadataKeysExpirationSeconds !== undefined &&
    (!Number.isSafeInteger(metadataKeysExpirationSeconds) ||
      metadataKeysExpirationSeconds <= 0)
  ) {
    throw new Error("metadataKeysExpirationSeconds must be a positive integer");
  }

  function assertClientIsReady(): void {
    if (!client.isReady) {
      throw new Error(
        "Redis client is not ready yet or connection is lost. Keep trying...",
      );
    }
  }

  function setMetadataField(
    key: string,
    field: string,
    value: string | number,
  ) {
    const commandClient = client.withAbortSignal(
      AbortSignal.timeout(timeoutMs),
    );

    if (metadataKeysExpirationSeconds === undefined) {
      return commandClient.hSet(key, field, value);
    }

    return commandClient.eval(HSET_WITH_EXPIRATION_SCRIPT, {
      keys: [key],
      arguments: [field, String(value), String(metadataKeysExpirationSeconds)],
    });
  }

  async function revalidateTag(tag: string) {
    assertClientIsReady();

    if (isImplicitTag(tag)) {
      await setMetadataField(revalidatedTagsKey, tag, Date.now());
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

      const cacheValue = await valueSerializer.deserialize(result);

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
    async set(key, cacheHandlerValue, ctx) {
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

      const serializedValue = await valueSerializer.serialize({
        ...cacheHandlerValue,
        value: valueForStorage,
      });

      const setTagsOperation = setMetadataField(
        keyPrefix + sharedTagsKey,
        key,
        JSON.stringify(cacheHandlerValue.tags ?? []),
      );

      const setSharedTtlOperation = lifespan
        ? setMetadataField(keyPrefix + sharedTagsTtlKey, key, lifespan.expireAt)
        : undefined;

      switch (keyExpirationStrategy) {
        case "EXAT": {
          const hasExpireAt = typeof lifespan?.expireAt === "number";
          const isNX = ctx?.setOnlyIfNotExists === true;

          const setOptions =
            hasExpireAt || isNX
              ? {
                  ...(hasExpireAt && { EXAT: lifespan.expireAt }),
                  ...(isNX && { NX: true }),
                }
              : undefined;

          setOperation = client
            .withAbortSignal(AbortSignal.timeout(timeoutMs))
            .set(keyPrefix + key, serializedValue, setOptions);
          break;
        }
        case "EXPIREAT": {
          const setOptions = ctx?.setOnlyIfNotExists ? { NX: true } : undefined;

          setOperation = client
            .withAbortSignal(AbortSignal.timeout(timeoutMs))
            .set(keyPrefix + key, serializedValue, setOptions);

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

      await Promise.all(
        [
          setTagsOperation,
          setSharedTtlOperation,
          setOperation,
          expireOperation,
        ].filter(Boolean),
      );
    },
    async revalidateTag(tag) {
      assertClientIsReady();

      /*
       * Preserve the existing two-write implicit-tag path when metadata key
       * expiration is disabled. The private revalidation helper performs the
       * second write before scanning the shared tag map.
       */
      if (metadataKeysExpirationSeconds === undefined && isImplicitTag(tag)) {
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
    async prepare() {
      await revalidateSharedKeys();
    },
  };
}
