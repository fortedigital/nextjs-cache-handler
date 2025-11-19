import type { RedisClientType } from "@redis/client";
import { RedisClusterCacheAdapter } from "../helpers/redisClusterAdapter";

export type RedisCompliantCachedRouteValue = {
  // See: https://github.com/vercel/next.js/blob/f5444a16ec2ef7b82d30048890b613aa3865c1f1/packages/next/src/server/response-cache/types.ts#L97
  kind: "APP_ROUTE";
  body: string | undefined;
};

export type RedisCompliantCachedAppPageValue = {
  // See: https://github.com/vercel/next.js/blob/f5444a16ec2ef7b82d30048890b613aa3865c1f1/packages/next/src/server/response-cache/types.ts#L76
  kind: "APP_PAGE";
  rscData: string | undefined;
  segmentData: Record<string, string> | undefined;
};

export type CreateRedisStringsHandlerOptions<
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
   * Optional. The number of tags in a single query retrieved from Redis when scanning or searching for tags.
   *
   * @default 10_000 // 10,000 tags
   *
   * @remarks
   * You can adjust this value to optimize the number of commands sent to Redis when scanning or searching for tags.
   * A higher value will reduce the number of commands sent to Redis,
   * but it will also increase the amount of data transferred over the network.
   * Redis uses TCP and typically has 65,535 bytes as the maximum size of a packet (it can be lower depending on MTU).
   */
  revalidateTagQuerySize?: number;

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
