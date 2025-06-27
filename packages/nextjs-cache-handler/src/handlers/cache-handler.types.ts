import type {
  CacheHandler as NextCacheHandler,
  CacheHandlerValue as NextCacheHandlerValue,
} from "next/dist/server/lib/incremental-cache";

import type FileSystemCache from "next/dist/server/lib/incremental-cache/file-system-cache";

/**
 * A set of time periods and timestamps for controlling cache behavior.
 */
export type LifespanParameters = {
  /**
   * The Unix timestamp (in seconds) for when the cache entry was last modified.
   */
  readonly lastModifiedAt: number;
  /**
   * The Unix timestamp (in seconds) for when the cache entry entry becomes stale.
   * After this time, the entry is considered staled and may be used.
   */
  readonly staleAt: number;
  /**
   * The Unix timestamp (in seconds) for when the cache entry must be removed from the cache.
   * After this time, the entry is considered expired and should not be used.
   */
  readonly expireAt: number;
  /**
   * Time in seconds before the cache entry becomes stale.
   */
  readonly staleAge: number;
  /**
   * Time in seconds before the cache entry becomes expired.
   */
  readonly expireAge: number;
  /**
   * Value from Next.js revalidate option. May be false if the page has no revalidate option or the revalidate option is set to false.
   */
  readonly revalidate: Revalidate | undefined;
};

export type Revalidate = false | number;

export type CacheHandlerParametersGet = Parameters<NextCacheHandler["get"]>;

export type CacheHandlerParametersGetWithTags = [
  ...CacheHandlerParametersGet,
  string[],
];

/**
 * Represents an internal Next.js metadata for a `get` method.
 * This metadata is available in the `get` method of the cache handler.
 */
export type HandlerGetMeta = {
  /**
   * An array of tags that are implicitly associated with the cache entry.
   */
  implicitTags: string[];
};
/**
 * Represents a cache Handler.
 */
export type Handler = {
  /**
   * A descriptive name for the cache Handler.
   */
  name: string;
  /**
   * Retrieves the value associated with the given key from the cache.
   *
   * @param key - The unique string identifier for the cache entry.
   *
   * @param meta - The metadata object for the `get` method. See {@link HandlerGetMeta}.
   *
   * @returns A Promise that resolves to the cached value (if found), `null` or `undefined` if the entry is not found.
   *
   * @example
   * ### With auto expiration
   *
   * If your cache store supports time based key eviction, the `get` method is straightforward.
   *
   * ```js
   *  const handler = {
   *    async get(key) {
   *      const cacheValue = await cacheStore.get(key);
   *
   *      if (!cacheValue) {
   *          return null;
   *      }
   *
   *      return cacheValue;
   *   }
   * }
   * ```
   *
   * ### Without auto expiration
   *
   * If your cache store does not support time based key eviction,
   * you can implement the `delete` method to remove the cache entry when it becomes expired.
   *
   * ```js
   *  const handler = {
   *    async get(key) {
   *      const cacheValue = await cacheStore.get(key);
   *
   *      if (!cacheValue) {
   *          return null;
   *      }
   *
   *      return cacheValue;
   *    },
   *    async delete(key) {
   *      await cacheStore.delete(key);
   *    }
   * }
   * ```
   */
  get: (
    key: string,
    meta: HandlerGetMeta,
  ) => Promise<CacheHandlerValue | null | undefined>;
  /**
   * Sets or updates a value in the cache store.
   *
   * @param key - The unique string identifier for the cache entry.
   *
   * @param value - The value to be stored in the cache. See {@link CacheHandlerValue}.
   *
   * @returns A Promise that resolves when the value has been successfully set in the cache.
   *
   * @remarks
   *
   * Read more about the `lifespan` parameter: {@link LifespanParameters}.
   *
   * ### LifespanParameters
   * If no `revalidate` option or `revalidate: false` is set in your [`getStaticProps` ↗](https://nextjs.org/docs/pages/api-reference/functions/get-static-props#revalidate)
   * the `lifespan` parameter will be `null` and you should consider the cache entry as always fresh and never stale.
   *
   * Use the absolute time (`expireAt`) to set and expiration time for the cache entry in your cache store to be in sync with the file system cache.
   */
  set: (key: string, value: CacheHandlerValue) => Promise<void>;
  /**
   * Deletes all cache entries that are associated with the specified tag.
   * See [fetch `options.next.tags` and `revalidateTag` ↗](https://nextjs.org/docs/app/building-your-application/caching#fetch-optionsnexttags-and-revalidatetag)
   *
   * @param tag - A string representing the cache tag associated with the data you want to revalidate.
   * Must be less than or equal to 256 characters. This value is case-sensitive.
   */
  revalidateTag: (tag: string) => Promise<void>;
  /**
   * Deletes the cache entry associated with the given key from the cache store.
   * This method is optional and supposed to be used only when the cache store does not support time based key eviction.
   * This method will be automatically called by the `CacheHandler` class when the retrieved cache entry is stale.
   *
   * @param key - The unique string identifier for the cache entry with prefix if present.
   *
   * @returns A Promise that resolves when the cache entry has been successfully deleted.
   */
  delete?: (key: string) => Promise<void>;
};
/**
 * Represents the parameters for Time-to-Live (TTL) configuration.
 */
export type TTLParameters = {
  /**
   * The time in seconds for when the cache entry becomes stale.
   *
   * @default 31536000 // 1 year
   */
  defaultStaleAge: number;
  /**
   * Estimates the expiration age based on the stale age.
   *
   * @param staleAge - The stale age in seconds.
   * After the stale age, the cache entry is considered stale, can be served from the cache, and should be revalidated.
   * Revalidation is handled by the `CacheHandler` class.
   *
   * @default (staleAge) => staleAge * 1.5
   *
   * @returns The expiration age in seconds.
   */
  estimateExpireAge(staleAge: number): number;
};
/**
 * Configuration options for the {@link CacheHandler}.
 */
export type CacheHandlerConfig = {
  /**
   * An array of cache instances that conform to the Handler interface.
   * Multiple caches can be used to implement various caching strategies or layers.
   */
  handlers: (Handler | undefined | null)[];
  /**
   * Time-to-live (TTL) options for the cache entries.
   */
  ttl?: Partial<TTLParameters>;
};
/**
 * Contextual information provided during cache creation, including server directory paths and environment mode.
 */
export type CacheCreationContext = {
  /**
   * The absolute path to the Next.js server directory.
   */
  serverDistDir: string;
  /**
   * Indicates if the Next.js application is running in development mode.
   * When in development mode, caching behavior might differ.
   */
  dev?: boolean;
  /**
   * The unique identifier for the current build of the Next.js application.
   * This build ID is generated during the `next build` process and is used
   * to ensure consistency across multiple instances of the application,
   * especially when running in containerized environments. It helps in
   * identifying which version of the application is being served.
   *
   * https://nextjs.org/docs/pages/building-your-application/deploying#build-cache
   *
   * @example
   * ```js
   * // cache-handler.mjs
   * CacheHandler.onCreation(async ({ buildId }) => {
   *   let redisHandler;
   *
   *   if (buildId) {
   *     await client.connect();
   *
   *     redisHandler = await createRedisHandler({
   *       client,
   *       keyPrefix: `${buildId}:`,
   *     });
   *   }
   *
   *   const localHandler = createLruHandler();
   *
   *   return {
   *     handlers: [redisHandler, localHandler],
   *   };
   * });
   * ```
   */
  buildId?: string;
};
/**
 * Represents a hook function that is called during the build and on start of the application.
 *
 * @param context - The {@link CacheCreationContext} object, providing contextual information about the cache creation environment,
 * such as server directory paths and whether the application is running in development mode.
 *
 * @returns Either a {@link CacheHandlerConfig} object or a Promise that resolves to a {@link CacheHandlerConfig},
 * specifying how the cache should be configured.
 */
export type OnCreationHook = (
  context: CacheCreationContext,
) => Promise<CacheHandlerConfig> | CacheHandlerConfig;
declare class CacheHandler implements NextCacheHandler {
  #private;
  /**
   * Provides a descriptive name for the CacheHandler class.
   *
   * The name includes the number of handlers and whether file system caching is used.
   * If the cache handler is not configured yet, it will return a string indicating so.
   *
   * This property is primarily intended for debugging purposes
   * and its visibility is controlled by the `NEXT_PRIVATE_DEBUG_CACHE` environment variable.
   *
   * @returns A string describing the cache handler configuration.
   *
   * @example
   * ```js
   * // cache-handler.mjs
   * CacheHandler.onCreation(async () => {
   *   const redisHandler = await createRedisHandler({
   *    client,
   *   });
   *
   *   const localHandler = createLruHandler();
   *
   *   return {
   *     handlers: [redisHandler, localHandler],
   *   };
   * });
   *
   * // after the Next.js called the onCreation hook
   * console.log(CacheHandler.name);
   * // Output: "cache-handler with 2 Handlers"
   * ```
   */
  static get name(): string;
  /**
   * Registers a hook to be called during the creation of an CacheHandler instance.
   * This method allows for custom cache configurations to be applied at the time of cache instantiation.
   *
   * The provided {@link OnCreationHook} function can perform initialization tasks, modify cache settings,
   * or integrate additional logic into the cache creation process. This function can either return a {@link CacheHandlerConfig}
   * object directly for synchronous operations, or a `Promise` that resolves to a {@link CacheHandlerConfig} for asynchronous operations.
   *
   * Usage of this method is typically for advanced scenarios where default caching behavior needs to be altered
   * or extended based on specific application requirements or environmental conditions.
   *
   * @param onCreationHook - The {@link OnCreationHook} function to be called during cache creation.
   */
  static onCreation(onCreationHook: OnCreationHook): void;
  /**
   * Creates a new CacheHandler instance. Constructor is intended for internal use only.
   */
  constructor(context: FileSystemCacheContext);
  get(
    cacheKey: CacheHandlerParametersGet[0],
    ctx?: CacheHandlerParametersGet[1],
  ): Promise<CacheHandlerValue | null>;
  set(
    cacheKey: CacheHandlerParametersSet[0],
    incrementalCacheValue: CacheHandlerParametersSet[1],
    ctx: CacheHandlerParametersSet[2] & {
      internal_lastModified?: number;
    },
  ): Promise<void>;
  revalidateTag(tag: CacheHandlerParametersRevalidateTag[0]): Promise<void>;
  resetRequestCache(): void;
}
export type CacheHandlerParametersSet = Parameters<NextCacheHandler["set"]>;

export type CacheHandlerParametersRevalidateTag = Parameters<
  NextCacheHandler["revalidateTag"]
>;

export type FileSystemCacheContext = ConstructorParameters<
  typeof FileSystemCache
>[0];

export type CacheHandlerValue = NextCacheHandlerValue & {
  /**
   * Timestamp in milliseconds when the cache entry was last modified.
   */
  lastModified: number;
  /**
   * Tags associated with the cache entry. They are used for on-demand revalidation.
   */
  tags: Readonly<string[]>;
  /**
   * The lifespan parameters for the cache entry.
   *
   * Null for pages with `fallback: false` in `getStaticPaths`.
   * Consider these pages as always fresh and never stale.
   */
  lifespan: LifespanParameters | null;
};
