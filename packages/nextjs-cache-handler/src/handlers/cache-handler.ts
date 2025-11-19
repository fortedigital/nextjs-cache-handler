import { promises as fsPromises } from "node:fs";
import path from "node:path";
import type { CacheHandler as NextCacheHandler } from "next/dist/server/lib/incremental-cache";
import { createValidatedAgeEstimationFunction } from "../helpers/createValidatedAgeEstimationFunction";
import { getTagsFromHeaders } from "../helpers/getTagsFromHeaders";
import {
  CacheHandlerValue,
  FileSystemCacheContext,
  LifespanParameters,
  CacheHandlerParametersRevalidateTag,
  CacheHandlerParametersSet,
  CacheHandlerParametersGet,
  Handler,
  OnCreationHook,
  Revalidate,
} from "./cache-handler.types";
import { PrerenderManifest } from "next/dist/build";
import {
  type IncrementalCachedPageValue,
  type GetIncrementalResponseCacheContext,
  type GetIncrementalFetchCacheContext,
  IncrementalCacheValue,
  CachedFetchValue,
  SetIncrementalFetchCacheContext,
} from "next/dist/server/response-cache/types";
import { resolveRevalidateValue } from "../helpers/resolveRevalidateValue";
import { PHASE_PRODUCTION_BUILD } from "next/constants.js";

const PRERENDER_MANIFEST_VERSION = 4;

/**
 * Deletes an entry from all handlers.
 *
 * @param handlers - The list of handlers.
 * @param key - The key to delete.
 * @param debug - Whether to log debug messages.
 *
 * @returns A Promise that resolves when all handlers have finished deleting the entry.
 */
async function removeEntryFromHandlers(
  handlers: Handler[],
  key: string,
  debug: boolean,
): Promise<void> {
  if (debug) {
    console.info(
      "[CacheHandler] [method: %s] [key: %s] %s",
      "delete",
      key,
      "Started deleting entry from Handlers.",
    );
  }

  const operationsResults = await Promise.allSettled(
    handlers.map((handler) => handler.delete?.(key)),
  );

  if (!debug) {
    return;
  }

  operationsResults.forEach((handlerResult, index) => {
    if (handlerResult.status === "rejected") {
      console.warn(
        "[CacheHandler] [handler: %s] [method: %s] [key: %s] %s",
        handlers[index]?.name ?? `unknown-${index}`,
        "delete",
        key,
        `Error: ${handlerResult.reason}`,
      );
    } else {
      console.info(
        "[CacheHandler] [handler: %s] [method: %s] [key: %s] %s",
        handlers[index]?.name ?? `unknown-${index}`,
        "delete",
        key,
        "Successfully deleted value.",
      );
    }
  });
}

export class CacheHandler implements NextCacheHandler {
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
  static get name(): string {
    if (CacheHandler.#cacheListLength === undefined) {
      return "cache-handler is not configured yet";
    }

    return `cache-handler with ${CacheHandler.#cacheListLength} Handler${
      CacheHandler.#cacheListLength > 1 ? "s" : ""
    }`;
  }

  static #context: FileSystemCacheContext;

  static #mergedHandler: Omit<Handler, "name">;

  static #configureTask: Promise<void> | undefined;

  static #cacheListLength: number;

  static #debug = typeof process.env.NEXT_PRIVATE_DEBUG_CACHE !== "undefined";

  // Default stale age is 1 year in seconds
  static #defaultStaleAge = 60 * 60 * 24 * 365;

  static #estimateExpireAge: (staleAge: number) => number;

  static #fallbackFalseRoutes = new Set<string>();

  static #onCreationHook: OnCreationHook;

  static #serverDistDir: string;

  static async #readPagesRouterPage(
    cacheKey: string,
  ): Promise<CacheHandlerValue | null> {
    if (cacheKey === "/") {
      cacheKey = "/index";
    }

    let cacheHandlerValue: CacheHandlerValue | null = null;
    let pageHtmlHandle: fsPromises.FileHandle | null = null;

    if (CacheHandler.#debug) {
      console.info(
        "[CacheHandler] [handler: %s] [method: %s] [key: %s] %s",
        "file system",
        "get",
        cacheKey,
        "Started retrieving value.",
      );
    }

    try {
      const pageHtmlPath = path.join(
        CacheHandler.#serverDistDir,
        "pages",
        `${cacheKey}.html`,
      );
      const pageDataPath = path.join(
        CacheHandler.#serverDistDir,
        "pages",
        `${cacheKey}.json`,
      );

      pageHtmlHandle = await fsPromises.open(pageHtmlPath, "r");

      const [pageHtmlFile, { mtimeMs }, pageData] = await Promise.all([
        pageHtmlHandle.readFile("utf-8"),
        pageHtmlHandle.stat(),
        fsPromises
          .readFile(pageDataPath, "utf-8")
          .then((data) => JSON.parse(data) as object),
      ]);

      if (CacheHandler.#debug) {
        console.info(
          "[CacheHandler] [handler: %s] [method: %s] [key: %s] %s",
          "file system",
          "get",
          cacheKey,
          "Successfully retrieved value.",
        );
      }

      const value: IncrementalCacheValue &
        Pick<IncrementalCachedPageValue, "pageData"> = {
        kind: "PAGES" as unknown as any,
        html: pageHtmlFile,
        pageData,
        postponed: undefined,
        headers: undefined,
        status: undefined,
        rscData: undefined,
        segmentData: undefined,
      };

      cacheHandlerValue = {
        lastModified: mtimeMs,
        lifespan: null,
        tags: [],
        value: value,
      };
    } catch (error) {
      cacheHandlerValue = null;

      if (CacheHandler.#debug) {
        console.warn(
          "[CacheHandler] [handler: %s] [method: %s] [key: %s] %s",
          "file system",
          "get",
          cacheKey,
          `Error: ${error}`,
        );
      }
    } finally {
      await pageHtmlHandle?.close();
    }

    return cacheHandlerValue;
  }

  static async #writeFetch(
    cacheKey: string,
    data: CachedFetchValue,
    ctx: SetIncrementalFetchCacheContext,
  ): Promise<void> {
    try {
      const fetchDataPath = path.join(
        CacheHandler.#serverDistDir,
        "..",
        "cache",
        "fetch-cache",
        cacheKey,
      );

      await fsPromises.mkdir(path.dirname(fetchDataPath), {
        recursive: true,
      });

      await fsPromises.writeFile(
        fetchDataPath,
        JSON.stringify({
          ...data,
          tags: ctx.fetchCache ? ctx.tags : [],
        }),
      );

      if (CacheHandler.#debug) {
        console.info(
          "[CacheHandler] [handler: %s] [method: %s] [key: %s] %s",
          "file system",
          "set",
          cacheKey,
          "Successfully set value.",
        );
      }
    } catch (error) {
      if (CacheHandler.#debug) {
        console.warn(
          "[CacheHandler] [handler: %s] [method: %s] [key: %s] %s",
          "file system",
          "set",
          cacheKey,
          `Error: ${error}`,
        );
      }
    }
  }

  static async #writePagesRouterPage(
    cacheKey: string,
    pageData: IncrementalCachedPageValue,
  ): Promise<void> {
    if (cacheKey === "/") {
      cacheKey = "/index";
    }

    try {
      const pageHtmlPath = path.join(
        CacheHandler.#serverDistDir,
        "pages",
        `${cacheKey}.html`,
      );
      const pageDataPath = path.join(
        CacheHandler.#serverDistDir,
        "pages",
        `${cacheKey}.json`,
      );

      await fsPromises.mkdir(path.dirname(pageHtmlPath), {
        recursive: true,
      });

      await Promise.all([
        fsPromises.writeFile(pageHtmlPath, pageData.html),
        fsPromises.writeFile(pageDataPath, JSON.stringify(pageData.pageData)),
      ]);

      if (CacheHandler.#debug) {
        console.info(
          "[CacheHandler] [handler: %s] [method: %s] [key: %s] %s",
          "file system",
          "set",
          cacheKey,
          "Successfully set value.",
        );
      }
    } catch (error) {
      if (CacheHandler.#debug) {
        console.warn(
          "[CacheHandler] [handler: %s] [method: %s] [key: %s] %s",
          "file system",
          "set",
          cacheKey,
          `Error: ${error}`,
        );
      }
    }
  }

  /**
   * Returns the cache control parameters based on the last modified timestamp and revalidate option.
   *
   * @param lastModified - The last modified timestamp in milliseconds.
   *
   * @param revalidate - The revalidate option, representing the maximum age of stale data in seconds.
   *
   * @returns The cache control parameters including expire age, expire at, last modified at, stale age, stale at and revalidate.
   *
   * @remarks
   * - `lastModifiedAt` is the Unix timestamp (in seconds) for when the cache entry was last modified.
   * - `staleAge` is the time in seconds which equals to the `revalidate` option from Next.js pages.
   * If page has no `revalidate` option, it will be set to 1 year.
   * - `expireAge` is the time in seconds for when the cache entry becomes expired.
   * - `staleAt` is the Unix timestamp (in seconds) for when the cache entry becomes stale.
   * - `expireAt` is the Unix timestamp (in seconds) for when the cache entry must be removed from the cache.
   * - `revalidate` is the value from Next.js revalidate option.
   * May be false if the page has no revalidate option or the revalidate option is set to false.
   */
  static #getLifespanParameters(
    lastModified: number,
    revalidate?: Revalidate,
  ): LifespanParameters {
    const lastModifiedAt = Math.floor(lastModified / 1000);
    const staleAge = revalidate || CacheHandler.#defaultStaleAge;
    const staleAt = lastModifiedAt + staleAge;
    const expireAge = CacheHandler.#estimateExpireAge(staleAge);
    const expireAt = lastModifiedAt + expireAge;

    return {
      expireAge,
      expireAt,
      lastModifiedAt,
      revalidate,
      staleAge,
      staleAt,
    };
  }

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
   *

   */
  static onCreation(onCreationHook: OnCreationHook): void {
    CacheHandler.#onCreationHook = onCreationHook;
  }

  static async #ensureConfigured(): Promise<void> {
    if (CacheHandler.#mergedHandler) {
      if (CacheHandler.#debug) {
        console.info(
          "[CacheHandler] %s",
          "Using existing CacheHandler configuration.",
        );
      }
      return;
    }

    if (!CacheHandler.#configureTask) {
      CacheHandler.#configureTask = (async () => {
        try {
          await CacheHandler.#configureCacheHandlerInternal();
        } finally {
          CacheHandler.#configureTask = undefined;
        }
      })();
    }

    await CacheHandler.#configureTask;
  }

  static async #configureCacheHandlerInternal(): Promise<void> {
    if (CacheHandler.#mergedHandler) {
      return;
    }

    if (CacheHandler.#debug) {
      console.info(
        "[CacheHandler] %s",
        "Creating new CacheHandler configuration.",
      );
    }

    const { serverDistDir, dev } = CacheHandler.#context;

    let buildId: string | undefined;

    try {
      buildId = await fsPromises.readFile(
        path.join(serverDistDir, "..", "BUILD_ID"),
        "utf-8",
      );
    } catch (_error) {
      buildId = undefined;
    }
    // Retrieve cache configuration by invoking the onCreation hook with the provided context
    const config = CacheHandler.#onCreationHook({
      serverDistDir,
      dev,
      buildId,
    });

    if (CacheHandler.#debug) {
      console.info(
        "[CacheHandler] %s",
        "Cache configuration retrieved from onCreation hook.",
      );
    }

    // Wait for the cache configuration to be resolved
    const { handlers, ttl = {} } = await config;

    const { defaultStaleAge, estimateExpireAge } = ttl;

    if (typeof defaultStaleAge === "number") {
      CacheHandler.#defaultStaleAge = Math.floor(defaultStaleAge);
    }

    CacheHandler.#estimateExpireAge =
      createValidatedAgeEstimationFunction(estimateExpireAge);

    CacheHandler.#serverDistDir = serverDistDir;

    // Notify the user that the cache is not used in development mode
    if (dev) {
      console.warn(
        "[CacheHandler] %s",
        "Next.js does not use the cache in development mode. Use production mode to enable caching.",
      );
    }

    try {
      const prerenderManifestData = await fsPromises.readFile(
        path.join(serverDistDir, "..", "prerender-manifest.json"),
        "utf-8",
      );

      const prerenderManifest = JSON.parse(
        prerenderManifestData,
      ) as PrerenderManifest;

      if (prerenderManifest.version !== PRERENDER_MANIFEST_VERSION) {
        throw new Error(
          `Invalid prerender manifest version. Expected version ${PRERENDER_MANIFEST_VERSION}. Please check if the Next.js version is compatible with the CacheHandler version.`,
        );
      }

      for (const [route, { srcRoute, dataRoute }] of Object.entries(
        prerenderManifest.routes,
      )) {
        const isPagesRouter = dataRoute?.endsWith(".json");

        if (
          isPagesRouter &&
          prerenderManifest.dynamicRoutes[srcRoute || ""]?.fallback === false
        ) {
          CacheHandler.#fallbackFalseRoutes.add(route);
        }
      }
    } catch (_error) {
      if (CacheHandler.#debug) {
        console.warn(
          "[CacheHandler] [%s] %s %s",
          "instrumentation.cache",
          "Failed to read prerender manifest. Pages from the Pages Router with `fallback: false` will return 404 errors.",
          `Error: ${_error}`,
        );
      }
    }

    const handlersList: Handler[] = handlers.filter((handler) => !!handler);

    CacheHandler.#cacheListLength = handlersList.length;

    CacheHandler.#mergedHandler = {
      async get(key, meta) {
        for (const handler of handlersList) {
          if (CacheHandler.#debug) {
            console.info(
              "[CacheHandler] [handler: %s] [method: %s] [key: %s] %s",
              handler.name,
              "get",
              key,
              "Started retrieving value.",
            );
          }

          try {
            let cacheHandlerValue = await handler.get(key, meta);

            if (
              cacheHandlerValue?.lifespan &&
              cacheHandlerValue.lifespan.expireAt <
                Math.floor(Date.now() / 1000)
            ) {
              if (CacheHandler.#debug) {
                console.info(
                  "[CacheHandler] [handler: %s] [method: %s] [key: %s] %s",
                  handler.name,
                  "get",
                  key,
                  "Entry expired.",
                );
              }

              cacheHandlerValue = null;

              // remove the entry from all handlers in background
              removeEntryFromHandlers(handlersList, key, CacheHandler.#debug);
            }

            if (cacheHandlerValue && CacheHandler.#debug) {
              console.info(
                "[CacheHandler] [handler: %s] [method: %s] [key: %s] %s",
                handler.name,
                "get",
                key,
                "Successfully retrieved value.",
              );
            }

            return cacheHandlerValue;
          } catch (error) {
            if (CacheHandler.#debug) {
              console.warn(
                "[CacheHandler] [handler: %s] [method: %s] [key: %s] %s",
                handler.name,
                "get",
                key,
                `Error: ${error}`,
              );
            }
          }
        }

        return null;
      },
      async set(key, cacheHandlerValue) {
        const operationsResults = await Promise.allSettled(
          handlersList.map((handler) =>
            handler.set(key, { ...cacheHandlerValue }),
          ),
        );

        if (!CacheHandler.#debug) {
          return;
        }

        operationsResults.forEach((handlerResult, index) => {
          if (handlerResult.status === "rejected") {
            console.warn(
              "[CacheHandler] [handler: %s] [method: %s] [key: %s] %s",
              handlersList[index]?.name ?? `unknown-${index}`,
              "set",
              key,
              `Error: ${handlerResult.reason}`,
            );
          } else {
            console.info(
              "[CacheHandler] [handler: %s] [method: %s] [key: %s] %s",
              handlersList[index]?.name ?? `unknown-${index}`,
              "set",
              key,
              "Successfully set value.",
            );
          }
        });
      },
      async revalidateTag(tag) {
        const operationsResults = await Promise.allSettled(
          handlersList.map((handler) => handler.revalidateTag(tag)),
        );

        if (!CacheHandler.#debug) {
          return;
        }

        operationsResults.forEach((handlerResult, index) => {
          if (handlerResult.status === "rejected") {
            console.warn(
              "[CacheHandler] [handler: %s] [method: %s] [tag: %s] %s",
              handlersList[index]?.name ?? `unknown-${index}`,
              "revalidateTag",
              tag,
              `Error: ${handlerResult.reason}`,
            );
          } else {
            console.info(
              "[CacheHandler] [handler: %s] [method: %s] [tag: %s] %s",
              handlersList[index]?.name ?? `unknown-${index}`,
              "revalidateTag",
              tag,
              "Successfully revalidated tag.",
            );
          }
        });
      },
    };

    if (CacheHandler.#debug) {
      console.info(
        "[CacheHandler] [handlers: [%s]] %s",
        handlersList.map((handler) => handler.name).join(", "),
        "Successfully created CacheHandler configuration.",
      );
    }
  }

  /**
   * Creates a new CacheHandler instance. Constructor is intended for internal use only.
   */
  constructor(context: FileSystemCacheContext) {
    CacheHandler.#context = context;

    if (CacheHandler.#debug) {
      console.info(
        "[CacheHandler] %s",
        "Instance created with provided context.",
      );
    }
  }

  async get(
    cacheKey: CacheHandlerParametersGet[0],
    ctx:
      | GetIncrementalFetchCacheContext
      | (GetIncrementalResponseCacheContext & { softTags?: null | [] })
      | { softTags: [] } = { softTags: [] },
  ): Promise<CacheHandlerValue | null> {
    await CacheHandler.#ensureConfigured();

    const { softTags = [] } = ctx;

    if (CacheHandler.#debug) {
      console.info(
        "[CacheHandler] [method: %s] [key: %s] %s",
        "get",
        cacheKey,
        "Started retrieving value in order.",
      );
    }

    let cachedData: CacheHandlerValue | null | undefined =
      await CacheHandler.#mergedHandler.get(cacheKey, {
        implicitTags: softTags ?? [],
      });

    if (
      !cachedData &&
      process.env.NEXT_PHASE === PHASE_PRODUCTION_BUILD &&
      CacheHandler.#fallbackFalseRoutes.has(cacheKey)
    ) {
      cachedData = await CacheHandler.#readPagesRouterPage(cacheKey);

      // if we have a value from the file system, we should set it to the cache store
      if (cachedData) {
        await CacheHandler.#mergedHandler.set(cacheKey, cachedData);
      }
    }

    if (CacheHandler.#debug) {
      console.info(
        "[CacheHandler] [method: %s] [key: %s] %s",
        "get",
        cacheKey,
        `Retrieving value ${cachedData ? "found" : "not found"}.`,
      );
    }

    return cachedData ?? null;
  }

  async set(
    cacheKey: CacheHandlerParametersSet[0],
    incrementalCacheValue: CacheHandlerParametersSet[1],
    ctx: CacheHandlerParametersSet[2] & {
      internal_lastModified?: number;
      tags?: string[];
      revalidate?: Revalidate;
    },
  ): Promise<void> {
    await CacheHandler.#ensureConfigured();

    if (CacheHandler.#debug) {
      console.info(
        "[CacheHandler] [method: %s] [key: %s] %s",
        "set",
        cacheKey,
        "Started setting value in parallel.",
      );
    }

    const { tags = [], internal_lastModified } = ctx ?? {};

    const revalidate = resolveRevalidateValue(incrementalCacheValue, ctx);

    const lastModified = Math.round(internal_lastModified ?? Date.now());

    const hasFallbackFalse = CacheHandler.#fallbackFalseRoutes.has(cacheKey);

    const lifespan = CacheHandler.#getLifespanParameters(
      lastModified,
      revalidate,
    );

    // If expireAt is in the past, do not cache
    if (lifespan !== null && Date.now() > lifespan.expireAt * 1000) {
      return;
    }

    let cacheHandlerValueTags = tags;

    let value = incrementalCacheValue;

    switch (value?.kind) {
      case "APP_PAGE": {
        cacheHandlerValueTags = getTagsFromHeaders(value.headers ?? {});
        break;
      }
      default: {
        break;
      }
    }

    const cacheHandlerValue: CacheHandlerValue = {
      lastModified,
      lifespan,
      tags: Object.freeze(cacheHandlerValueTags),
      value: value,
    };

    await CacheHandler.#mergedHandler.set(cacheKey, cacheHandlerValue);

    if (
      process.env.NEXT_PHASE === PHASE_PRODUCTION_BUILD &&
      hasFallbackFalse &&
      cacheHandlerValue.value?.kind === "APP_PAGE"
    ) {
      await CacheHandler.#writePagesRouterPage(
        cacheKey,
        cacheHandlerValue.value as unknown as IncrementalCachedPageValue,
      );
    }

    if (
      process.env.NEXT_PHASE === PHASE_PRODUCTION_BUILD &&
      cacheHandlerValue.value?.kind === "FETCH"
    ) {
      await CacheHandler.#writeFetch(
        cacheKey,
        cacheHandlerValue.value as unknown as CachedFetchValue,
        ctx as SetIncrementalFetchCacheContext,
      );
    }
  }

  async revalidateTag(
    tag: CacheHandlerParametersRevalidateTag[0],
  ): Promise<void> {
    await CacheHandler.#ensureConfigured();

    const tags = typeof tag === "string" ? [tag] : tag;

    if (CacheHandler.#debug) {
      console.info(
        "[CacheHandler] [method: %s] [tags: [%s]] %s",
        "revalidateTag",
        tags.join(", "),
        "Started revalidating tag in parallel.",
      );
    }

    for (const tag of tags) {
      await CacheHandler.#mergedHandler.revalidateTag(tag);
    }
  }

  resetRequestCache(): void {
    // not implemented yet
  }
}
