import { promises as fsPromises } from "node:fs";
import path from "node:path";
import os from "node:os";
import { PRERENDER_MANIFEST, SERVER_DIRECTORY } from "next/constants";
import type { PrerenderManifest } from "next/dist/build";
import { CACHE_ONE_YEAR } from "next/dist/lib/constants";
import {
  CachedFetchValue,
  CachedRouteValue,
  IncrementalCachedAppPageValue,
  IncrementalCachedPageValue,
} from "next/dist/server/response-cache";
import type { OutgoingHttpHeaders } from "http";
import { getTagsFromHeaders } from "../helpers/getTagsFromHeaders";
import { Revalidate } from "../handlers/cache-handler.types";
import pLimit from "p-limit";

type CacheHandlerType = typeof import("../handlers/cache-handler").CacheHandler;

type NextRouteMetadata = {
  status: number | undefined;
  headers: OutgoingHttpHeaders | undefined;
  postponed: string | undefined;
};

type Router = "pages" | "app";

const PRERENDER_MANIFEST_VERSION = 4;

const DEFAULT_BUILD_DIR = ".next";

/**
 * Options for the `registerInitialCache` instrumentation.
 */
export type RegisterInitialCacheOptions = {
  /**
   * Whether to populate the cache with fetch calls.
   *
   * @default true
   */
  fetch?: boolean;
  /**
   * Whether to populate the cache with pre-rendered pages.
   *
   * @default true
   */
  pages?: boolean;
  /**
   * Whether to populate the cache with routes.
   *
   * @default true
   */
  routes?: boolean;
  /**
   * Override the default build directory.
   *
   * @default .next
   */
  buildDir?: string;
  /**
   * The maximum number of concurrent operations.
   * This speeds up the initial cache population because routes are read and processed in parallel.
   * The default value is either `os.availableParallelism()` (i.e., in most cases the number of CPU cores) or,
   * since most Next.js instances only have a single CPU core, 4, whichever is higher.
   * Depending on your specific needs, this value can be adjusted to optimize the startup performance.
   * By supplying 1, you can disable parallelism and run all operations sequentially.
   *
   * @default Math.max(4, os.availableParallelism())
   */
  parallelism?: number;
};

/**
 * Populates the cache with the initial data.
 *
 * By default, it includes the following:
 * - Pre-rendered pages
 * - Routes
 * - Fetch calls
 *
 * @param CacheHandler - The configured CacheHandler class, not an instance.
 *
 * @param [options={}] - Options for the instrumentation. See {@link RegisterInitialCacheOptions}.
 *
 * @param [options.fetch=true] - Whether to populate the cache with fetch calls.
 *
 * @param [options.pages=true] - Whether to populate the cache with pre-rendered pages.
 *
 * @param [options.routes=true] - Whether to populate the cache with routes.
 *
 * @example file: `instrumentation.ts`
 *
 * ```js
 * export async function register() {
 *  if (process.env.NEXT_RUNTIME === 'nodejs') {
 *    const { registerInitialCache } = await import('@fortedigital/nextjs-cache-handler/instrumentation');
 *    // Assuming that your CacheHandler configuration is in the root of the project and the instrumentation is in the src directory.
 *    // Please adjust the path accordingly.
 *    // CommonJS CacheHandler configuration is also supported.
 *    const CacheHandler = (await import('../cache-handler.mjs')).default;
 *    await registerInitialCache(CacheHandler);
 *  }
 * }
 * ```
 *
 *
 */
export async function registerInitialCache(
  CacheHandler: CacheHandlerType,
  options: RegisterInitialCacheOptions = {},
) {
  const debug = typeof process.env.NEXT_PRIVATE_DEBUG_CACHE !== "undefined";
  const nextJsPath = path.join(
    process.cwd(),
    options.buildDir ?? DEFAULT_BUILD_DIR,
  );
  const prerenderManifestPath = path.join(nextJsPath, PRERENDER_MANIFEST);
  const serverDistDir = path.join(nextJsPath, SERVER_DIRECTORY);
  const fetchCacheDir = path.join(nextJsPath, "cache", "fetch-cache");

  const populateFetch = options.fetch ?? true;
  const populatePages = options.pages ?? true;
  const populateRoutes = options.routes ?? true;

  let prerenderManifest: PrerenderManifest | undefined;

  try {
    const prerenderManifestData = await fsPromises.readFile(
      prerenderManifestPath,
      "utf-8",
    );
    prerenderManifest = JSON.parse(prerenderManifestData) as PrerenderManifest;

    if (prerenderManifest.version !== PRERENDER_MANIFEST_VERSION) {
      throw new Error(
        `Invalid prerender manifest version. Expected version ${PRERENDER_MANIFEST_VERSION}. Please check if the Next.js version is compatible with the CacheHandler version.`,
      );
    }
  } catch (error) {
    if (debug) {
      console.warn(
        "[CacheHandler] [%s] %s %s",
        "registerInitialCache",
        "Failed to read prerender manifest",
        `Error: ${error}`,
      );
    }

    return;
  }

  const context = {
    serverDistDir,
    dev: process.env.NODE_ENV === "development",
  };

  let cacheHandler: InstanceType<CacheHandlerType>;

  try {
    cacheHandler = new CacheHandler(
      context as ConstructorParameters<typeof CacheHandler>[0],
    );
  } catch (error) {
    if (debug) {
      console.warn(
        "[CacheHandler] [%s] %s %s",
        "registerInitialCache",
        "Failed to create CacheHandler instance",
        `Error: ${error}`,
      );
    }

    return;
  }

  async function setRouteCache(
    cachePath: string,
    router: Router,
    revalidate: Revalidate,
  ) {
    const pathToRouteFiles = path.join(serverDistDir, router, cachePath);

    let lastModified: number | undefined;

    try {
      const stats = await fsPromises.stat(`${pathToRouteFiles}.body`);
      lastModified = stats.mtimeMs;
    } catch (error) {
      if (debug) {
        console.warn(
          "[CacheHandler] [%s] %s %s",
          "registerInitialCache",
          "Failed to read route body file",
          `Error: ${error}`,
        );
      }

      return;
    }

    let body: Buffer;
    let meta: NextRouteMetadata;

    try {
      [body, meta] = await Promise.all([
        fsPromises.readFile(`${pathToRouteFiles}.body`),
        fsPromises
          .readFile(`${pathToRouteFiles}.meta`, "utf-8")
          .then((data) => JSON.parse(data) as NextRouteMetadata),
      ]);

      if (!(meta.headers && meta.status)) {
        throw new Error("Invalid route metadata. Missing headers or status.");
      }
    } catch (error) {
      if (debug) {
        console.warn(
          "[CacheHandler] [%s] %s %s",
          "registerInitialCache",
          "Failed to read route body or metadata file, or parse metadata",
          `Error: ${error}`,
        );
      }

      return;
    }

    try {
      const value: CachedRouteValue = {
        kind: "APP_ROUTE" as unknown as any,
        body,
        headers: meta.headers,
        status: meta.status,
      };
      await cacheHandler.set(cachePath, value, {
        revalidate,
        internal_lastModified: lastModified,
        tags: getTagsFromHeaders(meta.headers),
      });
    } catch (error) {
      if (debug) {
        console.warn(
          "[CacheHandler] [%s] %s %s",
          "registerInitialCache",
          "Failed to set route cache. Please check if the CacheHandler is configured correctly",
          `Error: ${error}`,
        );
      }

      return;
    }
  }

  async function setPageCache(
    cachePath: string,
    router: Router,
    revalidate: Revalidate,
  ) {
    const isAppRouter = router === "app";

    if (cachePath === "/") {
      cachePath = "/index";
    }

    const pathToRouteFiles = path.join(serverDistDir, router, cachePath);

    let lastModified: number | undefined;

    try {
      const stats = await fsPromises.stat(`${pathToRouteFiles}.html`);
      lastModified = stats.mtimeMs;
    } catch (error) {
      if (debug) {
        console.warn(
          "[CacheHandler] [%s] %s %s",
          "registerInitialCache",
          "Failed to read page html file",
          `Error: ${error}`,
        );
      }
      return;
    }

    let html: string | undefined;
    let pageData: string | object | undefined;
    let meta: NextRouteMetadata | undefined;
    let rscData: string | undefined;

    if (debug) {
      console.info(
        "[CacheHandler] [%s] %s",
        "registerInitialCache",
        "Reading file system cache",
      );
    }

    try {
      [html, pageData, rscData, meta] = await Promise.all([
        fsPromises.readFile(`${pathToRouteFiles}.html`, "utf-8"),
        fsPromises
          .readFile(
            `${pathToRouteFiles}.${isAppRouter ? "rsc" : "json"}`,
            "utf-8",
          )
          .then((data) => (isAppRouter ? data : (JSON.parse(data) as object)))
          .catch((error) => {
            if (debug) {
              console.warn(
                "[CacheHandler] [%s] %s %s",
                "registerInitialCache",
                "Failed to read page data, assuming it does not exist",
                `Error: ${error}`,
              );
            }

            return undefined;
          }),
        isAppRouter
          ? fsPromises
              .readFile(`${pathToRouteFiles}.prefetch.rsc`, "utf-8")
              .then((data) => data)
              .catch((error) => {
                if (debug) {
                  console.warn(
                    "[CacheHandler] [%s] %s %s",
                    "registerInitialCache",
                    "Failed to read page prefetch data, assuming it does not exist",
                    `Error: ${error}`,
                  );
                }

                return undefined;
              })
          : undefined,
        isAppRouter
          ? fsPromises
              .readFile(`${pathToRouteFiles}.meta`, "utf-8")
              .then((data) => JSON.parse(data) as NextRouteMetadata)
          : undefined,
      ]);
    } catch (error) {
      if (debug) {
        console.warn(
          "[CacheHandler] [%s] %s %s",
          "registerInitialCache",
          "Failed to read page html, page data, or metadata file, or parse metadata",
          `Error: ${error}`,
        );
      }

      return;
    }

    if (debug) {
      console.info(
        "[CacheHandler] [%s] %s",
        "registerInitialCache",
        "Saving file system cache to cache handler",
      );
    }

    try {
      const value: IncrementalCachedAppPageValue &
        Partial<Pick<IncrementalCachedPageValue, "pageData">> = {
        kind: (isAppRouter ? "APP_PAGE" : "PAGES") as unknown as any,
        html,
        pageData,
        postponed: meta?.postponed,
        headers: meta?.headers,
        status: meta?.status,
        rscData:
          isAppRouter && rscData ? Buffer.from(rscData, "utf-8") : undefined,
        segmentData: undefined, // TODO: Add segment data
      };

      await cacheHandler.set(cachePath, value, {
        revalidate,
        internal_lastModified: lastModified,
      });

      if (debug) {
        console.info(
          "[CacheHandler] [%s] %s",
          "registerInitialCache",
          "Saved file system cache to cache handler",
        );
      }
    } catch (error) {
      if (debug) {
        console.warn(
          "[CacheHandler] [%s] %s %s",
          "registerInitialCache",
          "Failed to set page cache. Please check if the CacheHandler is configured correctly",
          `Error: ${error}`,
        );
      }

      return;
    }
  }

  // We either take a user-supplied parallelism value or use the default value
  // of 4 or os.availableParallelism(), whichever is higher.
  const limit = pLimit(
    options.parallelism ?? Math.max(4, os.availableParallelism()),
  );

  const promises = Object.entries(prerenderManifest.routes).map(
    ([cachePath, { dataRoute, initialRevalidateSeconds }]) =>
      limit(async () => {
        if (populatePages && dataRoute?.endsWith(".json")) {
          await setPageCache(cachePath, "pages", initialRevalidateSeconds);
        } else if (populatePages && dataRoute?.endsWith(".rsc")) {
          await setPageCache(cachePath, "app", initialRevalidateSeconds);
        } else if (populateRoutes && dataRoute === null) {
          await setRouteCache(cachePath, "app", initialRevalidateSeconds);
        }
      }),
  );

  await Promise.all(promises);

  if (!populateFetch) {
    return;
  }

  let fetchFiles: string[];

  try {
    fetchFiles = await fsPromises.readdir(fetchCacheDir);
  } catch (error) {
    if (debug) {
      console.warn(
        "[CacheHandler] [%s] %s %s",
        "registerInitialCache",
        "Failed to read cache/fetch-cache directory",
        `Error: ${error}`,
      );
    }

    return;
  }

  for (const fetchCacheKey of fetchFiles) {
    const filePath = path.join(fetchCacheDir, fetchCacheKey);

    let lastModified: number | undefined;

    try {
      const stats = await fsPromises.stat(filePath);
      lastModified = stats.mtimeMs;
    } catch (error) {
      if (debug) {
        console.warn(
          "[CacheHandler] [%s] %s %s",
          "registerInitialCache",
          "Failed to read fetch cache file",
          `Error: ${error}`,
        );
      }
      return;
    }

    let fetchCache: CachedFetchValue;
    try {
      fetchCache = await fsPromises
        .readFile(filePath, "utf-8")
        .then((data) => JSON.parse(data) as CachedFetchValue);
    } catch (error) {
      if (debug) {
        console.warn(
          "[CacheHandler] [%s] %s %s",
          "registerInitialCache",
          "Failed to parse fetch cache file",
          `Error: ${error}`,
        );
      }

      return;
    }

    const revalidateValue = fetchCache.revalidate;

    // HACK: By default, Next.js sets the revalidate option to CACHE_ONE_YEAR if the revalidate option is set
    const revalidate =
      revalidateValue === CACHE_ONE_YEAR ? false : revalidateValue;

    try {
      await cacheHandler.set(fetchCacheKey, fetchCache, {
        revalidate,
        internal_lastModified: lastModified,
        tags: fetchCache.tags,
      });
    } catch (error) {
      if (debug) {
        console.warn(
          "[CacheHandler] [%s] %s %s",
          "registerInitialCache",
          "Failed to set fetch cache. Please check if the CacheHandler is configured correctly",
          `Error: ${error}`,
        );
      }
    }
  }
}
