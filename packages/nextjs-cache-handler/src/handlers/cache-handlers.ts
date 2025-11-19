import { createMergedHandler } from "./cache-handler.js";
import type {
  CacheHandlerConfig,
  CacheHandlersValue,
  Handler,
  LifespanParameters,
} from "./cache-handler.types.js";
import type { CacheHandler as CacheHandlerType } from "next/dist/server/lib/cache-handlers/types";

export async function createCacheHandlers(
  config: CacheHandlerConfig<CacheHandlersValue>,
): Promise<CacheHandlerType> {
  const handlersList: Handler<CacheHandlersValue>[] = config.handlers.filter(
    (handler) => !!handler,
  );
  const memoryCache = createMergedHandler(handlersList);

  const debug = process.env.NEXT_PRIVATE_DEBUG_CACHE
    ? console.debug.bind(console, "CustomCacheHandler:")
    : undefined;

  function getLifespanParameters(
    revalidate: number,
    timestamp: number,
    expire: number,
  ): LifespanParameters {
    const lastModifiedAt = Math.floor(timestamp / 1000);
    const staleAge = revalidate;
    const staleAt = lastModifiedAt + staleAge;
    const expireAge = expire;
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

  return {
    async get(cacheKey, softTags) {
      const cachedData = await memoryCache.get(cacheKey, {
        implicitTags: softTags,
      });

      if (!cachedData) {
        debug?.("get", cacheKey, "not found");
        return undefined;
      }

      const entry = cachedData.value;

      debug?.("get", cacheKey, "found", {
        ...cachedData,
        value: {
          ...entry,
          value: "[ReadableStream]",
        },
      });

      return {
        ...entry,
        value: new ReadableStream({
          start(controller) {
            controller.enqueue(entry.value);
            controller.close();
          },
        }),
      };
    },

    async set(cacheKey, pendingEntry) {
      debug?.("set", cacheKey, "start");

      const entry = await pendingEntry;

      // See: https://github.com/vercel/next.js/blob/aba8a9fb890c0a236fe86d38cb946c7df01c3d2f/docs/01-app/03-api-reference/05-config/01-next-config-js/cacheHandlers.mdx?plain=1#L368-L382
      const reader = entry.value.getReader();
      const chunks = [];

      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          chunks.push(value);
        }
      } finally {
        reader.releaseLock();
      }

      const data = Buffer.concat(chunks.map((chunk) => Buffer.from(chunk)));

      const lifespan = getLifespanParameters(
        entry.revalidate,
        entry.timestamp,
        entry.expire,
      );

      // If expireAt is in the past, do not cache
      if (lifespan !== null && Date.now() > lifespan.expireAt * 1000) {
        return;
      }

      const cacheHandlerValueTags = entry.tags;

      const cacheHandlerValue: CacheHandlersValue = {
        lastModified: entry.timestamp,
        lifespan,
        tags: Object.freeze(cacheHandlerValueTags),
        value: {
          ...entry,
          value: data,
        },
      };

      await memoryCache.set(cacheKey, cacheHandlerValue);
    },

    async refreshTags() {
      // Nothing to do for an in-memory cache handler.
    },

    async getExpiration() {
      return Infinity;
    },

    async updateTags(tags, durations) {
      await Promise.all(
        tags.map(async (tag) => memoryCache.revalidateTag(tag)),
      );
    },
  };
}
