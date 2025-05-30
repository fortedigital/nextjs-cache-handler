import { CacheHandler } from "@neshca/cache-handler";
import {
  CachedFetchValue,
  IncrementalCacheValue,
} from "next/dist/server/response-cache";
import { Next15IncrementalCacheValue } from "./next15.types";

/*
 * Use this handler in Next 15.2.1 and higher after `revalidate` property had been removed from context object.
 * https://github.com/caching-tools/next-shared-cache/issues/1027
 * https://github.com/vercel/next.js/commit/6a1b6336a3062dfdb1b9dddcee6b836da94fd198
 */
export class Next15CacheHandler extends CacheHandler {
  async set(
    cacheKey: string,
    incrementalCacheValue:
      | IncrementalCacheValue
      | Next15IncrementalCacheValue
      | null,
    ctx: {
      revalidate?: number | false;
      fetchCache?: boolean;
      fetchUrl?: string;
      fetchIdx?: number;
      tags?: string[];
    } & { neshca_lastModified?: number },
  ) {
    await super.set(
      cacheKey,
      incrementalCacheValue as unknown as IncrementalCacheValue,
      {
        ...ctx,
        revalidate:
          ctx.revalidate ||
          (incrementalCacheValue as CachedFetchValue)?.revalidate,
      },
    );
  }
}
