import {
  type CachedFetchValue,
  IncrementalCachedAppPageValue,
  SetIncrementalResponseCacheContext,
} from "next/dist/server/response-cache";
import {
  CacheHandlerParametersSet,
  Revalidate,
} from "../handlers/cache-handler.types";

/**
 * Resolved revalidate value based on type of the cached value.
 *
 * @param incrementalCacheValue - Todo
 * @param ctx - todo
 *
 * @returns Cache revalidate value.
 */
export function resolveRevalidateValue(
  incrementalCacheValue: CacheHandlerParametersSet[1],
  ctx: CacheHandlerParametersSet[2] & {
    revalidate?: Revalidate;
  },
) {
  const cachedFetchValue = incrementalCacheValue as CachedFetchValue;
  const cachedPageValue =
    incrementalCacheValue as IncrementalCachedAppPageValue;
  const responseCacheCtx = ctx as SetIncrementalResponseCacheContext;

  let revalidate;

  if (cachedFetchValue?.kind === "FETCH") {
    revalidate = cachedFetchValue.revalidate;
  } else if (
    cachedPageValue?.kind === "APP_PAGE" ||
    cachedPageValue?.kind === "PAGES"
  ) {
    revalidate = responseCacheCtx.cacheControl?.revalidate;
  }

  return revalidate ?? ctx.revalidate;
}
