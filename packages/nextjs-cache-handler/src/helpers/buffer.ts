import {
  IncrementalCacheValue,
  CachedRouteValue,
  IncrementalCachedAppPageValue,
} from "next/dist/server/response-cache";
import { CacheHandlerValue } from "../handlers/cache-handler.types";
import {
  RedisCompliantCachedRouteValue,
  RedisCompliantCachedAppPageValue,
} from "../handlers/redis-strings.types";

export function parseBuffersToStrings(cacheHandlerValue: CacheHandlerValue) {
  if (!cacheHandlerValue?.value) {
    return;
  }

  const value: IncrementalCacheValue | null = cacheHandlerValue.value;

  const kind = value?.kind;

  if (kind === "APP_ROUTE") {
    const appRouteData = value as unknown as RedisCompliantCachedRouteValue;
    const appRouteValue = value as unknown as CachedRouteValue;

    if (appRouteValue?.body) {
      // Convert body Buffer to string
      // See: https://github.com/vercel/next.js/blob/f5444a16ec2ef7b82d30048890b613aa3865c1f1/packages/next/src/server/response-cache/types.ts#L97
      appRouteData.body = appRouteValue.body.toString("base64");
    }
  } else if (kind === "APP_PAGE") {
    const appPageData = value as unknown as RedisCompliantCachedAppPageValue;
    const appPageValue = value as unknown as IncrementalCachedAppPageValue;

    if (appPageValue?.rscData) {
      // Convert rscData Buffer to string
      // See: https://github.com/vercel/next.js/blob/f5444a16ec2ef7b82d30048890b613aa3865c1f1/packages/next/src/server/response-cache/types.ts#L76
      appPageData.rscData = appPageValue.rscData.toString("base64");
    }

    if (appPageValue?.segmentData) {
      // Convert segmentData Map<string, Buffer> to Record<string, string>
      // See: https://github.com/vercel/next.js/blob/f5444a16ec2ef7b82d30048890b613aa3865c1f1/packages/next/src/server/response-cache/types.ts#L80
      appPageData.segmentData = Object.fromEntries(
        Array.from(appPageValue.segmentData.entries()).map(([key, value]) => [
          key,
          value.toString("base64"),
        ]),
      );
    }
  }
}

export function convertStringsToBuffers(cacheValue: CacheHandlerValue) {
  const value = cacheValue.value;
  const kind = value?.kind;

  if (kind === "APP_ROUTE") {
    const appRouteData = value as unknown as RedisCompliantCachedRouteValue;

    if (appRouteData?.body) {
      // Convert body string to Buffer
      // See: https://github.com/vercel/next.js/blob/f5444a16ec2ef7b82d30048890b613aa3865c1f1/packages/next/src/server/response-cache/types.ts#L97
      const appRouteValue = value as unknown as CachedRouteValue;
      appRouteValue.body = Buffer.from(appRouteData.body, "base64");
    }
  } else if (kind === "APP_PAGE") {
    const appPageData = value as unknown as RedisCompliantCachedAppPageValue;
    const appPageValue = value as unknown as IncrementalCachedAppPageValue;

    if (appPageData.rscData) {
      // Convert rscData string to Buffer
      // See: https://github.com/vercel/next.js/blob/f5444a16ec2ef7b82d30048890b613aa3865c1f1/packages/next/src/server/response-cache/types.ts#L76
      appPageValue.rscData = Buffer.from(appPageData.rscData, "base64");
    }

    if (appPageData.segmentData) {
      // Convert segmentData Record<string, string> to Map<string, Buffer>
      // See: https://github.com/vercel/next.js/blob/f5444a16ec2ef7b82d30048890b613aa3865c1f1/packages/next/src/server/response-cache/types.ts#L80
      appPageValue.segmentData = new Map(
        Object.entries(appPageData.segmentData).map(([key, value]) => [
          key,
          Buffer.from(value, "base64"),
        ]),
      );
    }
  }
}
