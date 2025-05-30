import { CacheHandlerValue, Handler } from "@neshca/cache-handler";
import {
  CachedAppPageValue,
  CachedRouteValue,
  ConvertedCachedAppPageValue,
  ConvertedCachedRouteValue,
} from "./buffer-string-decorator.types";
import { IncrementalCacheValue } from "next/dist/server/response-cache";
import { Next15IncrementalCacheValue } from "./next15.types";

/*
 * This cache handler converts buffers from cached route values to strings on save and back to buffers on read.
 *
 * Next 15 decided to change type of data.value.body property from String to Buffer
 * which conflicts with how data is serialized to redis.
 */
export default function bufferStringDecorator(handler: Handler): Handler {
  return {
    name: "forte-digital-next15-buffer-resolver-handler",

    async get(key, ctx) {
      const hit = await handler.get(key, ctx);

      if (!hit?.value) {
        return hit;
      }

      const value = hit.value;
      const kind = value?.kind as string;

      if (kind === "APP_ROUTE") {
        const appRouteData = value as unknown as ConvertedCachedRouteValue;

        if (appRouteData?.body) {
          // Convert body string to Buffer
          // See: https://github.com/vercel/next.js/blob/f5444a16ec2ef7b82d30048890b613aa3865c1f1/packages/next/src/server/response-cache/types.ts#L97

          const appRouteValue = value as unknown as CachedRouteValue;
          appRouteValue.body = Buffer.from(appRouteData.body, "utf-8");
        }
      } else if (kind === "APP_PAGE") {
        const appPageData = value as unknown as ConvertedCachedAppPageValue;
        const appPageValue = value as unknown as CachedAppPageValue;

        if (appPageData.rscData) {
          // Convert rscData string to Buffer
          // See: https://github.com/vercel/next.js/blob/f5444a16ec2ef7b82d30048890b613aa3865c1f1/packages/next/src/server/response-cache/types.ts#L76

          appPageValue.rscData = Buffer.from(appPageData.rscData, "utf-8");
        }

        if (appPageData.segmentData) {
          // Convert segmentData Record<string, string> to Map<string, Buffer>
          // See: https://github.com/vercel/next.js/blob/f5444a16ec2ef7b82d30048890b613aa3865c1f1/packages/next/src/server/response-cache/types.ts#L80

          appPageValue.segmentData = new Map(
            Object.entries(appPageData.segmentData).map(([key, value]) => [
              key,
              Buffer.from(value, "utf-8"),
            ]),
          );
        }
      }

      return hit;
    },

    async set(key, data) {
      if (!data?.value) {
        await handler.set(key, data);
        return;
      }

      const value = { ...data.value };
      const kind = value?.kind as string;

      if (kind === "APP_ROUTE") {
        const appRouteData = value as unknown as ConvertedCachedRouteValue;
        const appRouteValue = value as unknown as CachedRouteValue;

        if (appRouteValue?.body) {
          // Convert body Buffer to string
          // See: https://github.com/vercel/next.js/blob/f5444a16ec2ef7b82d30048890b613aa3865c1f1/packages/next/src/server/response-cache/types.ts#L97

          appRouteData.body = appRouteValue.body.toString();
        }
      } else if (kind === "APP_PAGE") {
        const appPageData = value as unknown as ConvertedCachedAppPageValue;
        const appPageValue = value as unknown as CachedAppPageValue;

        if (appPageValue?.rscData) {
          // Convert rscData Buffer to string
          // See: https://github.com/vercel/next.js/blob/f5444a16ec2ef7b82d30048890b613aa3865c1f1/packages/next/src/server/response-cache/types.ts#L76

          appPageData.rscData = appPageValue.rscData.toString();
        }

        if (appPageValue?.segmentData) {
          // Convert segmentData Map<string, Buffer> to Record<string, string>
          // See: https://github.com/vercel/next.js/blob/f5444a16ec2ef7b82d30048890b613aa3865c1f1/packages/next/src/server/response-cache/types.ts#L80

          appPageData.segmentData = Object.fromEntries(
            Array.from(appPageValue.segmentData.entries()).map(
              ([key, value]) => [key, value.toString()],
            ),
          );
        }
      }

      await handler.set(key, { ...data, value });
    },

    async revalidateTag(tag) {
      await handler.revalidateTag(tag);
    },

    async delete(key) {
      await handler.delete?.(key);
    },
  };
}
