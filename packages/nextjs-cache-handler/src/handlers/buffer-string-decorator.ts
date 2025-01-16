import { Handler } from "@neshca/cache-handler";
import { CachedRouteValue } from "next/dist/server/response-cache";

type ConvertedStaticPageCacheData = CachedRouteValue & {
  body: string;
};

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
      const staticPageCacheData =
        hit?.value as unknown as ConvertedStaticPageCacheData;
      if (hit?.value && staticPageCacheData?.body) {
        return {
          ...hit,
          value: {
            ...hit.value,
            body: Buffer.from(staticPageCacheData.body, "utf-8"),
          },
        };
      }
      return hit;
    },

    async set(key, data) {
      const routeValue = data.value as CachedRouteValue;
      if (routeValue?.kind === "ROUTE" && routeValue?.body) {
        await handler.set(key, {
          ...data,
          value: {
            ...data.value,
            body: routeValue.body.toString(),
          } as ConvertedStaticPageCacheData,
        });
      } else {
        await handler.set(key, data);
      }
    },

    async revalidateTag(tag) {
      await handler.revalidateTag(tag);
    },

    async delete(key) {
      await handler.delete?.(key);
    },
  };
}
