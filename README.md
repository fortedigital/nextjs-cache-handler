![NPM Version](https://img.shields.io/npm/v/%40fortedigital%2Fnextjs-cache-handler)

# @fortedigital/nextjs-cache-handler

This package extends the functionality of [`@neshca/cache-handler`](https://www.npmjs.com/package/@neshca/cache-handler) by providing additional cache handlers for specialized use cases, specifically for Redis-based caching solutions. The original `@neshca/cache-handler` offers a robust caching API for Next.js applications, and this package introduces two new handlers for managing Redis cache with different expiration strategies and tag-based revalidation.

## Installation

To install this package along with its dependencies:

```bash
npm install @fortedigital/nextjs-cache-handler
```

Package depends on the original `@neshca/cache-handler` package - you can use anything provided by it by using import/require from `@neshca/cache-handler`.

## Next 15 Support

As `@neshca/cache-handler` does not officially support Next 15+ yet, we try to keep up with Next and prepare more or less temporary workarounds. At some point we will either create a fork of `@neshca/cache-handler` to fully support Next 15 or it gets updated by the maintainers. As for now we're building a set of decorators/workarounds you can use to build cache solutions for Next 15. We might need to do a full-blown rework which will be marked with a proper major version upgrade.

### String buffer breaking change

If you use Redis Strings cache handler with Next15+ you need to decorate the default Redis String handler with a buffer converter like this:

```
// ...
const redisCacheHandler = createRedisHandler({
    client: redisClient,
    keyPrefix: "nextjs:",
});

return {
  handlers: [
    createBufferStringHandler(redisCacheHandler)
  ]
}

// ...
```

Read more about this in Handlers section below.

### Revalidate fetch breaking change

Instead of:

```js
const { CacheHandler } = require("@neshca/cache-handler");
module.exports = CacheHandler;
```

Use this:

```js
const { CacheHandler } = require("@neshca/cache-handler");
const {
  Next15CacheHandler,
} = require("@fortedigital/nextjs-cache-handler/next-15-cache-handler");

CacheHandler.onCreation(() => {
  // your usual setup
});

module.exports = new Next15CacheHandler();
```

## Handlers

### 1. `redis-strings`

This handler is designed to manage cache operations using Redis strings. It supports key-based and tag-based caching with flexible expiration strategies. Opposite to `@neshca/cache-handler`, `@fortedigital/nextjs-cache-handler`'s implementation does not have a memory leak caused by endlessly growing shared key hashmap, by adding another hashmap with TTL for shared keys entries. It also has more reliable revalidateTagQuerySize default value - 10_000 - preventing long tag revalidatation requests with large caches.

#### Features:

- **Key Expiration Strategy**: Choose between `EXAT` (more efficient with Redis 6.2 or newer) or `EXPIREAT` (compatible with Redis 4.0 or newer).
- **Tag-based Revalidation**: Supports cache revalidation using tags for efficient and fine-grained cache invalidation.
- **TTL Management**: Automatically manages time-to-live (TTL) for cache keys and tags.

#### Usage:

```js
const redisHandler = await createHandler({
  client, // Redis client instance
  keyPrefix: "myApp:",
  sharedTagsKey: "myTags",
  sharedTagsTtlKey: "myTagTtls",
});
```

### 2. `composite`

The composite handler allows for managing cache operations using a combination of other handlers. It provides a flexible way to route cache operations to multiple underlying handlers based on a defined strategy.

#### Features:

- **Flexible Handling**: Supports multiple handlers, enabling complex caching strategies.
- **Custom Set Strategy**: Allows you to define how cache entries are routed to different handlers.
- **On-Demand Revalidation**: Retrieves values from the first available handler, ensuring efficient data access.

#### Usage:

```js
const compositeHandler = createHandler({
  handlers: [handler1, handler2], // Array of underlying handlers
  setStrategy: (data) => (data?.tags.includes("handler1") ? 0 : 1),
});
```

### 3. `buffer-string-decorator`

#### Features:

This cache handler converts buffers from cached route values to strings on save and back to buffers on read.

Next 15 decided to change types of some properties from String to Buffer which conflicts with how data is serialized to redis. It is recommended to use this handler with `redis-strings` in Next 15 as this handler make the following adjustment.

- **Converts `body` `Buffer` to `string`**  
  See: https://github.com/vercel/next.js/blob/f5444a16ec2ef7b82d30048890b613aa3865c1f1/packages/next/src/server/response-cache/types.ts#L97
- **Converts `rscData` `string` to `Buffer`**  
  See: https://github.com/vercel/next.js/blob/f5444a16ec2ef7b82d30048890b613aa3865c1f1/packages/next/src/server/response-cache/types.ts#L76
- **Converts `segmentData` `Record<string, string>` to `Map<string, Buffer>`**  
  See: https://github.com/vercel/next.js/blob/f5444a16ec2ef7b82d30048890b613aa3865c1f1/packages/next/src/server/response-cache/types.ts#L80

## Full example

```js
// @neshca/cache-handler dependencies
const { CacheHandler } = require("@neshca/cache-handler");
const createLruHandler = require("@neshca/cache-handler/local-lru").default;

// Next/Redis dependencies
const { createClient } = require("redis");
const { PHASE_PRODUCTION_BUILD } = require("next/constants");

// @fortedigital/nextjs-cache-handler dependencies
const createCompositeHandler =
  require("@fortedigital/nextjs-cache-handler/composite").default;
const createRedisHandler =
  require("@fortedigital/nextjs-cache-handler/redis-strings").default;
const createBufferStringHandler =
  require("@fortedigital/nextjs-cache-handler/buffer-string-decorator").default;
const {
  Next15CacheHandler,
} = require("@fortedigital/nextjs-cache-handler/next-15-cache-handler");

// Usual onCreation from @neshca/cache-handler
CacheHandler.onCreation(() => {
  // Important - It's recommended to use global scope to ensure only one Redis connection is made
  // This ensures only one instance get created
  if (global.cacheHandlerConfig) {
    return global.cacheHandlerConfig;
  }

  // Important - It's recommended to use global scope to ensure only one Redis connection is made
  // This ensures new instances are not created in a race condition
  if (global.cacheHandlerConfigPromise) {
    return global.cacheHandlerConfigPromise;
  }

  // You may need to ignore Redis locally, remove this block otherwise
  if (process.env.NODE_ENV === "development") {
    const lruCache = createLruHandler();
    return { handlers: [lruCache] };
  }

  // Main promise initializing the handler
  global.cacheHandlerConfigPromise = (async () => {
    /** @type {import("redis").RedisClientType | null} */
    let redisClient = null;
    if (PHASE_PRODUCTION_BUILD !== process.env.NEXT_PHASE) {
      const settings = {
        url: process.env.REDIS_URL, // Make sure you configure this variable
        pingInterval: 10000,
      };

      // This is optional and needed only if you use access keys
      if (process.env.REDIS_ACCESS_KEY) {
        settings.password = process.env.REDIS_ACCESS_KEY;
      }

      try {
        redisClient = createClient(settings);
        redisClient.on("error", (e) => {
          if (typeof process.env.NEXT_PRIVATE_DEBUG_CACHE !== "undefined") {
            console.warn("Redis error", e);
          }
          global.cacheHandlerConfig = null;
          global.cacheHandlerConfigPromise = null;
        });
      } catch (error) {
        console.warn("Failed to create Redis client:", error);
      }
    }

    if (redisClient) {
      try {
        console.info("Connecting Redis client...");
        await redisClient.connect();
        console.info("Redis client connected.");
      } catch (error) {
        console.warn("Failed to connect Redis client:", error);
        await redisClient
          .disconnect()
          .catch(() =>
            console.warn(
              "Failed to quit the Redis client after failing to connect."
            )
          );
      }
    }
    const lruCache = createLruHandler();

    if (!redisClient?.isReady) {
      console.error("Failed to initialize caching layer.");
      global.cacheHandlerConfigPromise = null;
      global.cacheHandlerConfig = { handlers: [lruCache] };
      return global.cacheHandlerConfig;
    }

    const redisCacheHandler = createRedisHandler({
      client: redisClient,
      keyPrefix: "nextjs:",
    });

    global.cacheHandlerConfigPromise = null;

    // This example uses composite handler to switch from Redis to LRU cache if tags contains `memory-cache` tag.
    // You can skip composite and use Redis or LRU only.
    global.cacheHandlerConfig = {
      handlers: [
        createCompositeHandler({
          handlers: [
            lruCache,
            createBufferStringHandler(redisCacheHandler), // Use `createBufferStringHandler` in Next15 and ignore it in Next14 or below
          ],
          setStrategy: (ctx) => (ctx?.tags.includes("memory-cache") ? 0 : 1), // You can adjust strategy for deciding which cache should the composite use
        }),
      ],
    };

    return global.cacheHandlerConfig;
  })();

  return global.cacheHandlerConfigPromise;
});

module.exports = new Next15CacheHandler();
```

## Reference to Original Package

This package builds upon the core functionality provided by [`@neshca/cache-handler`](https://www.npmjs.com/package/@neshca/cache-handler). You can find more information about the core library, including usage examples and API documentation, at the [official documentation page](https://caching-tools.github.io/next-shared-cache).

## License

This project is licensed under the [MIT License](./LICENSE), as is the original `@neshca/cache-handler` package.
