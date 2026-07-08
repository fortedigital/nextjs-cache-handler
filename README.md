![NPM Version](https://img.shields.io/npm/v/%40fortedigital%2Fnextjs-cache-handler)

# @fortedigital/nextjs-cache-handler

A caching utility built originally on top of [`@neshca/cache-handler`](https://www.npmjs.com/package/@neshca/cache-handler), providing additional cache handlers for specialized use cases with a focus on Redis-based caching.

Starting from version `2.0.0`, this package no longer depends on `@neshca/cache-handler` and is fully maintained and compatible with Next.js 15 and partially 16. See the [compatibility matrix](#feature-compatibility-matrix) for detailed feature support.

**Version Requirements:**

- **Next.js 15**: Version 2.0.0+ (version 3.0.0+ recommended for latest improvements and maintenance development)
- **Next.js 16**: Version 3.0.0+ required

## Table of Contents

- [Documentation](#documentation)
- [Migration](#migration)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Next.js Compatibility](#nextjs-compatibility)
  - [Feature Compatibility Matrix](#feature-compatibility-matrix)
- [Migration](#migration)
  - [Swapping from @neshca/cache-handler](#swapping-from-neshcacache-handler)
- [Handlers](#handlers)
  - [redis-strings](#redis-strings)
  - [local-lru](#local-lru)
  - [composite](#composite)
- [Examples](#examples)
- [Reference to Original Package](#reference-to-original-package)
- [API Reference Links](#api-reference-links)
- [Troubleshooting](#troubleshooting)
- [Legacy / Deprecated](#legacy--deprecated)
- [Contributing](#contributing)
- [License](#license)

## Documentation

The documentation at [@neshca/cache-handler - caching-tools.github.io/next-shared-cache](https://caching-tools.github.io/next-shared-cache) is mostly still relevant, though some details may be outdated. New features or relevant changes are described below.

## Migration

- [1.x.x → ^2.x.x](https://github.com/fortedigital/nextjs-cache-handler/blob/master/docs/migration/1_x_x__2_x_x.md)
- [1.2.x -> ^1.3.x](https://github.com/fortedigital/nextjs-cache-handler/blob/master/docs/migration/1_2_x__1_3_x.md)

### Swapping from `@neshca/cache-handler`

If you already use `@neshca/cache-handler` the setup is very streamlined and you just need to replace package references. If you're starting fresh please check [the example project](./examples/redis-minimal).

#### Cache handler

**Before:**

```js
// cache-handler.mjs

import { CacheHandler } from "@neshca/cache-handler";

CacheHandler.onCreation(() => {
  // setup
});

export default CacheHandler;
```

**After:**

```js
// cache-handler.mjs

import { CacheHandler } from "@fortedigital/nextjs-cache-handler";

CacheHandler.onCreation(() => {
  // setup
});

export default CacheHandler;
```

---

#### Instrumentation

**Before:**

```js
// instrumentation.ts

export async function register() {
  if (process.env.NEXT_RUNTIME === "nodejs") {
    const { registerInitialCache } =
      await import("@neshca/cache-handler/instrumentation");
    const CacheHandler = (await import("../cache-handler.mjs")).default;
    await registerInitialCache(CacheHandler);
  }
}
```

**After:**

```js
// instrumentation.ts

export async function register() {
  if (process.env.NEXT_RUNTIME === "nodejs") {
    const { registerInitialCache } =
      await import("@fortedigital/nextjs-cache-handler/instrumentation");
    const CacheHandler = (await import("../cache-handler.mjs")).default;
    await registerInitialCache(CacheHandler);
  }
}
```

## Prerequisites

Before installing, ensure you have:

- **Node.js** >= 22.0.0
- **Next.js** >= 15.2.4 (for version 2.0.0+) or >= 16.0.0 (for version 3.0.0+)
- **Redis** >= 5.5.6 (or compatible Redis-compatible service)
- **pnpm** >= 9.0.0 (for development)

> **Important:** This package only supports the official [`redis`](https://github.com/redis/node-redis) package (also known as `node-redis`). The `ioredis` package is **not supported**.

See [Version Requirements](#version-requirements) for package version compatibility.

## Installation

`npm i @fortedigital/nextjs-cache-handler`

If upgrading from Next 14 or earlier, **flush your Redis cache** before running new version of the application locally and on your hosted environments. **Cache formats between Next 14 and 15 are incompatible**.

## Quick Start

Here's a minimal setup to get started:

```js
// cache-handler.mjs
import { CacheHandler } from "@fortedigital/nextjs-cache-handler";
import createRedisHandler from "@fortedigital/nextjs-cache-handler/redis-strings";
import { createClient } from "redis";

const client = createClient({ url: process.env.REDIS_URL });
await client.connect();

CacheHandler.onCreation(() => ({
  handlers: [createRedisHandler({ client })],
}));

export default CacheHandler;
```

Then configure it in your `next.config.js`:

```js
// next.config.js
module.exports = {
  cacheHandler: require.resolve("./cache-handler.mjs"),
};
```

For a complete example with error handling, fallbacks, and production setup, see the [Examples](#examples) section below. The quick start code is not meant for production use.

## Next.js Compatibility

The original `@neshca/cache-handler` package does not support Next.js 15.

Prior to 2.0.0, this package provided wrappers and enhancements to allow using `@neshca/cache-handler` with Next.js 15.  
From version 2.0.0 onward, `@fortedigital/nextjs-cache-handler` is a standalone solution with no dependency on `@neshca/cache-handler` and is fully compatible with Next.js 15 and [redis 5](https://www.npmjs.com/package/redis).

<a id="version-requirements"></a>

**Version Requirements:**

- **Next.js 15**: Version 2.0.0+ (version 3.0.0+ recommended for latest improvements and maintenance development)
- **Next.js 16**: Version 3.0.0+ required

We aim to keep up with new Next.js releases and will introduce major changes with appropriate version bumps.

### Feature Compatibility Matrix

| Feature                                              | Next.js 15 | Next.js 16 | Notes                                                |
| ---------------------------------------------------- | ---------- | ---------- | ---------------------------------------------------- |
| **Fetch API Caching**                                |
| `fetch` with default cache (`force-cache`)           | ✅         | ✅         | Default behavior, caches indefinitely                |
| `fetch` with `no-store`                              | ✅         | ✅         | Never caches, always fresh                           |
| `fetch` with `no-cache`                              | ✅         | ✅         | Validates cache on each request                      |
| `fetch` with `next.revalidate`                       | ✅         | ✅         | Time-based revalidation                              |
| `fetch` with `next.tags`                             | ✅         | ✅         | Tag-based cache invalidation                         |
| **Cache Invalidation**                               |
| `revalidateTag(tag)`                                 | ✅         | N/A        | Breaking change in Next.js 16                        |
| `revalidateTag(tag, cacheLife)`                      | N/A        | ✅         | New required API in Next.js 16                       |
| `updateTag(tag)`                                     | N/A        | ✅         | New API for immediate invalidation in Server Actions |
| `revalidatePath(path)`                               | ✅         | ✅         | Path-based revalidation                              |
| `revalidatePath(path, type)`                         | ✅         | ✅         | Type-specific path revalidation                      |
| **Function Caching**                                 |
| `unstable_cache()`                                   | ✅         | ✅         | Cache any function with tags and revalidation        |
| **Static Generation**                                |
| `generateStaticParams()`                             | ✅         | ✅         | Static params generation                             |
| ISR (Incremental Static Regeneration)                | ✅         | ✅         | On-demand regeneration                               |
| Route segment config (`revalidate`, `dynamic`, etc.) | ✅         | ✅         | All segment config options                           |
| **Redis Client Support**                             |
| `redis` package (node-redis)                         | ✅         | ✅         | Official Redis client - fully supported              |
| `ioredis` package                                    | ✅         | ✅         | IORedis client - fully supported                     |
| **Next.js 16 New Features**                          |
| `cacheHandlers` config (for `'use cache'`)           | ❌         | ❌         | Not yet supported - Help needed                      |
| `'use cache'` directive                              | ❌         | ❌         | Not yet supported - Help needed                      |
| `'use cache: remote'` directive                      | ❌         | ❌         | Not yet supported - Help needed                      |
| `'use cache: private'` directive                     | ❌         | ❌         | Not yet supported - Help needed                      |
| `cacheComponents`                                    | ❌         | ❌         | Not yet supported - Help needed                      |

**Notes:**

- `revalidateTag()` in Next.js 16 requires a `cacheLife` parameter (`'max'`, `'hours'`, or `'days'`). This is a breaking change from Next.js 15.
- `cacheLife` profiles are primarily designed for Vercel's infrastructure. Custom cache handlers may not fully differentiate between different `cacheLife` profiles.
- `updateTag()` is only available in Server Actions, not Route Handlers.
- The new `cacheHandlers` API and `'use cache'` directives are not yet supported by this package.

## Handlers

### `redis-strings`

A Redis-based handler for key- and tag-based caching. Compared to the original implementation, it prevents memory leaks caused by growing shared tag maps by implementing TTL-bound hashmaps.

> **Note:** This handler requires the official [`redis`](https://github.com/redis/node-redis) package. `ioredis` is not supported.

**Features:**

- Key expiration using `EXAT` or `EXPIREAT`
- Tag-based revalidation
- Automatic TTL management
- Automatic buffer/string conversion for Next.js 15+ compatibility (previously required `buffer-string-decorator` in version 1.x.x)
- Default `revalidateTagQuerySize`: `10_000` (safe for large caches)

```js
import createRedisHandler from "@fortedigital/nextjs-cache-handler/redis-strings";

const redisHandler = await createRedisHandler({
  client: createClient({
    url: process.env.REDIS_URL,
  }),
  keyPrefix: "myApp:",
  sharedTagsKey: "myTags",
  sharedTagsTtlKey: "myTagTtls",
});
```

#### Custom value serializer

By default, the handler stores each entry as a JSON string (`JSON.stringify` / `JSON.parse`), matching earlier releases. You can plug in your own wire format to shrink payloads (compression), add encryption, or use another encoding-this package does not ship extra codecs so your dependencies stay minimal.

**Contract**

- `serialize(value)` receives the full cache object Next.js passes in (metadata such as `tags`, `lastModified`, `lifespan`, plus the nested `value` payload). It must return a string suitable for Redis `SET`.
- `deserialize(stored)` receives that string from Redis `GET`. Return a parsed object compatible with the handler, or `null` to treat the key as a miss (stale entries are removed).

The handler normalizes `Buffer` fields inside the payload to strings before `serialize`, and restores buffers after `deserialize`, so a plain `JSON.stringify` / `JSON.parse` round trip remains valid.

**Default export for reuse**

You can import the built-in serializer if you want to wrap or compare behavior:

```js
import createRedisHandler, {
  jsonCacheValueSerializer,
} from "@fortedigital/nextjs-cache-handler/redis-strings";
```

**Example: gzip**

Useful when cache entries are large text (RSC payloads, HTML). Uses Node’s built-in `zlib`; `gzipSync` / `gunzipSync` run on the server during cache reads and writes-profile if your traffic is very hot.

```js
import createRedisHandler from "@fortedigital/nextjs-cache-handler/redis-strings";
import { gzipSync, gunzipSync } from "zlib";

const redisCacheHandler = createRedisHandler({
  client: redisClient,
  keyPrefix: "nextjs:",
  valueSerializer: {
    serialize: (value) => gzipSync(JSON.stringify(value)).toString("base64"),
    deserialize: (stored) => {
      const parsed = JSON.parse(
        gunzipSync(Buffer.from(stored, "base64")).toString("utf-8"),
      );
      return parsed;
    },
  },
});
```

**Example: brotli**

Useful when cache entries are large text (RSC payloads, HTML). Uses Node’s built-in `zlib`; `brotliCompress` / `brotliDecompress` run on the server during cache reads and writes-profile if your traffic is very hot.

```js
import createRedisHandler from "@fortedigital/nextjs-cache-handler/redis-strings";
import { brotliCompress, brotliDecompress } from "zlib";
import { promisify } from "util";

const brotliCompressAsync = promisify(brotliCompress);
const brotliDecompressAsync = promisify(brotliDecompress);

const redisCacheHandler = createRedisHandler({
  client: redisClient,
  keyPrefix: "nextjs:",
  valueSerializer: {
    async serialize(value) {
      const compressed = await brotliCompressAsync(
        Buffer.from(JSON.stringify(value), "utf8"),
      );
      return compressed.toString("base64");
    },
    async deserialize(stored) {
      const decompressed = await brotliDecompressAsync(
        Buffer.from(stored, "base64"),
      );
      return JSON.parse(decompressed.toString("utf8"));
    },
  },
});
```

**Operational notes**

- Changing `valueSerializer` (or toggling compression) makes existing Redis keys unreadable until you flush those keys or run a migration; plan a key prefix bump or cache clear on deploy.
- Tag maps and TTL sidecar hashes are still stored as JSON by the handler; only the main entry value uses your serializer.

#### Redis Cluster

```js
import { createCluster } from "@redis/client";
import createRedisHandler from "@fortedigital/nextjs-cache-handler/redis-strings";
import { withAdapter } from "@fortedigital/nextjs-cache-handler/cluster/adapter";

const { hostname: redisHostName } = new URL(process.env.REDIS_URL);
redis = withAdapter(
  createCluster({
    rootNodes: [{ url: process.env.REDIS_URL }],

    // optional if you use TLS and need to resolve shards' ip to proper hostname
    nodeAddressMap(address) {
      const [_, port] = address.split(":");

      return {
        host: redisHostName,
        port: Number(port),
      };
    },
  }),
);

// after using withAdapter you can use redis cluster instance as parameter for createRedisHandler
const redisCacheHandler = createRedisHandler({
  client: redis,
  keyPrefix: CACHE_PREFIX,
});
```

### Using ioredis

If you prefer using `ioredis` instead of `@redis/client`, you can use the `ioredisAdapter` helper.

`npm i ioredis`

```js
import Redis from "ioredis";
import createRedisHandler from "@fortedigital/nextjs-cache-handler/redis-strings";
import { ioredisAdapter } from "@fortedigital/nextjs-cache-handler/helpers/ioredisAdapter";

const client = new Redis(process.env.REDIS_URL);
const redisClient = ioredisAdapter(client);

const redisHandler = createRedisHandler({
  client: redisClient,
  keyPrefix: "my-app:",
});
```

---

### `local-lru`

The local-lru Handler uses a lru-cache ↗ instance as the cache store. It stores the cache in memory and evicts the least recently used entries when the cache reaches its limits. You can use this Handler as a fallback cache when the shared cache is unavailable.

> ⚠️ The local-lru Handler is not suitable for production environments. It is intended for development and testing purposes only.

**Features:**

- Key expiration using `EXAT` or `EXPIREAT`
- Tag-based revalidation
- Automatic TTL management
- Default `revalidateTagQuerySize`: `10_000` (safe for large caches)

```js
import createLruHandler from "@fortedigital/nextjs-cache-handler/local-lru";

const localHandler = createLruHandler({
  maxItemsNumber: 10000,
  maxItemSizeBytes: 1024 * 1024 * 500,
});
```

---

### `composite`

Routes cache operations across multiple underlying handlers.

**Features:**

- Multiple backend support
- Custom routing strategies
- First-available read strategy

```js
import createCompositeHandler from "@fortedigital/nextjs-cache-handler/composite";

const compositeHandler = createCompositeHandler({
  handlers: [handler1, handler2],
  setStrategy: (data) => (data?.tags.includes("handler1") ? 0 : 1),
});
```

## Instrumentation

### Initial cache registration

By default, `registerInitialCache` populates the cache by overwriting any existing
entries with values generated from build-time artifacts (fetch calls, pages, routes).

#### Initial cache write strategy

If you want to preserve values that may already exist in the cache (for example,
entries written at runtime by another instance), you can enable the
`setOnlyIfNotExists` option:

```ts
await registerInitialCache(CacheHandler, {
  setOnlyIfNotExists: true,
});
```

When enabled, cache writes performed during the initial cache registration will only
occur if the corresponding cache key does not already exist. This allows you to
explicitly choose the cache population strategy instead of enforcing a single default.

## Examples

### Example Project

The [example project](./examples/redis-minimal) provides a comprehensive demonstration of Next.js caching features with interactive examples:

- **Default Cache** - Demonstrates `force-cache` behavior
- **No Store** - Shows `no-store` for always-fresh data
- **Time-based Revalidation** - Automatic cache revalidation
- **Fetch with Tags** - Tag-based cache invalidation
- **unstable_cache** - Function caching with tags
- **ISR** - Incremental Static Regeneration
- **Static Params** - Dynamic route static generation

To run the examples:

```bash
pnpm install
cd examples/redis-minimal
npm run build
npm run start
```

> **Note:** Caching only works in production mode. See the [examples README](./examples/redis-minimal/README.md) for more details.

### Production Setup Example

Here's a complete production-ready `cache-handler.js` example:

```js
import { createClient } from "redis";
import { PHASE_PRODUCTION_BUILD } from "next/constants.js";
import { CacheHandler } from "@fortedigital/nextjs-cache-handler";
import createLruHandler from "@fortedigital/nextjs-cache-handler/local-lru";
import createRedisHandler from "@fortedigital/nextjs-cache-handler/redis-strings";
import createCompositeHandler from "@fortedigital/nextjs-cache-handler/composite";

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
    let redisClient = null;

    if (PHASE_PRODUCTION_BUILD !== process.env.NEXT_PHASE) {
      const settings = {
        url: process.env.REDIS_URL,
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
              "Failed to quit the Redis client after failing to connect.",
            ),
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
          handlers: [lruCache, redisCacheHandler],
          setStrategy: (ctx) => (ctx?.tags.includes("memory-cache") ? 0 : 1), // You can adjust strategy for deciding which cache should the composite use
        }),
      ],
    };

    return global.cacheHandlerConfig;
  })();

  return global.cacheHandlerConfigPromise;
});

export default CacheHandler;
```

---

## Reference to Original Package

This project was originally based on [`@neshca/cache-handler`](https://www.npmjs.com/package/@neshca/cache-handler). Versions prior to `2.0.0` wrapped or extended the original. As of `2.0.0`, this project is fully independent and no longer uses or requires `@neshca/cache-handler`.

For context or historical documentation, you may still reference the [original project](https://caching-tools.github.io/next-shared-cache).

## API Reference Links

### Next.js Documentation

- [Caching in Next.js](https://nextjs.org/docs/app/building-your-application/caching) - Comprehensive guide to Next.js caching
- [Data Fetching, Caching, and Revalidating](https://nextjs.org/docs/app/building-your-application/data-fetching) - Fetch API caching options
- [`fetch` API](https://nextjs.org/docs/app/api-reference/functions/fetch) - Next.js fetch options (`next.revalidate`, `next.tags`)
- [`revalidateTag`](https://nextjs.org/docs/app/api-reference/functions/revalidateTag) - Tag-based cache invalidation
- [`revalidatePath`](https://nextjs.org/docs/app/api-reference/functions/revalidatePath) - Path-based cache invalidation
- [`updateTag`](https://nextjs.org/docs/app/api-reference/functions/updateTag) - Immediate cache invalidation (Next.js 16)
- [`unstable_cache`](https://nextjs.org/docs/app/api-reference/functions/unstable_cache) - Function caching
- [Route Segment Config](https://nextjs.org/docs/app/api-reference/file-conventions/route-segment-config) - `revalidate`, `dynamic`, etc.
- [Incremental Static Regeneration](https://nextjs.org/docs/app/building-your-application/data-fetching/incremental-static-regeneration) - ISR documentation

### Redis Documentation

- [Redis Client for Node.js](https://github.com/redis/node-redis) - Official Redis client library
- [Redis Documentation](https://redis.io/docs/) - Redis server documentation
- [Redis Commands](https://redis.io/commands/) - Redis command reference

## Troubleshooting

### Cache not working in development mode

**Issue:** Caching doesn't seem to work when running `npm run dev`.

**Solution:** This is expected behavior. Next.js intentionally disables caching in development mode for faster hot reloading. To test caching functionality, you must use production mode:

```bash
npm run build
npm run start
```

### Redis connection errors

**Issue:** Getting connection errors or "Redis client is not ready" errors.

**Solutions:**

- Verify Redis is running: `redis-cli ping` should return `PONG`
- Check `REDIS_URL` environment variable is set correctly
- Ensure Redis is accessible from your application (check firewall/network settings)
- For production, verify Redis credentials and connection string format
- Check Redis logs for connection issues

### Cache not invalidating after revalidateTag

**Issue:** Calling `revalidateTag()` doesn't seem to clear the cache.

**Solutions:**

- In Next.js 16, ensure you're using `revalidateTag(tag, cacheLife)` with the required `cacheLife` parameter
- Verify the tag matches exactly (tags are case-sensitive)
- Check that the cache entry was created with the same tag
- In development mode, caching is disabled - test in production mode

### Migration from Next.js 14

**Issue:** Errors after upgrading from Next.js 14 to 15/16.

**Solution:** Cache formats between Next.js 14 and 15 are incompatible. **You must flush your Redis cache** before running the new version:

```bash
redis-cli FLUSHALL
```

Or if using a specific database:

```bash
redis-cli -n <database-number> FLUSHDB
```

### Version compatibility issues

**Issue:** Package version doesn't work with your Next.js version.

**Solutions:**

- Next.js 15: Use version 2.0.0+ (3.0.0+ recommended)
- Next.js 16: Use version 3.0.0+ (required)
- Check the [Version Requirements](#version-requirements) section
- Verify your Node.js version is >= 22.0.0

### Debugging cache behavior

**Issue:** Need to debug what's happening with the cache.

**Solution:** Enable debug logging by setting the environment variable:

```bash
NEXT_PRIVATE_DEBUG_CACHE=1 npm run start
```

This will output detailed cache operation logs to help diagnose issues.

## Contributing

This project uses [Turborepo](https://turbo.build/repo) to manage the monorepo structure with the main package and examples.

### Prerequisites

- Node.js >= 22.0.0
- pnpm >= 9.0.0

### Development Workflow

- **Start dev server**: `pnpm dev` (runs all dev servers in parallel)
- **Run all tests**: `pnpm test`
- **Run e2e tests**: `pnpm test:e2e`

### Running e2e tests

Playwright integration tests live in [`examples/redis-minimal`](examples/redis-minimal). They run the example app in production mode against a real Redis instance and assert cache behavior (tag/path revalidation, including Pages Router).

**Prerequisites:** Redis running locally (default `redis://localhost:6379`).

---

## Legacy / Deprecated

### neshClassicCache

⚠️ **Deprecated:** This function was migrated from @neshca for compatibility purposes only. Use with caution - no further development or support is planned.

**Migration:** Use [`unstable_cache`](https://nextjs.org/docs/app/api-reference/functions/unstable_cache) instead, which provides similar functionality with better Next.js integration.

`neshClassicCache` allows you to cache the results of expensive operations, like database queries, and reuse them across multiple requests. Unlike the [`neshCache`](/functions/nesh-cache) or [`unstable_cache` ↗](https://nextjs.org/docs/app/api-reference/functions/unstable_cache) function, `neshClassicCache` must be used in a Next.js Pages Router allowing users to cache data in the `getServerSideProps` and API routes.

> [!NOTE]
>
> Cache entries created with `neshClassicCache` can be revalidated only by the [`revalidateTag` ↗](https://nextjs.org/docs/app/api-reference/functions/revalidateTag) method.

#### Parameters

- `fetchData` - An asynchronous function that fetches the data you want to cache. It must be a function that returns a `Promise`.
- `commonOptions` - An object that controls how the cache behaves:
  - `tags` - An array of tags to associate with the cached result
  - `revalidate` - The revalidation interval in seconds
  - `argumentsSerializer` - Function to serialize arguments (defaults to `JSON.stringify`)
  - `resultSerializer` - Function to serialize results
  - `resultDeserializer` - Function to deserialize results
  - `responseContext` - The response context object

#### Example

```jsx filename="src/pages/api/api-example.js"
import { neshClassicCache } from "@fortedigital/nextjs-cache-handler/functions";
import axios from "axios";

const cachedAxios = neshClassicCache(async (url) => {
  return (await axios.get(url.href)).data;
});

export default async function handler(request, response) {
  const data = await cachedAxios(
    { revalidate: 5, tags: ["api-data"], responseContext: response },
    new URL("https://api.example.com/data.json"),
  );
  response.json(data);
}
```

## License

Licensed under the [MIT License](./LICENSE), consistent with the original `@neshca/cache-handler`.
