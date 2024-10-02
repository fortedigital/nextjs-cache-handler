![NPM Version](https://img.shields.io/npm/v/%40fortedigital%2Fnextjs-cache-handler)

# @fortedigital/nextjs-cache-handler

This package extends the functionality of [`@neshca/cache-handler`](https://www.npmjs.com/package/@neshca/cache-handler) by providing additional cache handlers for specialized use cases, specifically for Redis-based caching solutions. The original `@neshca/cache-handler` offers a robust caching API for Next.js applications, and this package introduces two new handlers for managing Redis cache with different expiration strategies and tag-based revalidation.

## Installation

To install this package along with its dependencies:

```bash
npm install @fortedigital/nextjs-cache-handler
```

Package depends on the original `@neshca/cache-handler` package - you can use anything provided by it by using import/require from `@neshca/cache-handler`.

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

## Reference to Original Package

This package builds upon the core functionality provided by [`@neshca/cache-handler`](https://www.npmjs.com/package/@neshca/cache-handler). You can find more information about the core library, including usage examples and API documentation, at the [official documentation page](https://caching-tools.github.io/next-shared-cache).

## License

This project is licensed under the [MIT License](./LICENSE), as is the original `@neshca/cache-handler` package.
