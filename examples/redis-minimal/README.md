# Next.js Cache Handler Examples

This example application demonstrates various Next.js caching functionalities using the Redis cache handler. It provides a comprehensive UI to explore and test different caching strategies.

## Getting Started

First, install dependencies:

```bash
npm i
```

### Important: Development vs Production Mode

**Next.js does not use the cache handler in development mode.** This is a Next.js limitation - caching is intentionally disabled in dev mode for faster hot reloading and to ensure developers always see fresh data.

To test caching functionality, you must use **production mode**:

```bash
npm run build
npm run start
```

For development (without caching):

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

## Configuration

Modify the `.env` file if you need to configure Redis connection settings. The default Redis URL is used if not specified.

### Redis Client Configuration

The example supports both Redis clients:

- **@redis/client** (default): Set `REDIS_TYPE="redis"` or leave unset
- **ioredis**: Set `REDIS_TYPE="ioredis"`

This allows you to test the `ioredisAdapter` functionality.

## Examples

The application includes several examples demonstrating different Next.js caching features:

### 1. Home Page (`/`)

Overview page listing all available examples with descriptions and features.

### 2. Default Cache (`/examples/default-cache`)

Demonstrates the default fetch caching behavior with `force-cache`.

**Features:**

- Default caching behavior (indefinite cache duration)
- Perfect for static or rarely-changing data
- Shows how Next.js caches by default

**Try it:**

- Visit `/examples/default-cache` to see cached data
- The timestamp will remain the same on subsequent requests
- Data is cached until manually cleared or build is redeployed

### 3. No Store (`/examples/no-store`)

Shows fetch with `no-store` option, which always fetches fresh data.

**Features:**

- Never caches responses
- Always fetches fresh data from API
- Perfect for real-time or user-specific data
- Timestamp changes on every request

**Try it:**

- Visit `/examples/no-store` to see fresh data on every load
- Refresh the page multiple times - timestamp changes each time
- Compare with other caching strategies

### 4. Time-based Revalidation (`/examples/time-based-revalidation`)

Shows fetch with time-based revalidation (standalone, without tags).

**Features:**

- Automatic revalidation after specified time (30 seconds)
- Balances freshness with performance
- Standalone example of `next.revalidate`

**Try it:**

- Visit `/examples/time-based-revalidation` to see cached data
- Refresh within 30 seconds - timestamp stays the same
- Wait 30+ seconds and refresh - timestamp updates

### 5. Fetch with Tags (`/examples/fetch-tags`)

Demonstrates fetch caching with tags and time-based revalidation.

**Features:**

- Time-based revalidation (24 hours)
- Cache tags for selective invalidation
- Clear cache button to test tag revalidation
- Shows character data from Futurama API
- Displays cache information and rendered timestamp

**Try it:**

- Visit `/examples/fetch-tags` to see cached data
- Click "Clear Cache" to invalidate the cache
- Reload the page to see fresh data

### 6. unstable_cache (`/examples/unstable-cache`)

Demonstrates persistent caching with `unstable_cache` for function results.

**Features:**

- Cache any function, not just fetch requests
- Tags and revalidation support
- Side-by-side comparison with fetch caching
- Perfect for database queries and computations
- Shows when to use unstable_cache vs fetch

**Try it:**

- Visit `/examples/unstable-cache` to see both caching methods
- Compare the timestamps and behavior
- Click "Clear Tag Cache" to invalidate both caches
- Understand when to use unstable_cache vs fetch

### 7. revalidateTag() with cacheLife (`/examples/revalidate-tag-cachelife`)

Demonstrates the updated `revalidateTag()` API in Next.js 16 with cacheLife profiles.

**Features:**

- Breaking change from Next.js 15 (cacheLife now required)
- Different cacheLife profiles: 'max', 'hours', 'days'
- Stale-while-revalidate behavior
- Examples for each profile type
- Code examples showing migration from Next.js 15

**Try it:**

- Visit `/examples/revalidate-tag-cachelife` to see all three profiles
- Click "Revalidate" buttons to test each profile
- Compare the behavior of different cacheLife profiles
- See code examples for Next.js 15 vs Next.js 16

### 8. ISR with Static Params (`/examples/isr/blog/[id]`)

Incremental Static Regeneration with `generateStaticParams`.

**Features:**

- Static generation at build time
- On-demand regeneration
- Time-based revalidation (1 hour)
- Multiple blog post routes

**Try it:**

- Visit `/examples/isr/blog/1` for the first post
- Try different IDs like `/examples/isr/blog/2`, `/examples/isr/blog/3`
- Check the rendered timestamp to see caching in action

### 9. Static Params Test (`/examples/static-params/[testName]`)

Tests static params generation with dynamic routes.

**Features:**

- Static params generation
- Dynamic params support
- Short revalidation period (5 seconds) for testing
- Shows generation type (static vs dynamic)

**Try it:**

- Visit `/examples/static-params/cache` (pre-generated)
- Try `/examples/static-params/test1` or `/examples/static-params/test2` (on-demand)

## API Routes

### Cache Revalidation (`/api/revalidate`)

Unified endpoint for revalidating cache by tag or path.

**Usage:**

**Tag-based revalidation (GET):**

- `GET /api/revalidate?tag=futurama` - Revalidates cache for the "futurama" tag with 'max' profile
- `GET /api/revalidate?tag=futurama&cacheLife=hours` - Revalidates with 'hours' profile
- `GET /api/revalidate?tag=futurama&cacheLife=days` - Revalidates with 'days' profile

**Tag-based revalidation (POST):**

- `POST /api/revalidate` with body `{ "tag": "futurama" }` - Revalidates cache for a tag (defaults to 'max')
- `POST /api/revalidate` with body `{ "tag": "futurama", "cacheLife": "hours" }` - Revalidates with specific profile

**Path-based revalidation (POST):**

- `POST /api/revalidate` with body `{ "path": "/examples/default-cache" }` - Revalidates cache for a specific path

**Examples:**

```bash
# Revalidate by tag (GET) - defaults to 'max' profile
curl http://localhost:3000/api/revalidate?tag=futurama

# Revalidate by tag with specific profile (GET)
curl http://localhost:3000/api/revalidate?tag=futurama&cacheLife=hours

# Revalidate by tag (POST) - defaults to 'max' profile
curl -X POST http://localhost:3000/api/revalidate \
  -H "Content-Type: application/json" \
  -d '{"tag": "futurama"}'

# Revalidate by tag with specific profile (POST)
curl -X POST http://localhost:3000/api/revalidate \
  -H "Content-Type: application/json" \
  -d '{"tag": "futurama", "cacheLife": "days"}'

# Revalidate by path (POST)
curl -X POST http://localhost:3000/api/revalidate \
  -H "Content-Type: application/json" \
  -d '{"path": "/examples/default-cache"}'
```

## Cache Handler

This example uses a custom Redis cache handler configured in `cache-handler.mjs`. The handler supports:

- Redis string-based caching
- Local LRU fallback
- Composite caching strategy
- Tag-based cache invalidation

**Note:** The cache handler only works in production mode. In development mode, Next.js bypasses the cache handler entirely. You'll see a warning message in the console: `"Next.js does not use the cache in development mode. Use production mode to enable caching."`

## Technologies

- Next.js 16
- React 19
- TypeScript
- Tailwind CSS
- Redis
- @fortedigital/nextjs-cache-handler
