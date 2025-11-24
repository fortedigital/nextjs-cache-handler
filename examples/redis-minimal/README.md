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

## Examples

The application includes several examples demonstrating different Next.js caching features:

### 1. Home Page (`/`)

Overview page listing all available examples with descriptions and features.

### 2. Fetch with Tags (`/examples/fetch-tags`)

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

### 3. ISR with Static Params (`/examples/isr/blog/[id]`)

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

### 5. Static Params Test (`/examples/static-params/[testName]`)

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

### Clear Cache (`/api/cache`)

Clears cache for a specific tag.

**Usage:**

- `GET /api/cache?tag=futurama` - Clears cache for the "futurama" tag
- Default tag is "futurama" if not specified

**Example:**

```bash
curl http://localhost:3000/api/cache?tag=futurama
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
