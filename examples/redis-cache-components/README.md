# Next.js Cache Components Examples

This example application demonstrates Next.js 16 Cache Components features using Redis as a remote cache handler. Cache Components is a new caching model that differs significantly from the traditional Next.js cache API.

## What is Cache Components?

Cache Components is an opt-in feature in Next.js 16 that provides:

- **Partial Prerendering (PPR)**: Creates a static HTML shell with dynamic content streaming in
- **Component-level caching**: Use `'use cache'` directive instead of route segment configs
- **New APIs**: `cacheLife`, `cacheTag`, `'use cache: remote'` replace traditional cache APIs
- **Different behavior**: Many traditional cache APIs don't work the same way

## Key Differences from Traditional Cache API

| Feature | Traditional API | Cache Components |
|---------|----------------|------------------|
| Configuration | `cacheHandler` in next.config | `cacheComponents: true` + `cacheHandlers.remote` |
| Caching directive | Route segment configs (`revalidate`, `dynamic`) | `'use cache'` directive |
| Cache expiration | `revalidate` number | `cacheLife('max' \| 'hours' \| 'days')` |
| Cache tags | `next.tags` in fetch | `cacheTag()` function |
| Prerendering | Static or dynamic pages | Partial prerendering with Suspense |

## Getting Started

First, install dependencies:

```bash
npm i
```

### Important: Development vs Production Mode

**Cache Components works in both development and production mode**, unlike the traditional cache handler which only works in production.

To test caching functionality:

```bash
npm run build
npm run start
```

For development:

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

## Configuration

### Environment Variables

Modify the `.env` file if you need to configure Redis connection settings. The default Redis URL is used if not specified.

### Redis Client Configuration

The example supports both Redis clients:

- **@redis/client** (default): Set `REDIS_TYPE="redis"` or leave unset
- **ioredis**: Set `REDIS_TYPE="ioredis"`

### Next.js Configuration

Cache Components requires specific configuration in `next.config.ts`:

```typescript
const nextConfig = {
  cacheComponents: true,
  cacheHandlers: {
    remote: require.resolve("./cache-handler.mjs"),
  },
};
```

## Examples

The application includes several examples demonstrating Cache Components features:

### 1. Home Page (`/`)

Overview page listing all available examples with descriptions and features.

### 2. use cache Directive (`/examples/use-cache`)

Demonstrates the basic `'use cache'` directive for caching component data.

**Features:**
- Component-level caching
- Automatic inclusion in static shell
- Prerendering support
- Perfect for data that doesn't change frequently

**Try it:**
- Visit `/examples/use-cache` to see cached data
- The timestamp will remain the same on subsequent requests
- Data is cached until expiration or manual invalidation

### 3. use cache: remote (`/examples/use-cache-remote`)

Shows how to use `'use cache: remote'` directive with Redis for remote caching.

**Features:**
- Remote caching with Redis
- `cacheHandlers.remote` configuration
- Shared cache across instances
- Perfect for distributed deployments

**Try it:**
- Visit `/examples/use-cache-remote` to see remote cached data
- The cache is shared across all application instances
- Data persists in Redis

### 4. cacheLife (`/examples/cache-life`)

Demonstrates cache expiration using `cacheLife` function.

**Features:**
- Cache expiration with `cacheLife`
- Different profiles: `'max'`, `'hours'`, `'days'`
- Replaces route segment `revalidate`
- Component-level cache control

**Try it:**
- Visit `/examples/cache-life` to see different cacheLife profiles
- Compare the behavior of different profiles
- See code examples for migration from traditional API

### 5. cacheTag (`/examples/cache-tag`)

Shows how to use `cacheTag` for cache invalidation.

**Features:**
- Cache tagging with `cacheTag`
- Selective cache invalidation
- Tag-based revalidation
- Perfect for content management

**Try it:**
- Visit `/examples/cache-tag` to see tagged cached data
- Click "Revalidate Tag" to invalidate the cache
- Reload the page to see fresh data

### 6. Suspense Boundaries (`/examples/suspense-boundaries`)

Demonstrates Partial Prerendering with Suspense boundaries.

**Features:**
- Partial Prerendering (PPR)
- Static shell with streaming content
- Suspense boundaries for dynamic data
- Fast initial page loads

**Try it:**
- Visit `/examples/suspense-boundaries` to see PPR in action
- Notice how static content appears immediately
- Dynamic content streams in after a delay

## Cache Handler

This example uses a custom Redis cache handler configured in `cache-handler.mjs` for the `cacheHandlers.remote` configuration.

The cache handler implements:
- `get(key)`: Retrieve cached value from Redis
- `set(key, value)`: Store value in Redis
- `delete(key)`: Remove value from Redis

**Note:** The Cache Components cache handler API is different from the traditional Next.js cache handler API. It's a simpler interface designed specifically for `'use cache: remote'` directive.

## Migration Guide

### From Traditional Cache API to Cache Components

**Before (Traditional API):**
```typescript
export const revalidate = 3600;

export default async function Page() {
  const data = await fetch("https://api.example.com/data");
  return <div>{data}</div>;
}
```

**After (Cache Components):**
```typescript
import { cacheLife } from "next/cache";

export default async function Page() {
  "use cache";
  cacheLife("hours");
  
  const data = await fetch("https://api.example.com/data");
  return <div>{data}</div>;
}
```

### Route Segment Configs

When using Cache Components, several route segment configs are no longer needed:

- `dynamic = "force-dynamic"` - Not needed (all pages are dynamic by default)
- `dynamic = "force-static"` - Remove and use `'use cache'` instead
- `revalidate` - Replace with `cacheLife()`
- `fetchCache` - Not needed (use `'use cache'` to control caching)
- `runtime = 'edge'` - Not supported (Cache Components requires Node.js)

## Technologies

- Next.js 16.1.3
- React 19
- TypeScript
- Tailwind CSS
- Redis
- @fortedigital/nextjs-cache-handler

## Further Reading

- [Next.js Cache Components Documentation](https://nextjs.org/docs/app/getting-started/cache-components)
- [use cache Directive](https://nextjs.org/docs/app/api-reference/directives/use-cache)
- [use cache: remote Directive](https://nextjs.org/docs/app/api-reference/directives/use-cache-remote)
- [cacheLife Function](https://nextjs.org/docs/app/api-reference/functions/cacheLife)
- [cacheTag Function](https://nextjs.org/docs/app/api-reference/functions/cacheTag)

