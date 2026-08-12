# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`@fortedigital/nextjs-cache-handler` is a pnpm/turbo monorepo providing custom Next.js `cacheHandler` implementations, focused on Redis-based caching (originally forked from `@neshca/cache-handler`, fully independent since 2.0.0). It's compatible with Next.js 15 and (partially) 16.

## Repo layout

- `packages/nextjs-cache-handler` — the published npm package (`@fortedigital/nextjs-cache-handler`). All library source lives under `src/`.
- `examples/redis-minimal` — a real Next.js app used both as a usage example and as the target of the Playwright e2e suite.
- `examples/redis-cache-components` — another example app.
- `docs/migration` — version migration guides referenced from the README.

## Commands

Run from the repo root (pnpm workspace, orchestrated by Turborepo):

```bash
pnpm install
pnpm build          # turbo build (builds all packages/examples, respecting dependency graph)
pnpm dev             # turbo dev (watch mode)
pnpm lint            # turbo lint
pnpm test            # turbo test (runs jest in packages/nextjs-cache-handler)
pnpm test:e2e        # turbo run test:e2e --filter=redis-minimal (Playwright, needs a prior build)
pnpm format          # turbo format (prettier)
```

Working inside `packages/nextjs-cache-handler` directly:

```bash
npm run build        # tsup --dts-resolve
npm run dev           # tsup --watch
npm test              # jest
npm run test:watch    # jest --watch
npx jest <path>       # run a single test file, e.g. npx jest src/redis-strings.test.ts
npm run lint          # eslint
npm run lint:fix       # prettier --write + eslint --fix
```

Caching only takes effect in Next.js **production** builds — to exercise the example app's caching behavior you must `npm run build && npm run start` inside `examples/redis-minimal`, not `next dev`.

## Architecture

### CacheHandler (`src/handlers/cache-handler.ts`)

`CacheHandler` is a static-state class implementing Next.js's `CacheHandler` interface (`get`/`set`/`revalidateTag`). Key points:

- Consumers call `CacheHandler.onCreation(hook)` once (in their `cache-handler.mjs`) to register an async hook that returns `{ handlers: Handler[], ttl? }`. Next.js instantiates `CacheHandler` itself; construction just stores the `FileSystemCacheContext`, and configuration happens lazily on first `get`/`set`/`revalidateTag` call via `#ensureConfigured` (guarded by a shared in-flight promise so concurrent calls don't double-configure).
- The configured `#mergedHandler` fans `get` out across the underlying `handlers` list **in order**, returning the first hit (falls through on miss/expiry/error to the next handler); `set` and `revalidateTag` fan out to **all** handlers in parallel via `Promise.allSettled`.
- Expired entries (`lifespan.expireAt` in the past) are treated as a miss and trigger a best-effort background delete across all handlers.
- Debug logging throughout is gated on `NEXT_PRIVATE_DEBUG_CACHE` being set (any value) — the `#debug` static flag.
- Special-cases Pages Router routes with `fallback: false` (read from `prerender-manifest.json`) by falling back to reading pre-rendered HTML/JSON directly off disk during `PHASE_PRODUCTION_BUILD`, then seeding the cache handlers from that.
- `FETCH`-kind and (fallback-false) `APP_PAGE`-kind entries are additionally mirrored to the filesystem during `PHASE_PRODUCTION_BUILD` for build-time compatibility.
- Tag derivation differs by `value.kind`: `APP_PAGE` tags come from response headers (`getTagsFromHeaders`), `PAGES` tags get an implicit path tag appended (`getImplicitPathTag`); `FETCH`/others use the tags passed in context as-is.

### Handlers (`src/handlers/*`)

Each handler is a factory function returning an object matching the `Handler` interface (`get`/`set`/`revalidateTag`/optional `delete`/`prepare`), and is exported as a **separate package subpath** (see `exports`/`typesVersions` in `packages/nextjs-cache-handler/package.json`) so consumers only pull in what they use:

- `redis-strings` (`./redis-strings`) — Redis-backed handler using the official `redis` (node-redis) client (or `ioredis` via `helpers/ioredisAdapter`). Stores tag membership in TTL-bound hashmaps to avoid the unbounded shared-tag-map memory leak of the original implementation. Supports a pluggable `valueSerializer` (`serialize`/`deserialize`) for custom wire formats (compression, encryption) — default is JSON; Buffers are normalized to strings before serialize and restored after deserialize. Also supports Redis Cluster via `helpers/redisClusterAdapter` (`withAdapter`).
- `local-lru` (`./local-lru`) — in-memory `lru-cache`-backed handler; dev/test only, explicitly not for production use, but commonly used as a fallback tier.
- `composite` (`./composite`) — routes `get`/`set` across multiple underlying handlers using a caller-supplied `setStrategy(ctx)` to pick a handler index; useful for tiering local-lru + redis-strings.

### Instrumentation (`src/instrumentation`)

`registerInitialCache` (exposed via `./instrumentation` subpath) is invoked from a Next.js `instrumentation.ts` `register()` hook to pre-populate the configured cache handlers from build-time artifacts on server startup. Supports a `setOnlyIfNotExists` option to avoid clobbering values already written at runtime by another instance.

### Types

`cache-handler.types.ts` re-derives/narrows types from Next.js's internal (non-public) `next/dist/server/...` modules — when bumping the Next.js peer dependency version, these are the first place type breakage will surface.

## Testing

- Unit tests are colocated with source as `*.test.ts` under `packages/nextjs-cache-handler/src/` (jest + ts-jest, `testEnvironment: "node"`).
- E2E tests live in `examples/redis-minimal/e2e` (Playwright) and exercise real caching behavior (fetch tag revalidation, ISR, `unstable_cache`, static params, etc.) against a built Next.js app backed by Redis — run via `pnpm test:e2e` from the root, which builds first (`test:e2e` depends on `^build`/`build` per `turbo.json`).
- Use the `new-example` skill (`.claude/skills/new-example/SKILL.md`) to scaffold a new caching example page in `examples/redis-minimal`, and the `new-e2e-test` skill (`.claude/skills/new-e2e-test/SKILL.md`) to add its Playwright coverage. These are commonly used together — a new example is only complete once it has e2e coverage proving the cache behavior it demonstrates, and writing that coverage needs page-specific facts (path, tags, `cacheLife` profile, expected reload behavior) that only whoever built the example page knows.

## Legacy project docs

Most of the original `@neshca/cache-handler` documentation (https://caching-tools.github.io/next-shared-cache/, source at https://github.com/caching-tools/next-shared-cache) still applies conceptually, since this package inherited its `Handler` interface, tag/TTL model, and overall caching approach from it. If a request needs cache-handler background or detail not covered by this repo's own README/CLAUDE.md/source, use the `legacy-cache-handler-docs` skill (`.claude/skills/legacy-cache-handler-docs/SKILL.md`) rather than guessing.

## Compatibility notes worth knowing before making changes

- Only the official `redis` (node-redis) package is supported by `redis-strings`; `ioredis` requires going through `helpers/ioredisAdapter` first — don't assume ioredis client methods work directly.
- Next.js 16 changed `revalidateTag` to require a `cacheLife` parameter and added `updateTag`; Next.js 15 vs 16 have diverging cache-invalidation APIs — check the compatibility matrix in `README.md` before assuming behavior is shared across major versions.
- `'use cache'` directive, `cacheComponents`, and the `cacheHandlers` config are **not** supported yet — don't design changes assuming they work.
- Changing a handler's `valueSerializer`/compression is a breaking change for existing Redis data (existing keys become unreadable) — this needs a key prefix bump or cache flush strategy, not a silent behavior change.
