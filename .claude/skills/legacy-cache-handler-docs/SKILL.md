---
name: legacy-cache-handler-docs
description: Look up documentation and source from the legacy @neshca/cache-handler project (caching-tools/next-shared-cache) that this repo was originally forked from. Use this when a question about cache handler behavior, options, or APIs can't be fully answered from this repo's own README/CLAUDE.md/source, since most of the original docs are still accurate for the concepts this package inherited (handler interface, tag/TTL model, fetch/pages/app-router caching mechanics, etc.).
---

# Legacy `@neshca/cache-handler` docs lookup

This package (`@fortedigital/nextjs-cache-handler`) was originally built on top of
`@neshca/cache-handler` and wrapped/extended it through version 1.x. As of `2.0.0` it is a fully
independent implementation with no runtime dependency on the original package, but a lot of the
original project's documentation and conceptual model (the `Handler` interface, tag-based
revalidation, TTL/lifespan semantics, fetch cache vs. page cache handling, etc.) still applies.

Use this skill when:

- A question is about cache handler concepts, options, or behavior that isn't fully documented in
  this repo's `README.md` or `CLAUDE.md`.
- You need historical/background context on *why* something is shaped the way it is (this repo's
  `CacheHandler` class, handler contract, etc. are direct descendants of the original design).
- You want to compare this package's behavior against the upstream project to spot intentional
  divergences (this repo's `README.md` documents some, e.g. TTL-bound tag hashmaps replacing the
  original's unbounded shared tag map, and the Next.js 16 compatibility matrix).

Do NOT use this skill as the first stop for questions fully answered by this repo's own source or
`CLAUDE.md` — check those first, since this package has diverged in real ways (no dependency on
`@neshca/cache-handler`, different tag-storage implementation, added handlers like `composite`,
different Next.js 15/16 support matrix). Treat the legacy docs as background/reference, not as an
authoritative description of this package's current behavior.

## Where to look

- Documentation site: https://caching-tools.github.io/next-shared-cache/
- Source repository: https://github.com/caching-tools/next-shared-cache

Useful documentation pages (fetch via WebFetch as needed; the site is a Docusaurus/VitePress-style
static site, so most content pages are reachable as `https://caching-tools.github.io/next-shared-cache/<path>`):

- `/` — project overview and quick start
- `/handlers` (and its subpages) — the built-in handler implementations (Redis strings, Redis
  stack, local LRU, etc.) and their configuration options
- `/configuration` — `CacheHandler.onCreation`, TTL/`estimateExpireAge` options, general setup
- `/api-reference` — public API surface (types, functions)
- `/experimental` — experimental features that may map to functionality now considered stable or
  removed here

If a docs page doesn't have the needed detail, fetch the corresponding source file from the GitHub
repo (e.g. under `packages/*/src`) via WebFetch on the `github.com` URL, or via the GitHub API
(`https://api.github.com/repos/caching-tools/next-shared-cache/...`) for directory listings/search.

## How to use findings

1. Prefer this repo's own source and docs when they cover the topic — the legacy project is a
   reference for concepts and rationale, not a spec for current behavior.
2. When citing something from the legacy project in an answer, say explicitly that it comes from
   the original `@neshca/cache-handler` project, and flag if you're not sure whether the behavior
   still applies to `@fortedigital/nextjs-cache-handler` (e.g. it predates the 2.0.0 independence
   cut, or predates Next.js 15/16 support).
3. If the legacy docs and this repo's docs conflict, trust this repo — call out the discrepancy
   rather than silently picking one.
