---
name: new-example
description: Scaffold a new caching example page in examples/redis-minimal (e.g. a new fetch/unstable_cache/ISR/tag-revalidation scenario), following this repo's existing example conventions (ExampleLayout, InfoCard, CodeBlock, revalidation buttons, Navigation entry).
---

# Add a new caching example

Use this skill when asked to add a new example page/scenario to `examples/redis-minimal` (the
example Next.js app used to demonstrate `@fortedigital/nextjs-cache-handler` caching behaviors).

## Where things live

All examples are App Router pages under `examples/redis-minimal/src/app/examples/<name>/page.tsx`
(dynamic segments use `[param]`, e.g. `examples/isr/blog/[id]/page.tsx`,
`examples/static-params/[testName]/page.tsx`). Pages Router examples (rarer, used to demonstrate
legacy routing) live under `src/pages/examples/...`.

Shared building blocks (reuse, don't reinvent):

- `@/components/ExampleLayout` — wraps every example page: `title`, `description`, optional
  `actions` (buttons shown top-right).
- `@/components/InfoCard` — the blue "How it works" bullet list explaining the behavior.
- `@/components/CodeBlock` — renders a fenced code snippet mirroring the page's actual caching
  code, for the reader to copy.
- `@/components/ClearCacheButton` (`tag`, optional `label`) — client button that calls
  `GET /api/revalidate?tag=<tag>&cacheLife=max` then reloads.
- `@/components/RevalidatePathButton` (`path`, optional `label`) — client button that
  `POST`s `{ path }` to `/api/revalidate` then reloads.
- `@/components/RevalidateTagButton` (`tag`, optional `cacheLife`, `label`) — client button that
  `POST`s `{ tag, cacheLife }` to `/api/revalidate` then reloads.
- `src/app/api/revalidate/route.ts` already handles both tag-based (`GET ?tag=&cacheLife=` or
  `POST { tag, cacheLife }`) and path-based (`POST { path, type? }`) revalidation generically —
  you should not need to touch it for a new example unless it needs an entirely new invalidation
  primitive.

## Conventions every example page follows

1. **Server component**, `async function`, fetches or computes data at render time (usually from
   a public demo API like `https://api.sampleapis.com/futurama/characters/<id>` or
   `https://jsonplaceholder.typicode.com/...` — reuse an existing data source unless the scenario
   specifically needs another one).
2. Wraps a `const timestamp = new Date().toISOString();` captured once per render, and renders it
   inside a `data-testid="build-timestamp"` span:
   ```tsx
   <span data-testid="build-timestamp" className="text-gray-900 dark:text-gray-100 font-mono text-sm">
     {timestamp}
   </span>
   ```
   This is the load-bearing convention of the whole example app — it's the only signal used to
   prove whether a page served a cached render or a fresh one. **Every new example must include
   exactly this pattern** (one `build-timestamp` per page; if a page shows two cache strategies
   side by side, either give each its own distinct testid or make sure only one authoritative
   timestamp represents the cache being demonstrated).
3. Wrap risky fetches in `try/catch` and render a `text-red-600 dark:text-red-400` error message
   inside `ExampleLayout` on failure, matching the existing pages.
4. Sets the actual Next.js caching primitive being demonstrated explicitly and visibly — `fetch`
   with `next: { revalidate, tags }`, `cache: "no-store"`, `unstable_cache(...)`, route segment
   config (`export const revalidate = ...`, `dynamicParams`, `generateStaticParams`), or
   `updateTag`/`revalidateTag` in a Server Action — and mirrors that exact code in a `CodeBlock`
   at the bottom of the page so the rendered example and the documented snippet never drift apart.
5. Body layout is a `<div className="space-y-6">` containing, in order: `InfoCard` ("How it
   works"), a data section (`bg-gray-50 dark:bg-gray-900 rounded-lg p-4` cards) showing the fetched
   data plus a "Cache Information" card (revalidation period, tags, strategy), then the
   `CodeBlock` code example. Follow the existing pages' Tailwind classes for visual consistency
   rather than inventing new styles.
6. If the example should be independently invalidated from the demo UI, pass the right button as
   `actions` on `ExampleLayout` — `ClearCacheButton` for a tag, `RevalidatePathButton` for the
   page's own path, or both if it demonstrates two invalidation mechanisms (see
   `unstable-cache/page.tsx`).
7. Register the new page in `@/components/Navigation.tsx`'s `examples` array (`href`, `label`,
   short `description`) so it's reachable from the sidebar. If it's a Pages Router example, also
   check `@/components/NavigationPages.tsx`.
8. If the new example needs a new external data type, add it under
   `examples/redis-minimal/src/types/` rather than inlining ad-hoc interfaces duplicated across
   pages (though small one-off `interface Post { ... }` blocks scoped to a single page, as seen in
   `isr/blog/[id]/page.tsx`, are also acceptable and match existing precedent).

## Make the example verifiable, not just visible

A new example is only useful if its caching behavior can be asserted from outside the page, not
just eyeballed. Beyond the `build-timestamp` testid (point 2 above), think about what a test would
need to prove the scenario actually works:

- Exact **path** of the new page (tests navigate by path).
- Any **cache tag(s)** used, and which `cacheLife` profile (`max`/`hours`/`days`) applies if using
  Next.js 16 `revalidateTag`/`updateTag` — a test can only invalidate a tag it knows about.
- Whether the page is expected to keep the same timestamp across a plain reload (cached), always
  change (e.g. `no-store`), or change only after a specific action (`revalidatePath`,
  `revalidateTag`, `updateTag`, or waiting out a short `revalidate` window).
- Whether the scenario needs a *short* revalidation window (seconds, not hours/days) to be
  practically testable without long waits — prefer short windows for anything time-based, matching
  `static-params/[testName]/page.tsx` (`revalidate = 5`) rather than realistic production values.

State these explicitly when you're done (path, tag/cacheLife if any, expected reload behavior) so
that whoever writes the corresponding e2e coverage has exactly what they need without having to
re-read the page source to infer it.

## After scaffolding

Run `npm run build && npm run start` (caching is disabled in `next dev`) inside
`examples/redis-minimal` to manually confirm the new page renders and its cache behaves as
described before considering the example done.
