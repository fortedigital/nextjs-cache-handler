---
name: new-e2e-test
description: Add a Playwright e2e test for a caching example in examples/redis-minimal, following this repo's existing spec patterns (build-timestamp helper, revalidatePath/revalidateTag helpers, test.step structure).
---

# Add a new e2e cache-behavior test

Use this skill when asked to add or extend Playwright e2e coverage for a caching example under
`examples/redis-minimal`.

## Where things live

- Specs: `examples/redis-minimal/e2e/*.spec.ts` — currently `path-revalidation.spec.ts` (grouped
  under `test.describe('App Router', ...)` / `test.describe('Pages Router', ...)`, one test per
  example page verifying `revalidatePath`) and `tag-revalidation.spec.ts` (one test per
  tag/cacheLife combination verifying `revalidateTag`/`updateTag`-driven invalidation).
- Shared helpers: `examples/redis-minimal/e2e/helpers/cache.ts`:
  - `BUILD_TIMESTAMP_TEST_ID = 'build-timestamp'`
  - `getBuildTimestamp(page)` — reads the first `data-testid="build-timestamp"` element's text.
  - `waitForBuildTimestamp(page)` — waits until that element is non-empty (page finished
    rendering) before reading it.
  - `revalidatePath(request, path)` — `POST /api/revalidate { path }`, asserts `response.ok()`.
  - `revalidateTag(request, tag, cacheLife = 'max')` — `POST /api/revalidate { tag, cacheLife }`,
    asserts `response.ok()`.
- Config: `playwright.config.ts` — `testDir: './e2e'`, `fullyParallel: false`, `workers: 1` (tests
  share one Redis-backed app instance, so don't assume isolation between specs), starts the app
  itself via `webServer: { command: "pnpm start --port 3000", ... }` against
  `REDIS_URL`/`REDIS_TYPE` env vars — this means the app must already be **built** (`next build`)
  before running tests; `pnpm test:e2e` from the repo root handles this via `turbo`'s `dependsOn`.

## The canonical test shape

Every test follows the same three-step arrange/act/assert pattern using `test.step` for readable
traces:

```ts
test('/examples/<name> revalidates via <mechanism>', async ({ page, request }) => {
  const path = '/examples/<name>';
  let first: string | null;

  await test.step('Load page and capture build timestamp', async () => {
    await page.goto(path);
    await waitForBuildTimestamp(page);
    first = await getBuildTimestamp(page);
  });

  await test.step('Reload without revalidation keeps timestamp', async () => {
    await page.reload();
    await waitForBuildTimestamp(page);
    expect(await getBuildTimestamp(page)).toBe(first);
  });

  await test.step('revalidate<X> updates timestamp on reload', async () => {
    await revalidatePath(request, path); // or revalidateTag(request, tag, cacheLife)
    await page.reload();
    await waitForBuildTimestamp(page);
    expect(await getBuildTimestamp(page)).not.toBe(first);
  });
});
```

Adapt the middle step to the scenario's actual caching semantics instead of assuming plain
"reload keeps timestamp" for every case:

- **`no-store` / always-fresh pages**: expect the timestamp to change on a bare reload with *no*
  revalidation call (see `/examples/no-store` in `path-revalidation.spec.ts`) — capture a `second`
  timestamp and assert it differs from `first`, then assert a further revalidation changes it
  again from `second`.
- **Short time-based revalidation** (`revalidate: N` seconds): if `N` is short enough to fit in a
  test (see the 5s window in `static-params/[testName]/page.tsx`), you can either poll/wait for
  the window to elapse and assert a natural change, or (preferred, faster) trigger the change
  explicitly via `revalidatePath`/`revalidateTag` rather than sleeping in the test.
- **On-demand ISR / ISR with `dynamicParams`**: an immediate reload should still return the
  cached timestamp (no implicit background regeneration on every hit) — assert `toBe(first)`
  before the explicit revalidation step, as in the `/examples/isr/blog/1` and
  `/examples/static-params/test1` tests.
- **Tag-based revalidation / `updateTag`**: use `revalidateTag(request, tag, cacheLife)` — pick
  the `cacheLife` (`'max' | 'hours' | 'days'`) that matches what the example page actually
  registered the tag with; a mismatched profile can no-op the revalidation. Group these tests in
  `tag-revalidation.spec.ts`, one `test()` per tag (a page can carry more than one tag — e.g.
  `/examples/update-tag` has both `posts` and `user-profile` — write a separate test per tag so
  failures pinpoint which tag's invalidation broke).

## Before writing the test, get the specifics right

You need exact, page-specific facts — do not guess them:

- The page's **path** (including any dynamic segment value you're testing against, e.g.
  `/examples/isr/blog/1`).
- Whether it's App Router (add to `path-revalidation.spec.ts`'s `'App Router'` describe block or a
  new file) or Pages Router (`'Pages Router'` describe block).
- The exact **tag string(s)** and, for Next.js 16 tag revalidation, the **`cacheLife` profile**
  the page actually used when calling `fetch`/`unstable_cache`/`updateTag` — read the page source
  rather than assuming `'max'`.
- Whether the page is expected to hold a cached timestamp across a plain reload, always change
  (`no-store`), or only change after a specific revalidation call — this determines which
  assertion shape from the list above applies. If this isn't obvious from the page, don't assume;
  check the source or ask.
- Confirm the page renders a `data-testid="build-timestamp"` element — if it doesn't, the test
  cannot use the shared helpers and the page itself needs that testid added first.

## Running

```bash
pnpm test:e2e                     # from repo root: builds, then runs against filter=redis-minimal
cd examples/redis-minimal && npx playwright test <file>.spec.ts   # single file, needs prior build+running Redis
```

Requires a reachable Redis at `REDIS_URL` (defaults to `redis://127.0.0.1:6379`) — start one
locally (e.g. `docker run -p 6379:6379 redis`) before running e2e tests outside CI.
