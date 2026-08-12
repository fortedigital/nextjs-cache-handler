<!--
Thanks for contributing! This project has limited maintainer bandwidth, so PRs that are
self-contained, tested, and clearly described get reviewed and merged much faster than
ones that require the maintainer to reproduce, test, or scope things down. Please fill
this in - don't delete sections, but it's fine to write "N/A" where something doesn't apply.
-->

## What does this PR do, and why?

<!-- Link the issue this addresses, e.g. "Fixes #123" / "Addresses #123". If there's no issue, explain the problem this solves and how you found it (repro steps, prod symptom, etc.). -->

## Type of change

- [ ] Bug fix
- [ ] New feature / handler
- [ ] Breaking change (existing Redis data or public API becomes incompatible - e.g. changing a handler's `valueSerializer`/wire format, key layout, or an exported type/interface)
- [ ] Docs / examples only
- [ ] Infra / chore (CI, tooling, deps)

If this is a **breaking change**, describe the migration path (key prefix bump, README callout, `docs/migration` entry, etc.):

## How was this verified?

This package's core value is "don't break caching behavior" - a change that passes `tsc`/lint but silently breaks tag revalidation or ISR is worse than no change. Please verify at both levels below rather than relying on unit tests alone.

**Automated tests**
- [ ] Added/updated unit tests in `packages/nextjs-cache-handler/src/**/*.test.ts` (`npm test` / `npx jest <file>`)
- [ ] Added/updated Playwright e2e coverage in `examples/redis-minimal/e2e` (`pnpm test:e2e`) - required for anything that changes caching/revalidation behavior observable by a running app
- [ ] Added/updated the isolated `registerInitialCache` suite (`pnpm test:e2e:isolated`, requires Docker) if this touches build-time cache hydration
- [ ] N/A - explain why (e.g. docs-only change):

**Manual verification against the example app(s)**
<!-- Reviewers have repeatedly had to bounce PRs back because they didn't work against examples/redis-minimal when actually run. Please do this before requesting review, not after. -->
- [ ] Ran `npm run build && npm run start` in `examples/redis-minimal` (or `examples/redis-cache-components` if relevant) and exercised the affected page(s)/route(s) by hand
- [ ] If behavior is observable in Redis (keys, TTLs, tags), inspected Redis directly (or via `NEXT_PRIVATE_DEBUG_CACHE=1` logs) to confirm the expected state
- [ ] N/A - explain why:

What did you actually do to verify it, and what did you observe? (commands run, pages visited, before/after logs or Redis state - screenshots welcome). For performance/behavior claims (hit-rate, race conditions, etc.), include concrete before/after numbers or logs, not just theoretical reasoning - several past PRs based purely on code inspection turned out not to fix the reported symptom in practice.

## Compatibility

- [ ] Checked against the Next.js version matrix in `README.md` (does this change apply to 15, 16, or both?)
- [ ] If this touches `redis-strings`: confirmed it works with the official `redis` (node-redis) client; if `ioredis`-specific, went through `helpers/ioredisAdapter` rather than assuming ioredis methods work directly
- [ ] `pnpm lint` and `pnpm build` pass locally

## Scope check

- [ ] This PR is limited to the change described above (no unrelated formatting/refactor/example-app noise mixed in)
- [ ] Updated `README.md` / `docs/migration` if this changes public behavior or requires user action
- [ ] Marked as **Draft** if this is still work-in-progress and you want early feedback rather than a full review
