import { test, expect } from "@playwright/test";
import { createClient } from "redis";
import {
  startEphemeralRedis,
  stopEphemeralRedis,
  waitForRedisReady,
  waitForRedisPopulated,
  startIsolatedNextServer,
  stopIsolatedNextServer,
  type IsolatedRedis,
  type IsolatedNextServer,
} from "./helpers/isolated-server";

// Two seeded paths deliberately span different `registerInitialCache` code
// paths (setPageCache(..., "app", ...) vs setPageCache(..., "pages", ...)) -
// setOnlyIfNotExists is a single global flag for the whole run (see
// register-initial-cache.ts), not something scoped to one specific
// cachePath, so this proves the NX guard is honored uniformly regardless of
// router kind, not just for one hardcoded App Router page.
const SEEDED_APP_ROUTER_CACHE_PATH = "nextjs:/examples/default-cache";
const SEEDED_PAGES_ROUTER_CACHE_PATH = "nextjs:/posts/1";

function makeSentinel(label: string) {
  return JSON.stringify({
    lastModified: 1,
    lifespan: null,
    tags: [],
    value: { kind: "SENTINEL", __test: label },
  });
}

const APP_ROUTER_SENTINEL = makeSentinel("app-router");
const PAGES_ROUTER_SENTINEL = makeSentinel("pages-router");

test.describe("registerInitialCache with setOnlyIfNotExists=true does not clobber existing keys", () => {
  let redis: IsolatedRedis;
  let server: IsolatedNextServer;

  test.beforeAll(async () => {
    redis = await startEphemeralRedis();
    await waitForRedisReady(redis.url);

    // Pre-seed BEFORE starting next start, so registerInitialCache sees existing keys.
    const seedClient = createClient({ url: redis.url });
    await seedClient.connect();
    await seedClient.set(SEEDED_APP_ROUTER_CACHE_PATH, APP_ROUTER_SENTINEL);
    await seedClient.set(SEEDED_PAGES_ROUTER_CACHE_PATH, PAGES_ROUTER_SENTINEL);
    await seedClient.quit();

    server = await startIsolatedNextServer({
      env: {
        REDIS_URL: redis.url,
        REDIS_TYPE: "redis",
        INITIAL_CACHE_SET_ONLY_IF_NOT_EXISTS: "true",
      },
    });

    await waitForRedisPopulated(
      redis.url,
      ["/examples/default-cache", "/examples/fetch-tags", "/posts/1"],
      { timeoutMs: 60_000 },
    );
  });

  test.afterAll(async () => {
    await stopIsolatedNextServer(server);
    await stopEphemeralRedis(redis);
  });

  test("pre-existing App Router value key is not overwritten", async () => {
    const client = createClient({ url: redis.url });
    await client.connect();
    try {
      const value = await client.get(SEEDED_APP_ROUTER_CACHE_PATH);
      expect(value).toBe(APP_ROUTER_SENTINEL);
    } finally {
      await client.quit();
    }
  });

  test("pre-existing Pages Router value key is not overwritten", async () => {
    const client = createClient({ url: redis.url });
    await client.connect();
    try {
      const value = await client.get(SEEDED_PAGES_ROUTER_CACHE_PATH);
      expect(value).toBe(PAGES_ROUTER_SENTINEL);
    } finally {
      await client.quit();
    }
  });

  test("shared tags hash is still written (unconditional, regardless of setOnlyIfNotExists)", async () => {
    const client = createClient({ url: redis.url });
    await client.connect();
    try {
      const sharedTagsLen = await client.hLen("nextjs:__sharedTags__");
      expect(sharedTagsLen).toBeGreaterThan(0);
    } finally {
      await client.quit();
    }
  });

  test("a route that was not pre-seeded is still written fresh", async () => {
    const client = createClient({ url: redis.url });
    await client.connect();
    try {
      const value = await client.get("nextjs:/examples/fetch-tags");
      expect(value).not.toBeNull();
    } finally {
      await client.quit();
    }
  });
});
