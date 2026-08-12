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

// Control/counterfactual for initial-cache-set-only-if-not-exists.spec.ts:
// that spec proves a pre-seeded key SURVIVES when setOnlyIfNotExists=true.
// On its own, that's consistent with two different explanations - either
// (a) the NX guard specifically causes it to survive, or (b)
// registerInitialCache simply never touches a key that already exists,
// regardless of the flag. This spec seeds the exact same keys and runs with
// the flag left unset (default false), asserting they DO get overwritten -
// ruling out (b) and isolating the flag as the actual cause.
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

test.describe("registerInitialCache with setOnlyIfNotExists left at its default (false) overwrites existing keys", () => {
  let redis: IsolatedRedis;
  let server: IsolatedNextServer;

  test.beforeAll(async () => {
    redis = await startEphemeralRedis();
    await waitForRedisReady(redis.url);

    // Pre-seed the SAME keys and sentinels as initial-cache-set-only-if-not-exists.spec.ts,
    // but this time boot next start WITHOUT INITIAL_CACHE_SET_ONLY_IF_NOT_EXISTS set.
    const seedClient = createClient({ url: redis.url });
    await seedClient.connect();
    await seedClient.set(SEEDED_APP_ROUTER_CACHE_PATH, APP_ROUTER_SENTINEL);
    await seedClient.set(SEEDED_PAGES_ROUTER_CACHE_PATH, PAGES_ROUTER_SENTINEL);
    await seedClient.quit();

    server = await startIsolatedNextServer({
      env: { REDIS_URL: redis.url, REDIS_TYPE: "redis" },
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

  test("pre-existing App Router value key IS overwritten", async () => {
    const client = createClient({ url: redis.url });
    await client.connect();
    try {
      const value = await client.get(SEEDED_APP_ROUTER_CACHE_PATH);
      expect(value).not.toBeNull();
      expect(value).not.toBe(APP_ROUTER_SENTINEL);
    } finally {
      await client.quit();
    }
  });

  test("pre-existing Pages Router value key IS overwritten", async () => {
    const client = createClient({ url: redis.url });
    await client.connect();
    try {
      const value = await client.get(SEEDED_PAGES_ROUTER_CACHE_PATH);
      expect(value).not.toBeNull();
      expect(value).not.toBe(PAGES_ROUTER_SENTINEL);
    } finally {
      await client.quit();
    }
  });
});
