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

const SEEDED_CACHE_PATH = "nextjs:/examples/default-cache";
const SENTINEL = JSON.stringify({
  lastModified: 1,
  lifespan: null,
  tags: [],
  value: { kind: "SENTINEL", __test: true },
});

test.describe("registerInitialCache with setOnlyIfNotExists=true does not clobber existing keys", () => {
  let redis: IsolatedRedis;
  let server: IsolatedNextServer;

  test.beforeAll(async () => {
    redis = await startEphemeralRedis();
    await waitForRedisReady(redis.url);

    // Pre-seed BEFORE starting next start, so registerInitialCache sees an existing key.
    const seedClient = createClient({ url: redis.url });
    await seedClient.connect();
    await seedClient.set(SEEDED_CACHE_PATH, SENTINEL);
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
      ["/examples/default-cache", "/examples/fetch-tags"],
      { timeoutMs: 60_000 },
    );
  });

  test.afterAll(async () => {
    await stopIsolatedNextServer(server);
    await stopEphemeralRedis(redis);
  });

  test("pre-existing value key is not overwritten", async () => {
    const client = createClient({ url: redis.url });
    await client.connect();
    try {
      const value = await client.get(SEEDED_CACHE_PATH);
      expect(value).toBe(SENTINEL);
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
