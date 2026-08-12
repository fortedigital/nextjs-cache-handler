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

// Routes whose presence in Redis can only be explained by `registerInitialCache`
// having read build-time artifacts off disk - no HTTP request is made anywhere
// in this file, so these keys cannot have been written by runtime cache writes.
const EXPECTED_CACHE_PATHS = [
  "/examples/default-cache",
  "/examples/fetch-tags",
  "/posts/1",
];

test.describe("registerInitialCache populates Redis on boot (setOnlyIfNotExists=false, default)", () => {
  let redis: IsolatedRedis;
  let server: IsolatedNextServer;

  test.beforeAll(async () => {
    redis = await startEphemeralRedis();
    await waitForRedisReady(redis.url);

    // Precondition: Redis must be genuinely empty before `next start` boots,
    // so the population assertions below can only be explained by
    // registerInitialCache - not by stale data left over in the container.
    const preCheckClient = createClient({ url: redis.url });
    await preCheckClient.connect();
    try {
      const dbSize = await preCheckClient.dbSize();
      expect(dbSize).toBe(0);
    } finally {
      await preCheckClient.quit();
    }

    server = await startIsolatedNextServer({
      env: { REDIS_URL: redis.url, REDIS_TYPE: "redis" },
    });

    await waitForRedisPopulated(redis.url, EXPECTED_CACHE_PATHS, {
      timeoutMs: 60_000,
    });
  });

  test.afterAll(async () => {
    await stopIsolatedNextServer(server);
    await stopEphemeralRedis(redis);
  });

  test("shared tags hash is populated before any HTTP request is made", async () => {
    const client = createClient({ url: redis.url });
    await client.connect();
    try {
      const sharedTagsLen = await client.hLen("nextjs:__sharedTags__");
      expect(sharedTagsLen).toBeGreaterThan(0);
    } finally {
      await client.quit();
    }
  });

  test("a static App Router page is populated from its pre-rendered artifacts", async () => {
    const client = createClient({ url: redis.url });
    await client.connect();
    try {
      const value = await client.get("nextjs:/examples/default-cache");
      expect(value).not.toBeNull();
    } finally {
      await client.quit();
    }
  });

  test("a fetch-cache entry's tags are populated from disk (proves fetch-cache population specifically)", async () => {
    const client = createClient({ url: redis.url });
    await client.connect();
    try {
      const pageValue = await client.get("nextjs:/examples/fetch-tags");
      expect(pageValue).not.toBeNull();

      const rawTags = await client.hGet(
        "nextjs:__sharedTags__",
        "/examples/fetch-tags",
      );
      expect(rawTags).not.toBeNull();
      const tags = JSON.parse(rawTags ?? "[]");
      expect(tags).toContain("futurama");
    } finally {
      await client.quit();
    }
  });

  test("a Pages Router (fallback: false) page gets its implicit path tag from disk", async () => {
    const client = createClient({ url: redis.url });
    await client.connect();
    try {
      const value = await client.get("nextjs:/posts/1");
      expect(value).not.toBeNull();

      const rawTags = await client.hGet("nextjs:__sharedTags__", "/posts/1");
      expect(rawTags).not.toBeNull();
      const tags = JSON.parse(rawTags ?? "[]");
      expect(tags).toContain("_N_T_/posts/1");
    } finally {
      await client.quit();
    }
  });

  // Bridges confidence between this real-app-boot proof and the exhaustive,
  // deterministic TTL matrix in redis-handler-tags-and-ttl.spec.ts: the real
  // CacheHandler -> registerInitialCache -> redis-strings pipeline must
  // produce internally-consistent state, not just "a TTL exists somewhere."
  for (const path of ["/examples/fetch-tags", "/posts/1"]) {
    test(`real Redis EXPIRETIME agrees with sharedTagsTtlKey for ${path}`, async () => {
      const client = createClient({ url: redis.url });
      await client.connect();
      try {
        const rawTtl = await client.hGet("nextjs:__sharedTagsTtl__", path);
        expect(rawTtl).not.toBeNull();
        const expectedExpireAt = Number(rawTtl);

        const expireTime = await client.expireTime(`nextjs:${path}`);
        expect(expireTime).toBe(expectedExpireAt);
        expect(expireTime).toBeGreaterThan(Math.floor(Date.now() / 1000));
      } finally {
        await client.quit();
      }
    });
  }
});
