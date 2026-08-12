import { test, expect } from "@playwright/test";
import { createClient } from "redis";
import {
  startEphemeralRedis,
  stopEphemeralRedis,
  waitForRedisReady,
  waitForRedisPopulated,
  waitForRedisKeyDeleted,
  waitForHttpReady,
  startIsolatedNextServer,
  stopIsolatedNextServer,
  type IsolatedRedis,
  type IsolatedNextServer,
} from "./helpers/isolated-server";

// Bridges the exhaustive, direct-handler-level revalidateTag coverage in
// redis-handler-revalidate-tag.spec.ts to the FULL real pipeline: Next.js's
// own revalidateTag() -> CacheHandler.revalidateTag() -> composite handler
// -> redis-strings handler. The shared e2e suite (examples/redis-minimal/e2e)
// already proves this indirectly via an HTTP response timestamp change; this
// spec asserts the real Redis deletion directly instead of inferring it.
test.describe("revalidateTag via the real /api/revalidate route deletes real Redis state", () => {
  let redis: IsolatedRedis;
  let server: IsolatedNextServer;

  test.beforeAll(async () => {
    redis = await startEphemeralRedis();
    await waitForRedisReady(redis.url);

    server = await startIsolatedNextServer({
      env: { REDIS_URL: redis.url, REDIS_TYPE: "redis" },
    });

    await waitForRedisPopulated(redis.url, ["/examples/fetch-tags"], {
      timeoutMs: 60_000,
    });
    await waitForHttpReady(server.baseURL);
  });

  test.afterAll(async () => {
    await stopIsolatedNextServer(server);
    await stopEphemeralRedis(redis);
  });

  test("POST /api/revalidate with a tag deletes the matching value key and tag hash entries", async () => {
    const client = createClient({ url: redis.url });
    await client.connect();
    try {
      // Precondition: confirm the entry is actually present before revalidating,
      // so the deletion we assert afterward is unambiguously caused by this call.
      expect(await client.get("nextjs:/examples/fetch-tags")).not.toBeNull();
      expect(
        await client.hExists("nextjs:__sharedTags__", "/examples/fetch-tags"),
      ).toBe(1);

      const response = await fetch(`${server.baseURL}/api/revalidate`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tag: "futurama", cacheLife: "max" }),
      });
      expect(response.ok).toBe(true);

      // Next's revalidateTag()/revalidatePath() don't guarantee the underlying
      // CacheHandler purge completes synchronously with the HTTP response -
      // poll rather than asserting immediately (confirmed empirically flaky
      // otherwise). See waitForRedisKeyDeleted's doc comment.
      await waitForRedisKeyDeleted(redis.url, "nextjs:/examples/fetch-tags");

      expect(await client.get("nextjs:/examples/fetch-tags")).toBeNull();
      expect(
        await client.hExists("nextjs:__sharedTags__", "/examples/fetch-tags"),
      ).toBe(0);
      expect(
        await client.hExists(
          "nextjs:__sharedTagsTtl__",
          "/examples/fetch-tags",
        ),
      ).toBe(0);
    } finally {
      await client.quit();
    }
  });
});
