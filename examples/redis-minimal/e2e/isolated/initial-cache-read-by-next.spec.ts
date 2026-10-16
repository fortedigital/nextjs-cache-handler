import { test, expect } from "@playwright/test";
import {
  startEphemeralRedis,
  stopEphemeralRedis,
  waitForRedisReady,
  waitForRedisPopulated,
  waitForHttpReady,
  startIsolatedNextServer,
  stopIsolatedNextServer,
  type IsolatedRedis,
  type IsolatedNextServer,
} from "./helpers/isolated-server";
import { cacheKeyFor } from "./helpers/cache-keys";

// The other registerInitialCache specs prove entries are *written*. This one
// proves Next.js *reads* them: the first request after boot must be served
// from the seeded entry. On Next.js 16.3.8+ response-cache keys are
// route-scoped, so seeding under the bare pathname leaves Redis full of
// entries Next.js never looks up - every request misses while the population
// assertions still pass.
const SEEDED_PATHS = ["/", "/examples/default-cache", "/posts/1"];

test.describe("Next.js serves registerInitialCache entries from Redis", () => {
  let redis: IsolatedRedis;
  let server: IsolatedNextServer;

  test.beforeAll(async () => {
    redis = await startEphemeralRedis();
    await waitForRedisReady(redis.url);

    server = await startIsolatedNextServer({
      env: { REDIS_URL: redis.url, REDIS_TYPE: "redis" },
    });

    await waitForRedisPopulated(redis.url, SEEDED_PATHS.map(cacheKeyFor), {
      timeoutMs: 60_000,
    });
    await waitForHttpReady(server.baseURL);
  });

  test.afterAll(async () => {
    await stopIsolatedNextServer(server);
    await stopEphemeralRedis(redis);
  });

  for (const pathname of SEEDED_PATHS) {
    test(`first request to ${pathname} is a cache HIT`, async ({ request }) => {
      const response = await request.get(`${server.baseURL}${pathname}`);

      expect(response.status()).toBe(200);
      expect(response.headers()["x-nextjs-cache"]).toBe("HIT");
    });
  }
});
