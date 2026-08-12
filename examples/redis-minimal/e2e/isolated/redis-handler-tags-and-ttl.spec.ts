import { test, expect } from "@playwright/test";
import { createClient, type RedisClientType } from "redis";
import createRedisHandler from "@fortedigital/nextjs-cache-handler/redis-strings";
import {
  startEphemeralRedis,
  stopEphemeralRedis,
  waitForRedisReady,
  type IsolatedRedis,
} from "./helpers/isolated-server";

// This spec constructs the redis-strings handler directly (the same public
// subpath export `examples/redis-minimal/cache-handler.mjs` uses in
// production) against a real, ephemeral Redis - no `next start` boot at all.
// That gives full deterministic control over arbitrary tags/lifespans, which
// is essential for a matrix of TTL/tag scenarios the fixed example-app
// routes can't cleanly provide, and it's fast (no Next.js process).
const KEY_PREFIX = "nextjs:";

function makeLifespan(expireAt: number) {
  return {
    expireAge: 0,
    expireAt,
    lastModifiedAt: Math.floor(Date.now() / 1000),
    revalidate: 0,
    staleAge: 0,
    staleAt: 0,
  };
}

test.describe("redis-strings handler: sharedTagsKey and sharedTagsTtlKey rules (direct construction, no next start)", () => {
  let redis: IsolatedRedis;
  let client: RedisClientType;
  let handler: ReturnType<typeof createRedisHandler>;

  test.beforeAll(async () => {
    redis = await startEphemeralRedis();
    await waitForRedisReady(redis.url);
    client = createClient({ url: redis.url }) as RedisClientType;
    await client.connect();
    // `redis` (this package's own dependency) and the internal `@redis/client`
    // version pinned inside @fortedigital/nextjs-cache-handler are structurally
    // identical at runtime but nominally distinct types (private class fields) -
    // cast at the boundary, same as this repo's own cache-handler.mjs does implicitly
    // via plain JS.
    handler = createRedisHandler({ client: client as any, keyPrefix: KEY_PREFIX });
  });

  test.afterAll(async () => {
    await client.quit();
    await stopEphemeralRedis(redis);
  });

  test("sharedTagsKey stores an empty tags array verbatim", async () => {
    await handler.set(
      "tags-empty",
      {
        lastModified: Date.now(),
        lifespan: makeLifespan(Math.floor(Date.now() / 1000) + 1000),
        tags: [],
        value: { kind: "FETCH" },
      } as any,
      {} as any,
    );

    const raw = await client.hGet("nextjs:__sharedTags__", "tags-empty");
    expect(raw).toBe("[]");
  });

  test("sharedTagsKey stores a multi-tag array verbatim, including duplicates (no dedup at this layer)", async () => {
    await handler.set(
      "tags-multi",
      {
        lastModified: Date.now(),
        lifespan: makeLifespan(Math.floor(Date.now() / 1000) + 1000),
        tags: ["a", "b", "a"],
        value: { kind: "FETCH" },
      } as any,
      {} as any,
    );

    const raw = await client.hGet("nextjs:__sharedTags__", "tags-multi");
    expect(raw).toBe(JSON.stringify(["a", "b", "a"]));
  });

  const ttlCases = [
    { label: "30s revalidate", offsetSeconds: 30 },
    { label: "3600s (1h) revalidate", offsetSeconds: 3600 },
    { label: "86400s (1d) revalidate", offsetSeconds: 86400 },
  ];

  for (const { label, offsetSeconds } of ttlCases) {
    test(`sharedTagsTtlKey and real Redis EXPIRETIME agree exactly for ${label}`, async () => {
      const key = `ttl-${offsetSeconds}`;
      const expireAt = Math.floor(Date.now() / 1000) + offsetSeconds;

      await handler.set(
        key,
        {
          lastModified: Date.now(),
          lifespan: makeLifespan(expireAt),
          tags: [],
          value: { kind: "FETCH" },
        } as any,
        {} as any,
      );

      const rawTtl = await client.hGet("nextjs:__sharedTagsTtl__", key);
      expect(rawTtl).toBe(String(expireAt));

      const expireTime = await client.expireTime(`nextjs:${key}`);
      expect(expireTime).toBe(expireAt);
    });
  }

  test("lifespan: null produces a permanent key with no TTL and no sharedTagsTtlKey entry", async () => {
    const key = "permanent-key";

    await handler.set(
      key,
      {
        lastModified: Date.now(),
        lifespan: null,
        tags: [],
        value: { kind: "FETCH" },
      } as any,
      {} as any,
    );

    const ttlEntryExists = await client.hExists("nextjs:__sharedTagsTtl__", key);
    expect(ttlEntryExists).toBe(0);

    const ttl = await client.ttl(`nextjs:${key}`);
    expect(ttl).toBe(-1);

    // "No TTL" must not mean "not written" - the value itself is still cached.
    const value = await client.get(`nextjs:${key}`);
    expect(value).not.toBeNull();
  });

  test("keyExpirationStrategy=EXAT produces the same real Redis expiry as the default EXPIREAT", async () => {
    const exatHandler = createRedisHandler({
      client: client as any,
      keyPrefix: "nextjs-exat:",
      keyExpirationStrategy: "EXAT",
    });
    const key = "exat-check";
    const expireAt = Math.floor(Date.now() / 1000) + 500;

    await exatHandler.set(
      key,
      {
        lastModified: Date.now(),
        lifespan: makeLifespan(expireAt),
        tags: [],
        value: { kind: "FETCH" },
      } as any,
      {} as any,
    );

    const expireTime = await client.expireTime(`nextjs-exat:${key}`);
    expect(expireTime).toBe(expireAt);
  });
});
