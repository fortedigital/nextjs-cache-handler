import { test, expect } from "@playwright/test";
import { createClient, type RedisClientType } from "redis";
import createRedisHandler from "@fortedigital/nextjs-cache-handler/redis-strings";
import {
  startEphemeralRedis,
  stopEphemeralRedis,
  waitForRedisReady,
  type IsolatedRedis,
} from "./helpers/isolated-server";

// Direct handler construction against a real, ephemeral Redis (see
// redis-handler-tags-and-ttl.spec.ts for rationale). One shared Redis for
// the whole file; each test uses its own keyPrefix so tests never interfere
// with each other despite sharing the container.

let redis: IsolatedRedis;
let client: RedisClientType;

test.beforeAll(async () => {
  redis = await startEphemeralRedis();
  await waitForRedisReady(redis.url);
  client = createClient({ url: redis.url }) as RedisClientType;
  await client.connect();
});

test.afterAll(async () => {
  await client.quit();
  await stopEphemeralRedis(redis);
});

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

function makeEntry(tags: string[], expireAt = Math.floor(Date.now() / 1000) + 10_000) {
  return {
    lastModified: Date.now(),
    lifespan: makeLifespan(expireAt),
    tags,
    value: { kind: "FETCH" },
  } as any;
}

function handlerFor(keyPrefix: string) {
  return createRedisHandler({ client: client as any, keyPrefix });
}

test("revalidateTag deletes only entries matching the tag, leaves unrelated entries fully intact", async () => {
  const keyPrefix = "revtag-selective:";
  const handler = handlerFor(keyPrefix);

  await handler.set("a", makeEntry(["shared-tag", "only-a"]), {} as any);
  await handler.set("b", makeEntry(["shared-tag", "only-b"]), {} as any);
  await handler.set("c", makeEntry(["unrelated"]), {} as any);

  const cValueBefore = await client.get(`${keyPrefix}c`);
  const cTagsBefore = await client.hGet(`${keyPrefix}__sharedTags__`, "c");
  const cTtlBefore = await client.hGet(`${keyPrefix}__sharedTagsTtl__`, "c");

  await handler.revalidateTag("shared-tag");

  expect(await client.get(`${keyPrefix}a`)).toBeNull();
  expect(await client.hExists(`${keyPrefix}__sharedTags__`, "a")).toBe(0);
  expect(await client.hExists(`${keyPrefix}__sharedTagsTtl__`, "a")).toBe(0);

  expect(await client.get(`${keyPrefix}b`)).toBeNull();
  expect(await client.hExists(`${keyPrefix}__sharedTags__`, "b")).toBe(0);
  expect(await client.hExists(`${keyPrefix}__sharedTagsTtl__`, "b")).toBe(0);

  // "c" never carried the revalidated tag - must be byte-for-byte unchanged.
  expect(await client.get(`${keyPrefix}c`)).toBe(cValueBefore);
  expect(await client.hGet(`${keyPrefix}__sharedTags__`, "c")).toBe(cTagsBefore);
  expect(await client.hGet(`${keyPrefix}__sharedTagsTtl__`, "c")).toBe(cTtlBefore);
});

test("revalidateTag with no matching entries is a safe no-op", async () => {
  const keyPrefix = "revtag-nomatch:";
  const handler = handlerFor(keyPrefix);

  await handler.set("x", makeEntry(["foo"]), {} as any);
  await handler.set("y", makeEntry(["bar"]), {} as any);

  const before = {
    x: await client.get(`${keyPrefix}x`),
    y: await client.get(`${keyPrefix}y`),
    xTags: await client.hGet(`${keyPrefix}__sharedTags__`, "x"),
    yTags: await client.hGet(`${keyPrefix}__sharedTags__`, "y"),
  };

  await handler.revalidateTag("does-not-exist-anywhere");

  expect(await client.get(`${keyPrefix}x`)).toBe(before.x);
  expect(await client.get(`${keyPrefix}y`)).toBe(before.y);
  expect(await client.hGet(`${keyPrefix}__sharedTags__`, "x")).toBe(before.xTags);
  expect(await client.hGet(`${keyPrefix}__sharedTags__`, "y")).toBe(before.yTags);
});

test("an implicit tag matching an existing entry's tags is eagerly deleted, and bookkeeping is written", async () => {
  const keyPrefix = "revtag-implicit-eager:";
  const handler = handlerFor(keyPrefix);
  const implicitTag = "_N_T_/some/path";

  // App/Pages Router entries genuinely carry implicit path tags like this one
  // in their stored tags array (confirmed via the real-app-boot specs) -
  // revalidateTag's eager hScan+match+delete logic does not special-case
  // implicit tags, so a match here is deleted immediately, same as any tag.
  await handler.set("some/path", makeEntry([implicitTag]), {} as any);

  await handler.revalidateTag(implicitTag);

  expect(await client.get(`${keyPrefix}some/path`)).toBeNull();
  expect(await client.hExists(`${keyPrefix}__sharedTags__`, "some/path")).toBe(0);
  expect(await client.hExists(`${keyPrefix}__sharedTagsTtl__`, "some/path")).toBe(0);

  const bookkeeping = await client.hGet(`${keyPrefix}__revalidated_tags__`, implicitTag);
  expect(bookkeeping).not.toBeNull();
});

test("an implicit tag not present on any entry only writes bookkeeping (the genuinely lazy path) and deletes nothing", async () => {
  const keyPrefix = "revtag-implicit-lazy:";
  const handler = handlerFor(keyPrefix);
  const implicitTag = "_N_T_/never/cached";

  await handler.set("other/path", makeEntry(["unrelated-tag"]), {} as any);
  const before = await client.get(`${keyPrefix}other/path`);

  await handler.revalidateTag(implicitTag);

  // Nothing in the shared-tags hash contains this literal tag, so the
  // eager scan+match+delete finds nothing to remove.
  expect(await client.get(`${keyPrefix}other/path`)).toBe(before);
  expect(await client.hExists(`${keyPrefix}__sharedTags__`, "other/path")).toBe(1);

  // But the bookkeeping write happens unconditionally for implicit tags,
  // regardless of whether anything currently matches - this is the only
  // effect in this scenario, and it's what makes later get() calls able to
  // lazily invalidate a not-yet-cached entry once it does get cached.
  const bookkeeping = await client.hGet(`${keyPrefix}__revalidated_tags__`, implicitTag);
  expect(bookkeeping).not.toBeNull();
});

test("revalidateTag also sweeps and removes independently TTL-expired hash bookkeeping as a side effect", async () => {
  const keyPrefix = "revtag-ttl-sweep:";
  const handler = handlerFor(keyPrefix);

  const pastExpireAt = Math.floor(Date.now() / 1000) - 1000;
  await handler.set("expired", makeEntry(["irrelevant-tag"], pastExpireAt), {} as any);

  // revalidateSharedKeys() runs as part of every revalidateTag() call
  // (Promise.all([revalidateTag, revalidateSharedKeys])) - even a completely
  // unrelated tag revalidation should still sweep up this independently
  // expired entry's hash bookkeeping.
  await handler.revalidateTag("completely-unrelated-tag");

  expect(await client.hExists(`${keyPrefix}__sharedTagsTtl__`, "expired")).toBe(0);
  expect(await client.hExists(`${keyPrefix}__sharedTags__`, "expired")).toBe(0);
});
