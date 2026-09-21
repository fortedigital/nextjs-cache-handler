import type { RedisClientType } from "@redis/client";
import type { RedisClusterType } from "@redis/client";
import type { CacheHandlerValue } from "./handlers/cache-handler.types";
import createHandler from "./handlers/redis-strings";
import { withAdapter } from "./helpers/redisClusterAdapter";

const HSET_WITH_EXPIRATION_SCRIPT = `
local result = redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return result
`;

function useControlledTimeoutSignal(): AbortController {
  const controller = new AbortController();
  jest.spyOn(AbortSignal, "timeout").mockReturnValue(controller.signal);
  return controller;
}

describe("redis-strings abort signals", () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("uses an inherited native withAbortSignal implementation", async () => {
    const unlink = jest.fn().mockResolvedValue(1);
    const hDel = jest.fn().mockResolvedValue(1);
    const signalClient = { unlink, hDel };
    const withAbortSignal = jest.fn().mockReturnValue(signalClient);
    const client = Object.assign(Object.create({ withAbortSignal }), {
      isReady: true,
      unlink: jest.fn().mockResolvedValue(1),
      hDel: jest.fn().mockResolvedValue(1),
    }) as RedisClientType;

    await createHandler({ client }).delete!("cache-key");

    expect(withAbortSignal).toHaveBeenCalledTimes(3);
    expect(withAbortSignal).toHaveBeenCalledWith(expect.any(AbortSignal));
    expect(unlink).toHaveBeenCalledWith("cache-key");
    expect(hDel).toHaveBeenNthCalledWith(1, "__sharedTags__", "cache-key");
    expect(hDel).toHaveBeenNthCalledWith(2, "__sharedTagsTtl__", "cache-key");
    expect(client.unlink).not.toHaveBeenCalled();
    expect(client.hDel).not.toHaveBeenCalled();
  });

  it("passes the timeout signal to the native command", async () => {
    const controller = useControlledTimeoutSignal();
    let commandWasCancelled = false;
    const withAbortSignal = jest.fn((signal: AbortSignal) => ({
      unlink: jest.fn(
        () =>
          new Promise<number>((_, reject) => {
            signal.addEventListener(
              "abort",
              () => {
                commandWasCancelled = true;
                reject(new Error("Native command aborted"));
              },
              { once: true },
            );
          }),
      ),
    }));
    const client = {
      isReady: true,
      withAbortSignal,
      unlink: jest.fn(() => new Promise<number>(() => undefined)),
    } as unknown as RedisClientType;

    const operation = createHandler({ client, timeoutMs: 1 }).delete!(
      "cache-key",
    );
    controller.abort();

    await expect(operation).rejects.toThrow("Native command aborted");

    expect(withAbortSignal).toHaveBeenCalledWith(expect.any(AbortSignal));
    expect(commandWasCancelled).toBe(true);
    expect(client.unlink).not.toHaveBeenCalled();
  });

  it("routes serialized set payloads through the native abort client", async () => {
    const hSet = jest.fn().mockResolvedValue(1);
    const set = jest.fn().mockResolvedValue("OK");
    const withAbortSignal = jest.fn().mockReturnValue({ hSet, set });
    const client = {
      isReady: true,
      withAbortSignal,
      hSet: jest.fn().mockResolvedValue(1),
      set: jest.fn().mockResolvedValue("OK"),
    } as unknown as RedisClientType;
    const serializedValue = "serialized-page-payload";
    const valueSerializer = {
      serialize: jest.fn().mockReturnValue(serializedValue),
      deserialize: jest.fn(),
    };
    const cacheValue = {
      value: null,
      lastModified: Date.now(),
      tags: [],
      lifespan: null,
    } as unknown as CacheHandlerValue;

    await createHandler({ client, valueSerializer }).set(
      "cache-key",
      cacheValue,
    );

    expect(withAbortSignal).toHaveBeenCalledTimes(2);
    expect(hSet).toHaveBeenCalledWith("__sharedTags__", "cache-key", "[]");
    expect(set).toHaveBeenCalledWith("cache-key", serializedValue, undefined);
    expect(client.hSet).not.toHaveBeenCalled();
    expect(client.set).not.toHaveBeenCalled();
  });

  it.each([
    ["is missing", {}],
    ["is not callable", { withAbortSignal: undefined }],
  ])(
    "falls back to the abort proxy when withAbortSignal %s",
    async (_, abortSignalProperty) => {
      const controller = useControlledTimeoutSignal();
      let completeCommand!: () => void;
      const underlyingCommand = new Promise<number>((resolve) => {
        completeCommand = () => resolve(1);
      });
      const unlink = jest.fn(() => underlyingCommand);
      const client = {
        isReady: true,
        unlink,
        ...abortSignalProperty,
      } as unknown as RedisClientType;

      const operation = createHandler({ client, timeoutMs: 1 }).delete!(
        "cache-key",
      );
      controller.abort();

      await expect(operation).rejects.toThrow("Operation aborted");

      expect(unlink).toHaveBeenCalledWith("cache-key");

      completeCommand();
      await expect(underlyingCommand).resolves.toBe(1);
    },
  );
});

describe("redis-strings metadata key expiration", () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("atomically refreshes metadata key expiration when cache metadata is written", async () => {
    const evalCommand = jest.fn().mockResolvedValue(1);
    const expireAt = jest.fn().mockResolvedValue(1);
    const hSet = jest.fn().mockResolvedValue(1);
    const set = jest.fn().mockResolvedValue("OK");
    const withAbortSignal = jest
      .fn()
      .mockReturnValue({ eval: evalCommand, expireAt, hSet, set });
    const client = {
      isReady: true,
      withAbortSignal,
      eval: jest.fn().mockResolvedValue(1),
      hSet: jest.fn().mockResolvedValue(1),
      set: jest.fn().mockResolvedValue("OK"),
    } as unknown as RedisClientType;
    const cacheValue = {
      value: null,
      lastModified: Date.now(),
      tags: ["article"],
      lifespan: { expireAt: 2_000_000_000 },
    } as unknown as CacheHandlerValue;

    await createHandler({
      client,
      metadataKeysExpirationSeconds: 2_419_200,
    }).set("cache-key", cacheValue);

    expect(evalCommand).toHaveBeenCalledTimes(2);
    expect(evalCommand).toHaveBeenNthCalledWith(
      1,
      HSET_WITH_EXPIRATION_SCRIPT,
      {
        arguments: ["cache-key", '["article"]', "2419200"],
        keys: ["__sharedTags__"],
      },
    );
    expect(evalCommand).toHaveBeenNthCalledWith(
      2,
      HSET_WITH_EXPIRATION_SCRIPT,
      {
        arguments: ["cache-key", "2000000000", "2419200"],
        keys: ["__sharedTagsTtl__"],
      },
    );
    expect(hSet).not.toHaveBeenCalled();
    expect(client.eval).not.toHaveBeenCalled();
    expect(client.hSet).not.toHaveBeenCalled();
  });

  it("preserves the default implicit-tag write behavior when expiration is disabled", async () => {
    const hSet = jest.fn().mockResolvedValue(1);
    const hScan = jest.fn().mockResolvedValue({ cursor: "0", entries: [] });
    const withAbortSignal = jest.fn().mockReturnValue({ hSet, hScan });
    const client = {
      isReady: true,
      withAbortSignal,
      hSet: jest.fn().mockResolvedValue(1),
      hScan,
    } as unknown as RedisClientType;

    await createHandler({ client }).revalidateTag("_N_T_/articles");

    expect(hSet).toHaveBeenCalledTimes(2);
    expect(hSet).toHaveBeenNthCalledWith(
      1,
      "__revalidated_tags__",
      "_N_T_/articles",
      expect.any(Number),
    );
    expect(hSet).toHaveBeenNthCalledWith(
      2,
      "__revalidated_tags__",
      "_N_T_/articles",
      expect.any(Number),
    );
  });

  it("routes the atomic metadata write through the Redis cluster adapter", async () => {
    const evalCommand = jest.fn().mockResolvedValue(1);
    const set = jest.fn().mockResolvedValue("OK");
    const withAbortSignal = jest
      .fn()
      .mockReturnValue({ eval: evalCommand, set });
    const cluster = {
      replicas: [{ client: { isReady: true } }],
      withAbortSignal,
    } as unknown as RedisClusterType;
    const cacheValue = {
      value: null,
      lastModified: Date.now(),
      tags: ["article"],
      lifespan: null,
    } as unknown as CacheHandlerValue;

    await createHandler({
      client: withAdapter(cluster),
      metadataKeysExpirationSeconds: 60,
    }).set("cache-key", cacheValue);

    expect(evalCommand).toHaveBeenCalledWith(HSET_WITH_EXPIRATION_SCRIPT, {
      arguments: ["cache-key", '["article"]', "60"],
      keys: ["__sharedTags__"],
    });
  });

  it.each([0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY])(
    "rejects invalid metadata key expiration %p",
    (metadataKeysExpirationSeconds) => {
      const client = { isReady: true } as RedisClientType;

      expect(() =>
        createHandler({ client, metadataKeysExpirationSeconds }),
      ).toThrow("metadataKeysExpirationSeconds must be a positive integer");
    },
  );

  it("atomically refreshes revalidated-tag metadata expiration", async () => {
    const evalCommand = jest.fn().mockResolvedValue(1);
    const hScan = jest.fn().mockResolvedValue({ cursor: "0", entries: [] });
    const hSet = jest.fn().mockResolvedValue(1);
    const withAbortSignal = jest
      .fn()
      .mockReturnValue({ eval: evalCommand, hScan, hSet });
    const client = {
      isReady: true,
      withAbortSignal,
      hScan,
      hSet: jest.fn().mockResolvedValue(1),
    } as unknown as RedisClientType;

    await createHandler({
      client,
      metadataKeysExpirationSeconds: 2_419_200,
    }).revalidateTag("_N_T_/articles");

    expect(evalCommand).toHaveBeenCalledTimes(1);
    expect(evalCommand).toHaveBeenCalledWith(HSET_WITH_EXPIRATION_SCRIPT, {
      arguments: ["_N_T_/articles", expect.any(String), "2419200"],
      keys: ["__revalidated_tags__"],
    });
    expect(hSet).not.toHaveBeenCalled();
    expect(client.hSet).not.toHaveBeenCalled();
  });
});
