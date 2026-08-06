import type { RedisClientType } from "@redis/client";
import type { CacheHandlerValue } from "./handlers/cache-handler.types";
import createHandler from "./handlers/redis-strings";

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
