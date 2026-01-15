import type { RedisClientType } from "@redis/client";
import type { Redis } from "ioredis";

/**
 * Adapter to make an ioredis client compatible with the interface expected by createRedisHandler.
 *
 * @param client - The ioredis client instance.
 * @returns A proxy that implements the subset of RedisClientType used by this library.
 */
export function ioredisAdapter(client: Redis): RedisClientType {
  return new Proxy(client, {
    get(target, prop, receiver) {
      if (prop === "isReady") {
        return target.status === "ready";
      }

      if (prop === "hScan") {
        return async (
          key: string,
          cursor: string,
          options?: { COUNT?: number },
        ) => {
          let result: [string, string[]];

          if (options?.COUNT) {
            result = await target.hscan(key, cursor, "COUNT", options.COUNT);
          } else {
            result = await target.hscan(key, cursor);
          }

          const [newCursor, items] = result;

          const entries = [];
          for (let i = 0; i < items.length; i += 2) {
            entries.push({ field: items[i], value: items[i + 1] });
          }

          return {
            cursor: newCursor,
            entries,
          };
        };
      }

      if (prop === "hDel") {
        return async (key: string, fields: string | string[]) => {
          const args = Array.isArray(fields) ? fields : [fields];
          if (args.length === 0) {
            return 0;
          }
          return target.hdel(key, ...args);
        };
      }

      if (prop === "unlink") {
        return async (keys: string | string[]) => {
          const args = Array.isArray(keys) ? keys : [keys];
          if (args.length === 0) {
            return 0;
          }
          return target.unlink(...args);
        };
      }

      if (prop === "set") {
        return async (key: string, value: string, options?: any) => {
          if (!options) {
            return target.set(key, value);
          }

          const extraArgs: (string | number)[] = [];

          // Expiration options (mutually exclusive)
          if (options.EXAT) {
            extraArgs.push("EXAT", options.EXAT);
          } else if (options.PXAT) {
            extraArgs.push("PXAT", options.PXAT);
          } else if (options.EX) {
            extraArgs.push("EX", options.EX);
          } else if (options.PX) {
            extraArgs.push("PX", options.PX);
          } else if (options.KEEPTTL) {
            extraArgs.push("KEEPTTL");
          }

          // Condition options (mutually exclusive with each other, but can be combined with expiration)
          if (options.NX) {
            extraArgs.push("NX");
          } else if (options.XX) {
            extraArgs.push("XX");
          }

          // GET option (can be combined with others)
          if (options.GET) {
            extraArgs.push("GET");
          }

          // Cast to a generic signature to avoid overload mismatch issues with dynamic args
          const setFn = target.set as unknown as (
            key: string,
            value: string | number,
            ...args: (string | number)[]
          ) => Promise<string | null>;

          return setFn(key, value, ...extraArgs);
        };
      }

      if (prop === "hmGet") {
        return async (key: string, fields: string | string[]) => {
          const args = Array.isArray(fields) ? fields : [fields];
          if (args.length === 0) {
            return [];
          }
          return target.hmget(key, ...args);
        };
      }

      if (prop === "get") {
        return target.get.bind(target);
      }

      if (prop === "expireAt") {
        return target.expireat.bind(target);
      }

      if (prop === "hSet") {
        return target.hset.bind(target);
      }

      if (prop === "hExists") {
        return target.hexists.bind(target);
      }

      if (prop === "on") {
        return target.on.bind(target);
      }

      if (prop === "destroy") {
        return async () => {
          await target.quit();
        };
      }

      return Reflect.get(target, prop, receiver);
    },
  }) as unknown as RedisClientType;
}
