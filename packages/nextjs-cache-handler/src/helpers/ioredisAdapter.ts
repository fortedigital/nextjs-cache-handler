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
          const args: (string | number)[] = [key, value];
          if (options) {
            // Expiration options (mutually exclusive)
            if (options.EXAT) {
              args.push("EXAT", options.EXAT);
            } else if (options.PXAT) {
              args.push("PXAT", options.PXAT);
            } else if (options.EX) {
              args.push("EX", options.EX);
            } else if (options.PX) {
              args.push("PX", options.PX);
            } else if (options.KEEPTTL) {
              args.push("KEEPTTL");
            }

            // Condition options (mutually exclusive with each other, but can be combined with expiration)
            if (options.NX) {
              args.push("NX");
            } else if (options.XX) {
              args.push("XX");
            }

            // GET option (can be combined with others)
            if (options.GET) {
              args.push("GET");
            }
          }
          // Cast to a generic signature to avoid overload mismatch issues with dynamic args
          const setFn = target.set as unknown as (
            key: string,
            value: string | number,
            ...args: (string | number)[]
          ) => Promise<string | null>;

          return setFn(
            args[0] as string,
            args[1] as string | number,
            ...args.slice(2),
          );
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

      return Reflect.get(target, prop, receiver);
    },
  }) as unknown as RedisClientType;
}
