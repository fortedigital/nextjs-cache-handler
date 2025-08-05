import type { RedisClusterType } from "@redis/client";
import { withAbortSignal } from "./withAbortSignal";

export type WithAbortSignalCluster<T extends RedisClusterType> = T & {
  withAbortSignal(signal: AbortSignal): T;
  isReady: boolean;
};

export function withAbortSignalWrapper<T extends RedisClusterType>(
  client: T,
  defaultSignal?: AbortSignal,
): WithAbortSignalCluster<T> {
  let signal: AbortSignal | undefined = defaultSignal;

  const handler: ProxyHandler<T> = {
    get(target, prop, receiver) {
      if (prop === "withAbortSignal") {
        return (s: AbortSignal) => {
          signal = s;
          return new Proxy(client, handler);
        };
      }

      if (prop === "isReady") {
        return client.isOpen;
      }

      const orig = Reflect.get(target, prop, receiver);

      if (typeof orig !== "function") return orig;

      return (...args: unknown[]) =>
        withAbortSignal(() => (orig as Function).apply(target, args), signal);
    },
  };

  return new Proxy(client, handler) as WithAbortSignalCluster<T>;
}
