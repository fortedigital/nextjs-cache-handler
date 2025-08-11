import type { RedisClusterType } from "@redis/client";
import { withAbortSignal } from "./withAbortSignal";

export type RedisClusterCacheProxy = RedisClusterType & {
  withAbortSignal(signal: AbortSignal): RedisClusterType;
  isReady: boolean;
};

export function withProxy<T extends RedisClusterType>(
  cluster: RedisClusterType,
  defaultSignal?: AbortSignal,
): RedisClusterCacheProxy {
  let signal: AbortSignal | undefined = defaultSignal;

  const handler: ProxyHandler<T> = {
    get(target, prop, receiver) {
      if (prop === "withAbortSignal") {
        return (s: AbortSignal) => {
          signal = s;
          return new Proxy(cluster, handler);
        };
      }

      if (prop === "isReady") {
        return cluster.replicas.every((s) => s.client?.isReady);
      }

      const orig = Reflect.get(target, prop, receiver);

      if (typeof orig !== "function") return orig;

      return (...args: unknown[]) =>
        withAbortSignal(() => (orig as Function).apply(target, args), signal);
    },
  };

  return new Proxy(cluster, handler) as RedisClusterCacheProxy;
}
