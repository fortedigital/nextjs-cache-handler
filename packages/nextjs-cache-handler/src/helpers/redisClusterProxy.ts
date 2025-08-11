import type { RedisClusterType } from "@redis/client";
import { withAbortSignal } from "./withAbortSignal";

export type RedisClusterWithAbortSignal<T extends RedisClusterType> = T & {
  withAbortSignal(signal: AbortSignal): T;
  isReady: boolean;
};

export function withProxy<T extends RedisClusterType>(
  cluster: T,
  defaultSignal?: AbortSignal,
): RedisClusterWithAbortSignal<T> {
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

  return new Proxy(cluster, handler) as RedisClusterWithAbortSignal<T>;
}
