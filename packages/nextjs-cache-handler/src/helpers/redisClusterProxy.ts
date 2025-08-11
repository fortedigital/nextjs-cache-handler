import type { RedisClusterType } from "@redis/client";

export type RedisClusterCacheProxy = RedisClusterType & {
  isReady: boolean;
};

export function withProxy<T extends RedisClusterType>(
  cluster: RedisClusterType,
): RedisClusterCacheProxy {
  const handler: ProxyHandler<T> = {
    get(target, prop, receiver) {
      if (prop === "isReady") {
        return cluster.replicas.every((s) => s.client?.isReady);
      }

      return Reflect.get(target, prop, receiver);
    },
  };

  return new Proxy(cluster, handler) as RedisClusterCacheProxy;
}
