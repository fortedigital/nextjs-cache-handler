import type { RedisClusterType } from "@redis/client";

export type RedisClusterCacheAdapter = RedisClusterType & {
  isReady: boolean;
};

export function withAdapter<T extends RedisClusterType>(
  cluster: RedisClusterType,
): RedisClusterCacheAdapter {
  const handler: ProxyHandler<T> = {
    get(target, prop, receiver) {
      if (prop === "isReady") {
        return cluster.replicas.every((s) => s.client?.isReady);
      }

      return Reflect.get(target, prop, receiver);
    },
  };

  return new Proxy(cluster, handler) as RedisClusterCacheAdapter;
}
