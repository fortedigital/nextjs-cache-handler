import { Handler } from "./cache-handler.types";
import { CreateRedisClusterStringsHandlerOptions } from "./redis-cluster-strings.types";
import createRedisStringsHandler from "./redis-strings";
import { withProxy } from "../helpers/redisClusterProxy";

/**
 * Creates a Handler for handling cache operations using Redis Cluster strings.
 *
 * This function initializes a Handler for managing cache operations using Redis Cluster.
 * It supports Redis Cluster and includes methods for on-demand revalidation of cache values.
 *
 * @param options - The configuration options for the Redis Handler. See {@link CreateRedisClusterStringsHandlerOptions}.
 *
 * @returns An object representing the Redis-based cache handler, with methods for cache operations.
 *
 * @remarks
 * - The `get` method retrieves a value from the cache, automatically converting `Buffer` types when necessary.
 * - The `set` method stores a value in the cache, using the configured expiration strategy.
 * - The `revalidateTag` and `delete` methods handle cache revalidation and deletion.
 */
export default function createHandler({
  client,
  ...options
}: CreateRedisClusterStringsHandlerOptions): Handler {
  return createRedisStringsHandler({
    client: withProxy(client),
    ...options,
  });
}
