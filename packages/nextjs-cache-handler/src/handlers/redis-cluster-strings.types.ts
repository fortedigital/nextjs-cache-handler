import type { RedisClusterType } from "@redis/client";
import { CreateRedisStringsHandlerOptions } from "./redis-strings.types";

export type CreateRedisClusterStringsHandlerOptions =
  CreateRedisStringsHandlerOptions<RedisClusterType>;
