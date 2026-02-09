import { createClient } from "redis";
import Redis from "ioredis";
import { PHASE_PRODUCTION_BUILD } from "next/constants.js";
import { CacheHandler } from "@fortedigital/nextjs-cache-handler";
import createLruHandler from "@fortedigital/nextjs-cache-handler/local-lru";
import createRedisHandler from "@fortedigital/nextjs-cache-handler/redis-strings";
import createCompositeHandler from "@fortedigital/nextjs-cache-handler/composite";
import { ioredisAdapter } from "@fortedigital/nextjs-cache-handler/helpers/ioredisAdapter";
import { getClientInfoTag } from "@fortedigital/nextjs-cache-handler/helpers/getClientInfoTag";

const isSingleConnectionModeEnabled = !!process.env.REDIS_SINGLE_CONNECTION;
const redisType = process.env.REDIS_TYPE || "redis"; // "redis" or "ioredis"

async function setupRedisClient() {
  if (PHASE_PRODUCTION_BUILD !== process.env.NEXT_PHASE) {
    let redisClient;

    try {
      if (redisType === "ioredis") {
        console.info(`Using ioredis client...`);
        const ioredisClient = new Redis(process.env.REDIS_URL, {
          // Set clientInfoTag for Redis driver identification
          // This helps identify the framework in CLIENT LIST output
          clientInfoTag: getClientInfoTag(),
        });

        // Wait for connection to be ready
        console.info("Connecting ioredis client...");
        await new Promise((resolve, reject) => {
          ioredisClient.once("ready", () => {
            console.info("ioredis client connected.");
            resolve();
          });
          ioredisClient.once("error", reject);
        });

        redisClient = ioredisAdapter(ioredisClient);
      } else {
        console.info(`Using @redis/client...`);
        redisClient = createClient({
          url: process.env.REDIS_URL,
          pingInterval: 10000,
          // Set clientInfoTag for Redis driver identification
          // This helps identify the framework in CLIENT LIST output
          clientInfoTag: getClientInfoTag(),
        });

        console.info("Connecting Redis client...");
        await redisClient.connect();
        console.info("Redis client connected.");
      }

      redisClient.on("error", (e) => {
        if (process.env.NEXT_PRIVATE_DEBUG_CACHE !== undefined) {
          console.warn("Redis error", e);
        }
        if (isSingleConnectionModeEnabled) {
          global.cacheHandlerConfig = null;
          global.cacheHandlerConfigPromise = null;
        }
      });

      if (!redisClient.isReady) {
        console.error("Failed to initialize caching layer.");
      }

      return redisClient;
    } catch (error) {
      console.warn("Failed to connect Redis client:", error);
      if (redisClient) {
        try {
          redisClient.destroy();
        } catch (e) {
          console.error(
            "Failed to quit the Redis client after failing to connect.",
            e
          );
        }
      }
    }
  }

  return null;
}

async function createCacheConfig() {
  const redisClient = await setupRedisClient();
  const lruCache = createLruHandler();

  if (!redisClient) {
    const config = { handlers: [lruCache] };
    if (isSingleConnectionModeEnabled) {
      global.cacheHandlerConfigPromise = null;
      global.cacheHandlerConfig = config;
    }
    return config;
  }

  const redisCacheHandler = createRedisHandler({
    client: redisClient,
    keyPrefix: "nextjs:",
  });

  const config = {
    handlers: [
      createCompositeHandler({
        handlers: [lruCache, redisCacheHandler],
        setStrategy: (ctx) => (ctx?.tags.includes("memory-cache") ? 0 : 1),
      }),
    ],
  };

  if (isSingleConnectionModeEnabled) {
    global.cacheHandlerConfigPromise = null;
    global.cacheHandlerConfig = config;
  }

  return config;
}

CacheHandler.onCreation(() => {
  if (isSingleConnectionModeEnabled) {
    if (global.cacheHandlerConfig) {
      return global.cacheHandlerConfig;
    }
    if (global.cacheHandlerConfigPromise) {
      return global.cacheHandlerConfigPromise;
    }
  }

  const promise = createCacheConfig();
  if (isSingleConnectionModeEnabled) {
    global.cacheHandlerConfigPromise = promise;
  }
  return promise;
});

export default CacheHandler;
