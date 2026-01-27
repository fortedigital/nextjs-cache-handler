import { createClient } from "redis";
import Redis from "ioredis";
import { PHASE_PRODUCTION_BUILD } from "next/constants.js";
import { ioredisAdapter } from "@fortedigital/nextjs-cache-handler/helpers/ioredisAdapter";


const isSingleConnectionModeEnabled = !!process.env.REDIS_SINGLE_CONNECTION;
const redisType = process.env.REDIS_TYPE || "redis";

let redisClient = null;

async function setupRedisClient() {
  if (PHASE_PRODUCTION_BUILD !== process.env.NEXT_PHASE) {
    try {
      if (redisType === "ioredis") {
        console.info(`Using ioredis client for Cache Components...`);
        const ioredisClient = new Redis(process.env.REDIS_URL);

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
        console.info(`Using @redis/client for Cache Components...`);
        redisClient = createClient({
          url: process.env.REDIS_URL,
          pingInterval: 10000,
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
          global.cacheComponentsHandler = null;
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

async function getRedisClient() {
  if (isSingleConnectionModeEnabled) {
    if (global.cacheComponentsHandler) {
      return global.cacheComponentsHandler;
    }
    if (global.cacheComponentsHandlerPromise) {
      return await global.cacheComponentsHandlerPromise;
    }
  }

  const promise = setupRedisClient();
  if (isSingleConnectionModeEnabled) {
    global.cacheComponentsHandlerPromise = promise;
    const client = await promise;
    if (client) {
      global.cacheComponentsHandler = client;
      global.cacheComponentsHandlerPromise = null;
    }
    return client;
  }

  return promise;
}

const keyPrefix = "cache-components:";

async function get(key) {
  const client = await getRedisClient();
  if (!client?.isReady) {
    return null;
  }

  try {
    const value = await client.get(`${keyPrefix}${key}`);
    if (!value) {
      return null;
    }
    return JSON.parse(value);
  } catch (error) {
    console.warn(`Cache get error for key ${key}:`, error);
    return null;
  }
}

async function set(key, value) {
  const client = await getRedisClient();
  if (!client?.isReady) {
    return;
  }

  try {
    await client.set(`${keyPrefix}${key}`, JSON.stringify(value));
  } catch (error) {
    console.warn(`Cache set error for key ${key}:`, error);
  }
}

async function deleteKey(key) {
  const client = await getRedisClient();
  if (!client?.isReady) {
    return;
  }

  try {
    await client.del(`${keyPrefix}${key}`);
  } catch (error) {
    console.warn(`Cache delete error for key ${key}:`, error);
  }
}

export default {
  get,
  set,
  delete: deleteKey,
};

