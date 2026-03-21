import { createClient } from "redis";
import { PHASE_PRODUCTION_BUILD } from "next/constants.js";
import createRedisHandler from "@fortedigital/nextjs-cache-handler/redis-strings-cache-components";

const client = createClient({
  url: process.env.REDIS_URL || "redis://localhost:6379",
});

client.on("error", (err) => {
  if (process.env.NEXT_PRIVATE_DEBUG_CACHE !== undefined) {
    console.error("Redis Client Error", err);
  }
});

// Only connect to Redis outside of `next build` — Redis is not available
// during the build phase. The handler checks client.isReady on each method
// call, so an unconnected client at build time is safe.
if (process.env.NEXT_PHASE !== PHASE_PRODUCTION_BUILD) {
  client.connect().catch((err) => {
    console.warn("Redis connection failed:", err.message);
  });
}

export default createRedisHandler({
  client,
  keyPrefix: process.env.CACHE_PREFIX || "",
});
