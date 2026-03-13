const { createClient } = require("redis")
const createRedisHandler =
  require("@fortedigital/nextjs-cache-handler/redis-strings-cache-components").default

const client = createClient({
  url: process.env.REDIS_URL || "redis://localhost:6379",
})

client.on("error", (err) => {
  console.error("Redis Client Error", err)
})

client.connect().catch(console.error)

module.exports = createRedisHandler({
  client,
  keyPrefix: process.env.CACHE_PREFIX || "",
})
