const cache = new Map()
const pendingSets = new Map()
 
// TODO: Replace basic in-memory cache with Redis implementation using nextjs-cache-handler library

module.exports = {
  async get(cacheKey, softTags) {
    console.log("[Cache Handler] get called", cacheKey)
    // Wait for any pending set operation to complete
    const pendingPromise = pendingSets.get(cacheKey)
    if (pendingPromise) {
      await pendingPromise
    }
 
    const entry = cache.get(cacheKey)
    if (!entry) {
      return undefined
    }
 
    // Check if entry has expired
    const now = Date.now()
    if (now > entry.timestamp + entry.revalidate * 1000) {
      return undefined
    }
 
    return entry
  },
 
  async set(cacheKey, pendingEntry) {
    console.log("[Cache Handler] set called", cacheKey)
    // Create a promise to track this set operation
    let resolvePending
    const pendingPromise = new Promise((resolve) => {
      resolvePending = resolve
    })
    pendingSets.set(cacheKey, pendingPromise)
 
    try {
      // Wait for the entry to be ready
      const entry = await pendingEntry
 
      // Store the entry in the cache
      cache.set(cacheKey, entry)
    } finally {
      resolvePending()
      pendingSets.delete(cacheKey)
    }
  },
 
  async refreshTags() {
    // No-op for in-memory cache
  },
 
  async getExpiration(tags) {
    // Return 0 to indicate no tags have been revalidated
    return 0
  },
 
  async updateTags(tags, durations) {
    // Implement tag-based invalidation
    for (const [key, entry] of cache.entries()) {
      if (entry.tags.some((tag) => tags.includes(tag))) {
        cache.delete(key)
      }
    }
  },
}