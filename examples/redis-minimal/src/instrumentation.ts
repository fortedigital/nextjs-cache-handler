export const register = async () => {
  if (process.env.NEXT_RUNTIME === "nodejs") {
    const { registerInitialCache } =
      await import("@fortedigital/nextjs-cache-handler/instrumentation");
    const CacheHandler = (await import("../cache-handler.mjs")).default;
    await registerInitialCache(CacheHandler, {
      setOnlyIfNotExists:
        process.env.INITIAL_CACHE_SET_ONLY_IF_NOT_EXISTS === "true",
    });
  }
};
