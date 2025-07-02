import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  cacheHandler:
    process.env.NODE_ENV === "production"
      ? require.resolve("./cache-handler.mjs")
      : undefined,
  cacheMaxMemorySize: 0, // disable default in-memory caching
};

export default nextConfig;
