import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  cacheComponents: true,
  cacheHandlers: {
    default: require.resolve("./cache-handler.mjs"),
    remote: require.resolve("./cache-handler.mjs"),
  },
};

export default nextConfig;

