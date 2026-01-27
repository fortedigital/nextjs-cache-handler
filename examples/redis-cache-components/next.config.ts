import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  cacheComponents: true,
  cacheHandlers: {
    remote: require.resolve("./cache-handler.mjs"),
  },
};

export default nextConfig;

