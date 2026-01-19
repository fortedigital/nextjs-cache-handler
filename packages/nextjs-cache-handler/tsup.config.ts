import { defineConfig } from "tsup";

export const tsup = defineConfig({
  name: "Build cache-handler",
  entry: [
    "src/handlers/*.ts",
    "src/instrumentation/*.ts",
    "src/functions/*.ts",
    "src/helpers/redisClusterAdapter.ts",
    "src/helpers/withAbortSignal.ts",
    "src/helpers/withAbortSignalProxy.ts",
    "src/helpers/ioredisAdapter.ts",
  ],
  splitting: false,
  outDir: "dist",
  clean: false,
  format: ["cjs", "esm"],
  dts: { resolve: true },
  target: "node18",
  noExternal: ["lru-cache", "cluster-key-slot"],
});
