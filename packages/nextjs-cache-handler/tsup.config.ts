import { defineConfig } from "tsup";

export const tsup = defineConfig({
  name: "Build cache-handler",
  entry: ["src/handlers/*.ts"],
  splitting: false,
  outDir: "dist",
  clean: false,
  format: ["cjs", "esm"],
  dts: { resolve: true },
  target: "node18",
  noExternal: ["lru-cache", "cluster-key-slot"],
});
