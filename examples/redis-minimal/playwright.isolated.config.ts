import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e/isolated",
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? "github" : "list",
  // Container start + `next start` boot needs more headroom than the default,
  // plus slack for a cold `docker pull` on a fresh CI runner where the image
  // isn't cached yet.
  timeout: 90_000,
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  // Deliberately no `webServer` block: each spec here manages its own ephemeral
  // Redis container and `next start` process via ./e2e/isolated/helpers/isolated-server.ts,
  // so it can assert Redis state that can only be explained by registerInitialCache,
  // independent of the shared suite's server/Redis (see playwright.config.ts).
});
