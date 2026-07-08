import { defineConfig, devices } from "@playwright/test";

const baseURL = process.env.BASE_URL ?? "http://127.0.0.1:3000";

export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 2 : 0,
  reporter: process.env.CI ? "github" : "list",
  use: {
    baseURL,
    trace: "on-first-retry",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    command: "pnpm start --port 3000",
    cwd: __dirname,
    url: baseURL,
    reuseExistingServer: !process.env.CI,
    env: {
      REDIS_URL: process.env.REDIS_URL ?? "redis://127.0.0.1:6379",
      REDIS_TYPE: "redis",
    },
  },
});
