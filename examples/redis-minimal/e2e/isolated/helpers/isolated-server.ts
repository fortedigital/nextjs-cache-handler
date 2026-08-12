import { spawn, exec as execCallback, type ChildProcess } from "node:child_process";
import { promisify } from "node:util";
import path from "node:path";
import { createClient } from "redis";

const exec = promisify(execCallback);

export type IsolatedRedis = {
  containerName: string;
  port: number;
  url: string;
};

export type IsolatedNextServer = {
  process: ChildProcess;
  port: number;
  baseURL: string;
  getOutput: () => { stdout: string; stderr: string };
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function randomSuffix(): string {
  return Math.random().toString(36).slice(2, 10);
}

export async function startEphemeralRedis(
  opts: { port?: number; image?: string } = {},
): Promise<IsolatedRedis> {
  const port = opts.port ?? 16380;
  const image = opts.image ?? "redis:7-alpine";
  const containerName = `nca-e2e-redis-${randomSuffix()}`;

  await exec(`docker run -d --rm --name ${containerName} -p ${port}:6379 ${image}`);

  return { containerName, port, url: `redis://127.0.0.1:${port}` };
}

export async function stopEphemeralRedis(redis: IsolatedRedis): Promise<void> {
  try {
    await exec(`docker rm -f ${redis.containerName}`);
  } catch {
    // container may already be gone (e.g. --rm already cleaned it up on stop) - nothing to do
  }
}

export async function waitForRedisReady(
  url: string,
  opts: { timeoutMs?: number; intervalMs?: number } = {},
): Promise<void> {
  const timeoutMs = opts.timeoutMs ?? 30_000;
  const intervalMs = opts.intervalMs ?? 250;
  const deadline = Date.now() + timeoutMs;

  let lastError: unknown;
  while (Date.now() < deadline) {
    const client = createClient({ url });
    try {
      await client.connect();
      await client.ping();
      await client.quit();
      return;
    } catch (error) {
      lastError = error;
      await client.quit().catch(() => {});
      await sleep(intervalMs);
    }
  }

  throw new Error(`Redis at ${url} was not ready after ${timeoutMs}ms: ${String(lastError)}`);
}

/**
 * Polls Redis directly for the signal that `registerInitialCache` has finished
 * writing a specific, known set of routes, rather than relying on HTTP readiness
 * (which says nothing about `register()` having completed) or on the shared-tags
 * hash simply being non-empty (which can be true while other routes, processed
 * concurrently under `registerInitialCache`'s own parallelism limit, are still
 * being written - `HLEN > 0` alone is not a reliable "population finished" signal).
 */
export async function waitForRedisPopulated(
  url: string,
  expectedCachePaths: string[],
  opts: { timeoutMs?: number; intervalMs?: number; hashKey?: string } = {},
): Promise<void> {
  const timeoutMs = opts.timeoutMs ?? 30_000;
  const intervalMs = opts.intervalMs ?? 250;
  const hashKey = opts.hashKey ?? "nextjs:__sharedTags__";
  const deadline = Date.now() + timeoutMs;

  const client = createClient({ url });
  await client.connect();
  try {
    while (Date.now() < deadline) {
      const results = await Promise.all(
        expectedCachePaths.map((cachePath) => client.hExists(hashKey, cachePath)),
      );
      if (results.every(Boolean)) return;
      await sleep(intervalMs);
    }
    throw new Error(
      `Redis hash "${hashKey}" did not contain all expected cache paths (${expectedCachePaths.join(", ")}) after ${timeoutMs}ms`,
    );
  } finally {
    await client.quit();
  }
}

export async function startIsolatedNextServer(opts: {
  port?: number;
  cwd?: string;
  env: Record<string, string>;
}): Promise<IsolatedNextServer> {
  const port = opts.port ?? 3010;
  const cwd = opts.cwd ?? path.resolve(__dirname, "..", "..", "..");

  const child = spawn(`pnpm exec next start --port ${port}`, [], {
    cwd,
    env: { ...process.env, ...opts.env },
    shell: true,
    detached: process.platform !== "win32",
    stdio: ["ignore", "pipe", "pipe"],
  });

  let stdout = "";
  let stderr = "";
  child.stdout?.on("data", (chunk) => {
    stdout += chunk.toString();
  });
  child.stderr?.on("data", (chunk) => {
    stderr += chunk.toString();
  });

  return {
    process: child,
    port,
    baseURL: `http://127.0.0.1:${port}`,
    getOutput: () => ({ stdout, stderr }),
  };
}

export async function waitForHttpReady(
  baseURL: string,
  opts: { timeoutMs?: number; intervalMs?: number } = {},
): Promise<void> {
  const timeoutMs = opts.timeoutMs ?? 30_000;
  const intervalMs = opts.intervalMs ?? 250;
  const deadline = Date.now() + timeoutMs;

  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(baseURL);
      if (response.status < 500) return;
    } catch (error) {
      lastError = error;
    }
    await sleep(intervalMs);
  }

  throw new Error(`Server at ${baseURL} was not ready after ${timeoutMs}ms: ${String(lastError)}`);
}

export async function stopIsolatedNextServer(server: IsolatedNextServer): Promise<void> {
  const { process: child } = server;
  if (child.exitCode !== null || child.pid === undefined) return;

  if (process.platform === "win32") {
    await exec(`taskkill /pid ${child.pid} /T /F`).catch(() => {});
  } else {
    try {
      process.kill(-child.pid, "SIGTERM");
    } catch {
      child.kill("SIGTERM");
    }
  }

  await new Promise<void>((resolve) => {
    if (child.exitCode !== null) {
      resolve();
      return;
    }
    child.once("exit", () => resolve());
    setTimeout(resolve, 5000);
  });
}
