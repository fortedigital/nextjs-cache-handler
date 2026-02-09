import { readFileSync } from "node:fs";
import { resolve } from "node:path";

function getPackageVersion(): string | undefined {
  try {
    const packageJsonPath = resolve(__dirname, '..', '..', 'package.json')
    const content = readFileSync(packageJsonPath, 'utf-8')
    const packageJson = JSON.parse(content) as { version: string }
    return packageJson.version
  } catch {
    return undefined
  }
}

/**
 * Returns a client info tag string for Redis client identification.
 *
 * This function reads the version from `@fortedigital/nextjs-cache-handler/package.json`
 * and returns a formatted string like `nextjs-cache-handler_v2.5.1`.
 *
 * If the version cannot be read, it falls back to `nextjs-cache-handler`.
 *
 * @example
 * ```js
 * import { createClient } from 'redis';
 * import { getClientInfoTag } from '@fortedigital/nextjs-cache-handler/helpers/getClientInfoTag';
 *
 * const client = createClient({
 *   url: 'redis://localhost:6379',
 *   clientInfoTag: getClientInfoTag(),
 * });
 * ```
 *
 * @returns The client info tag string for Redis identification.
 */
export function getClientInfoTag(): string {
  const version = getPackageVersion();
  if (version) {
    return `nextjs-cache-handler_v${version}`;
  }
  return "nextjs-cache-handler";
}

