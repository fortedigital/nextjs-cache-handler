import { createRequire } from "node:module";
import path from "node:path";

type RouteCacheOwner = {
  kind: "APP_PAGE" | "APP_ROUTE" | "PAGES";
  sourceRoute: string;
};

// Owners are written out by hand (the App Router module / Pages Router page
// that renders each path) so these expectations don't reuse the library's own
// resolution logic.
const OWNERS: Record<string, RouteCacheOwner> = {
  "/": {
    kind: "APP_PAGE",
    sourceRoute: "/page",
  },
  "/examples/default-cache": {
    kind: "APP_PAGE",
    sourceRoute: "/examples/default-cache/page",
  },
  "/examples/fetch-tags": {
    kind: "APP_PAGE",
    sourceRoute: "/examples/fetch-tags/page",
  },
  "/posts/1": { kind: "PAGES", sourceRoute: "/posts/[id]" },
};

const exampleRequire = createRequire(
  path.resolve(__dirname, "..", "..", "..", "package.json"),
);

function loadGetRouteCacheKey():
  ((pathname: string, owner: RouteCacheOwner) => string) | null {
  try {
    return exampleRequire("next/dist/server/lib/route-cache-key")
      .getRouteCacheKey;
  } catch {
    return null;
  }
}

const getRouteCacheKey = loadGetRouteCacheKey();

/**
 * The key Next.js itself reads a prerendered path from: route-scoped on
 * Next.js 16.3.8+ (`/route-cache/<kind>/<hash>/$<path>`), the pathname before.
 * Seeding must land exactly here, or every boot-time entry is a dead key.
 */
export function cacheKeyFor(pathname: string): string {
  const owner = OWNERS[pathname];
  if (!owner) {
    throw new Error(`No owner recorded for ${pathname}`);
  }
  if (getRouteCacheKey) {
    return getRouteCacheKey(pathname, owner);
  }
  // Before 16.3.8, Next normalises "/" to "/index" before calling the handler.
  return pathname === "/" ? "/index" : pathname;
}
