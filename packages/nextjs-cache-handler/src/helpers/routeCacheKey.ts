import { createRequire } from "node:module";
import path from "node:path";

/**
 * Next.js 16.3.8 (GHSA-4jqv-mc3x-m676, GHSA-mcj8-r9mp-w47p) stopped storing
 * response-cache entries (APP_PAGE, APP_ROUTE, PAGES) under their pathname.
 * The key a cache handler receives is now scoped by the route that owns it:
 *
 *   /route-cache/<kind>/<sha256(source route)>/$<normalized pathname>
 *
 * Fetch-cache keys are unchanged. Older Next.js versions keep pathname keys.
 */
const ROUTE_CACHE_KEY_PREFIX = "/route-cache/";

type RouteCacheOwner = {
  kind: "APP_PAGE" | "APP_ROUTE" | "PAGES";
  sourceRoute: string;
};

type GetRouteCacheKey = (pathname: string, owner: RouteCacheOwner) => string;

/**
 * Returns Next.js' own `getRouteCacheKey` when the installed Next.js has it
 * (16.3.8+), otherwise `null`. Resolved from the app's working directory so it
 * is the Next.js instance that will read the cache, not a copy bundled here.
 */
export function loadGetRouteCacheKey(): GetRouteCacheKey | null {
  try {
    const appRequire = createRequire(path.join(process.cwd(), "package.json"));
    const mod = appRequire("next/dist/server/lib/route-cache-key") as {
      getRouteCacheKey?: GetRouteCacheKey;
    };
    return typeof mod.getRouteCacheKey === "function"
      ? mod.getRouteCacheKey
      : null;
  } catch {
    return null;
  }
}

/**
 * Recovers the pathname a cache key belongs to. Route-scoped keys carry it
 * after `/$` (normalized: `/` is stored as `/index`); any other key is already
 * a pathname (or a fetch-cache key) and is returned unchanged.
 */
export function getPathnameFromCacheKey(cacheKey: string): string {
  if (!cacheKey.startsWith(ROUTE_CACHE_KEY_PREFIX)) {
    return cacheKey;
  }

  const separator = cacheKey.indexOf("/$");

  if (separator === -1) {
    return cacheKey;
  }

  const normalized = cacheKey.slice(separator + 2);

  if (normalized === "/index") {
    return "/";
  }

  return normalized.startsWith("/index/") ? normalized.slice(6) : normalized;
}

function stripLocale(pathname: string, locales: string[]): string {
  const [, first, ...rest] = pathname.split("/");
  const isLocale = locales.some(
    (locale) => locale.toLowerCase() === first?.toLowerCase(),
  );
  return isLocale ? `/${rest.join("/")}` : pathname;
}

type PrerenderRoute = {
  srcRoute?: string | null;
  dataRoute?: string | null;
};

/**
 * Builds a resolver from a prerendered pathname to the key Next.js will look
 * it up under. Returns `undefined` when the installed Next.js predates
 * route-scoped keys, so callers keep writing pathname keys.
 *
 * App Router owners are the page/route module (`/blog/[slug]/page`), taken
 * from `app-path-routes-manifest.json`; Pages Router owners are the page
 * pathname (`/posts/[id]`), i.e. the prerender entry's `srcRoute`.
 */
export function createRouteCacheKeyResolver(
  routes: Record<string, PrerenderRoute>,
  appPathRoutes: Record<string, string>,
  locales: string[] = [],
  getRouteCacheKey: GetRouteCacheKey | null = loadGetRouteCacheKey(),
): ((pathname: string) => string) | undefined {
  if (!getRouteCacheKey) {
    return undefined;
  }

  // { "/blog/[slug]/page": "/blog/[slug]" } -> { "/blog/[slug]": "/blog/[slug]/page" }
  const appPageByRoute = new Map(
    Object.entries(appPathRoutes).map(([page, route]) => [route, page]),
  );

  return (pathname: string) => {
    const route = routes[pathname];
    const sourceRoute = route?.srcRoute ?? pathname;
    const dataRoute = route?.dataRoute;

    if (dataRoute?.endsWith(".json")) {
      // Non-dynamic i18n pages have no srcRoute; Next.js strips the locale.
      return getRouteCacheKey(pathname, {
        kind: "PAGES",
        sourceRoute: route?.srcRoute ?? stripLocale(pathname, locales),
      });
    }

    const appPage = appPageByRoute.get(sourceRoute);

    if (!appPage) {
      return pathname;
    }

    return getRouteCacheKey(pathname, {
      kind: dataRoute === null ? "APP_ROUTE" : "APP_PAGE",
      sourceRoute: appPage,
    });
  };
}
