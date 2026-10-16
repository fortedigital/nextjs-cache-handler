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
