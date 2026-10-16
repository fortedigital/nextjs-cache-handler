import { getPathnameFromCacheKey } from "./routeCacheKey";

describe("getPathnameFromCacheKey", () => {
  const hash =
    "c063b63ca22df4c818d676a2e68103fe5caaafe3e3eae724febae544077120b5";

  it("recovers the pathname from a Next.js 16.3.8 route-scoped key", () => {
    expect(
      getPathnameFromCacheKey(`/route-cache/PAGES/${hash}/$/posts/1`),
    ).toBe("/posts/1");
    expect(
      getPathnameFromCacheKey(`/route-cache/APP_PAGE/${hash}/$/blog/[slug]`),
    ).toBe("/blog/[slug]");
  });

  it("undoes Next.js page-path normalization", () => {
    expect(getPathnameFromCacheKey(`/route-cache/PAGES/${hash}/$/index`)).toBe(
      "/",
    );
    expect(
      getPathnameFromCacheKey(`/route-cache/PAGES/${hash}/$/index/index`),
    ).toBe("/index");
  });

  it("returns pathname and fetch-cache keys unchanged", () => {
    expect(getPathnameFromCacheKey("/posts/1")).toBe("/posts/1");
    expect(getPathnameFromCacheKey("a1b2c3d4e5")).toBe("a1b2c3d4e5");
  });
});
