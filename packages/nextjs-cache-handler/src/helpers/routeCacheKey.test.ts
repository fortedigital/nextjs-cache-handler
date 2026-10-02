import {
  createRouteCacheKeyResolver,
  getPathnameFromCacheKey,
  loadGetRouteCacheKey,
} from "./routeCacheKey";

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

describe("createRouteCacheKeyResolver", () => {
  const routes = {
    "/blog/hello": { srcRoute: "/blog/[slug]", dataRoute: "/blog/hello.rsc" },
    "/feed.xml": { srcRoute: "/feed.xml", dataRoute: null },
    "/posts/1": {
      srcRoute: "/posts/[id]",
      dataRoute: "/_next/data/b/posts/1.json",
    },
    "/de/about": { srcRoute: null, dataRoute: "/_next/data/b/de/about.json" },
  };
  const appPathRoutes = {
    "/(marketing)/blog/[slug]/page": "/blog/[slug]",
    "/feed.xml/route": "/feed.xml",
  };
  const fakeGetRouteCacheKey = (
    pathname: string,
    owner: { kind: string; sourceRoute: string },
  ) => `${owner.kind}|${owner.sourceRoute}|${pathname}`;

  it("keeps pathname keys on Next.js versions without route-scoped keys", () => {
    expect(
      createRouteCacheKeyResolver(routes, appPathRoutes, [], null),
    ).toBeUndefined();
  });

  it("scopes App Router pages by their module, including route groups", () => {
    const resolve = createRouteCacheKeyResolver(
      routes,
      appPathRoutes,
      [],
      fakeGetRouteCacheKey,
    );
    expect(resolve?.("/blog/hello")).toBe(
      "APP_PAGE|/(marketing)/blog/[slug]/page|/blog/hello",
    );
    expect(resolve?.("/feed.xml")).toBe("APP_ROUTE|/feed.xml/route|/feed.xml");
  });

  it("scopes Pages Router pages by their page pathname, without the locale", () => {
    const resolve = createRouteCacheKeyResolver(
      routes,
      appPathRoutes,
      ["en", "de"],
      fakeGetRouteCacheKey,
    );
    expect(resolve?.("/posts/1")).toBe("PAGES|/posts/[id]|/posts/1");
    expect(resolve?.("/de/about")).toBe("PAGES|/about|/de/about");
  });

  it("leaves paths it cannot attribute to a route unchanged", () => {
    const resolve = createRouteCacheKeyResolver(
      routes,
      appPathRoutes,
      [],
      fakeGetRouteCacheKey,
    );
    expect(resolve?.("/unknown")).toBe("/unknown");
  });
});

describe("loadGetRouteCacheKey", () => {
  it("does not throw when Next.js lacks route-scoped keys", () => {
    expect(() => loadGetRouteCacheKey()).not.toThrow();
  });
});
