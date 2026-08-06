import { resolveRevalidateValue } from "./resolveRevalidateValue";

describe("resolveRevalidateValue", () => {
  it("uses cacheControl revalidate for app routes", () => {
    const value = { kind: "APP_ROUTE" };
    const context = {
      cacheControl: { revalidate: 3600 },
      revalidate: 31536000,
    };

    expect(resolveRevalidateValue(value as never, context as never)).toBe(3600);
  });

  it("keeps the existing app page behavior", () => {
    const value = { kind: "APP_PAGE" };
    const context = {
      cacheControl: { revalidate: 600 },
      revalidate: 31536000,
    };

    expect(resolveRevalidateValue(value as never, context as never)).toBe(600);
  });

  it("falls back to context revalidate when cache control is absent", () => {
    const value = { kind: "APP_ROUTE" };
    const context = { revalidate: 120 };

    expect(resolveRevalidateValue(value as never, context as never)).toBe(120);
  });
});
