import { resolveRevalidateValue } from "./resolveRevalidateValue";

describe("resolveRevalidateValue", () => {
  it("reads revalidate from a FETCH value", () => {
    const result = resolveRevalidateValue(
      { kind: "FETCH", revalidate: 60 } as any,
      {} as any,
    );

    expect(result).toBe(60);
  });

  it("reads revalidate from ctx.cacheControl for an APP_PAGE value", () => {
    const result = resolveRevalidateValue({ kind: "APP_PAGE" } as any, {
      cacheControl: { revalidate: 30 },
    } as any);

    expect(result).toBe(30);
  });

  it("reads revalidate from ctx.cacheControl for a PAGES value", () => {
    const result = resolveRevalidateValue({ kind: "PAGES" } as any, {
      cacheControl: { revalidate: 30 },
    } as any);

    expect(result).toBe(30);
  });

  it("reads revalidate from ctx.cacheControl when the incremental cache value is null (getStaticProps notFound: true)", () => {
    const result = resolveRevalidateValue(null as any, {
      cacheControl: { revalidate: 10 },
    } as any);

    expect(result).toBe(10);
  });

  it("reads revalidate from ctx.cacheControl when the incremental cache value is undefined", () => {
    const result = resolveRevalidateValue(undefined as any, {
      cacheControl: { revalidate: 10 },
    } as any);

    expect(result).toBe(10);
  });

  it("falls back to ctx.revalidate when the value has no recognized kind", () => {
    const result = resolveRevalidateValue({ kind: "UNKNOWN" } as any, {
      cacheControl: { revalidate: 10 },
      revalidate: 5,
    } as any);

    expect(result).toBe(5);
  });

  it("returns undefined when neither cacheControl.revalidate nor ctx.revalidate is set", () => {
    const result = resolveRevalidateValue(null as any, {} as any);

    expect(result).toBeUndefined();
  });
});
