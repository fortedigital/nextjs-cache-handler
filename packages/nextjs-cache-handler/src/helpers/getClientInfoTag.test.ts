import { getClientInfoTag } from "./getClientInfoTag";

describe("getClientInfoTag", () => {
  it("should return a string starting with 'nextjs-cache-handler'", () => {
    const result = getClientInfoTag();

    expect(result).toMatch(/^nextjs-cache-handler/);
  });

  it("should return a string with version in format 'nextjs-cache-handler_vX.X.X'", () => {
    const result = getClientInfoTag();

    // Should match pattern like "nextjs-cache-handler_v2.5.1"
    expect(result).toMatch(/^nextjs-cache-handler_v\d+\.\d+\.\d+$/);
  });

  it("should include the correct package version", () => {
    const result = getClientInfoTag();

    // The version should be 2.5.1 based on package.json
    expect(result).toBe("nextjs-cache-handler_v2.5.1");
  });

  it("should return consistent results on multiple calls", () => {
    const result1 = getClientInfoTag();
    const result2 = getClientInfoTag();

    expect(result1).toBe(result2);
  });
});

