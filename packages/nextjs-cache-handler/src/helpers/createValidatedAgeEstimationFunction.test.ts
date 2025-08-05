import { createValidatedAgeEstimationFunction } from "./createValidatedAgeEstimationFunction";
import { MAX_INT32 } from "./const";

describe("createValidatedAgeEstimationFunction", () => {
  it("returns the same age for simple callback", () => {
    const estimateAge = createValidatedAgeEstimationFunction((age) => age);
    const testAge = 100;

    expect(estimateAge(testAge)).toBe(testAge);
  });

  it("throws error for negative age", () => {
    const estimateAge = createValidatedAgeEstimationFunction((age) => age);
    const testAge = -1;

    expect(() => estimateAge(testAge)).toThrow(
      /The expire age must be a positive integer but got/,
    );
  });

  it("handles float by flooring", () => {
    const estimateAge = createValidatedAgeEstimationFunction(
      (age) => age + 0.9,
    );
    const testAge = 100;

    expect(estimateAge(testAge)).toBe(100);
  });

  it("handles numbers bigger than MAX_INT32 by returning MAX_INT32", () => {
    const estimateAge = createValidatedAgeEstimationFunction(
      (age) => age + MAX_INT32,
    );

    expect(estimateAge(100)).toBe(MAX_INT32);
  });

  it("throws error for non-integer", () => {
    const estimateAge = createValidatedAgeEstimationFunction(
      (age) => age + Number.NaN,
    );

    expect(() => estimateAge(10)).toThrow(
      /The expire age must be a positive integer but got/,
    );
  });

  it("throws error for zero", () => {
    const estimateAge = createValidatedAgeEstimationFunction((age) => age * 0);

    expect(() => estimateAge(10)).toThrow(
      /The expire age must be a positive integer but got/,
    );
  });

  it("handles MAX_INT32 correctly", () => {
    const estimateAge = createValidatedAgeEstimationFunction(
      (_age) => MAX_INT32,
    );

    expect(estimateAge(0)).toBe(MAX_INT32);
  });

  it("throws error for non-numeric input", () => {
    const estimateAge = createValidatedAgeEstimationFunction(
      (_age) => "non-numeric" as unknown as number,
    );

    expect(() => estimateAge(10)).toThrow(
      /The expire age must be a positive integer but got/,
    );
  });

  it("propagates callback errors", () => {
    const estimateAge = createValidatedAgeEstimationFunction(() => {
      throw new Error("Test error");
    });

    expect(() => estimateAge(10)).toThrow(/Test error/);
  });
});
