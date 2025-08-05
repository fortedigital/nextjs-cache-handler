import { withAbortSignal } from "./withAbortSignal";

describe("withAbortSignal", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe("when no AbortSignal is provided", () => {
    it("should execute the promise function normally and resolve", async () => {
      const mockPromise = jest.fn().mockResolvedValue("success");

      const result = await withAbortSignal(mockPromise);

      expect(result).toBe("success");
      expect(mockPromise).toHaveBeenCalledTimes(1);
    });

    it("should execute the promise function normally and reject", async () => {
      const error = new Error("test error");
      const mockPromise = jest.fn().mockRejectedValue(error);

      await expect(withAbortSignal(mockPromise)).rejects.toThrow("test error");
      expect(mockPromise).toHaveBeenCalledTimes(1);
    });
  });

  describe("when AbortSignal is provided", () => {
    it("should resolve successfully when promise completes before abort", async () => {
      const controller = new AbortController();
      const mockPromise = jest.fn().mockResolvedValue("success");

      const result = await withAbortSignal(mockPromise, controller.signal);

      expect(result).toBe("success");
      expect(mockPromise).toHaveBeenCalledTimes(1);
    });

    it("should reject with original error when promise fails before abort", async () => {
      const controller = new AbortController();
      const error = new Error("promise error");
      const mockPromise = jest.fn().mockRejectedValue(error);

      await expect(
        withAbortSignal(mockPromise, controller.signal),
      ).rejects.toThrow("promise error");
      expect(mockPromise).toHaveBeenCalledTimes(1);
    });

    it("should reject immediately if signal is already aborted", async () => {
      const controller = new AbortController();
      controller.abort();
      const mockPromise = jest.fn().mockResolvedValue("success");

      await expect(
        withAbortSignal(mockPromise, controller.signal),
      ).rejects.toThrow("Aborted");
      expect(mockPromise).not.toHaveBeenCalled();
    });

    it("should reject with abort error when signal is aborted during promise execution", async () => {
      const controller = new AbortController();
      let resolvePromise;
      const mockPromise = jest.fn(
        () =>
          new Promise((resolve) => {
            resolvePromise = resolve;
          }),
      );

      const promiseResult = withAbortSignal(mockPromise, controller.signal);

      // Abort after promise has started but before it completes
      setTimeout(() => controller.abort(), 10);

      await expect(promiseResult).rejects.toThrow("Operation aborted");
      expect(mockPromise).toHaveBeenCalledTimes(1);
    });

    it("should not reject with abort error if promise resolves before abort", async () => {
      const controller = new AbortController();
      const mockPromise = jest.fn().mockResolvedValue("quick success");

      const promiseResult = withAbortSignal(mockPromise, controller.signal);

      // Try to abort immediately after (but promise should resolve first)
      setTimeout(() => controller.abort(), 0);

      const result = await promiseResult;
      expect(result).toBe("quick success");
    });

    it("should not reject with abort error if promise rejects before abort", async () => {
      const controller = new AbortController();
      const error = new Error("quick error");
      const mockPromise = jest.fn().mockRejectedValue(error);

      const promiseResult = withAbortSignal(mockPromise, controller.signal);

      // Try to abort immediately after (but promise should reject first)
      setTimeout(() => controller.abort(), 0);

      await expect(promiseResult).rejects.toThrow("quick error");
    });

    it("should clean up event listener when promise resolves", async () => {
      const controller = new AbortController();
      const mockPromise = jest.fn().mockResolvedValue("success");
      const removeEventListenerSpy = jest.spyOn(
        controller.signal,
        "removeEventListener",
      );

      await withAbortSignal(mockPromise, controller.signal);

      expect(removeEventListenerSpy).toHaveBeenCalledWith(
        "abort",
        expect.any(Function),
      );
    });

    it("should clean up event listener when promise rejects", async () => {
      const controller = new AbortController();
      const error = new Error("test error");
      const mockPromise = jest.fn().mockRejectedValue(error);
      const removeEventListenerSpy = jest.spyOn(
        controller.signal,
        "removeEventListener",
      );

      await expect(
        withAbortSignal(mockPromise, controller.signal),
      ).rejects.toThrow("test error");

      expect(removeEventListenerSpy).toHaveBeenCalledWith(
        "abort",
        expect.any(Function),
      );
    });

    it("should handle multiple abort calls gracefully", async () => {
      const controller = new AbortController();
      let resolvePromise;
      const mockPromise = jest.fn(
        () =>
          new Promise((resolve) => {
            resolvePromise = resolve;
          }),
      );

      const promiseResult = withAbortSignal(mockPromise, controller.signal);

      // Abort multiple times
      setTimeout(() => {
        controller.abort();
        controller.abort(); // This should be safe
      }, 10);

      await expect(promiseResult).rejects.toThrow("Operation aborted");
    });
  });

  describe("edge cases", () => {
    it("should handle undefined signal gracefully", async () => {
      const mockPromise = jest.fn().mockResolvedValue("success");

      const result = await withAbortSignal(mockPromise, undefined);

      expect(result).toBe("success");
      expect(mockPromise).toHaveBeenCalledTimes(1);
    });

    it("should handle null signal gracefully", async () => {
      const mockPromise = jest.fn().mockResolvedValue("success");

      const result = await withAbortSignal(mockPromise, null!);

      expect(result).toBe("success");
      expect(mockPromise).toHaveBeenCalledTimes(1);
    });

    it("should preserve the original promise result type", async () => {
      const controller = new AbortController();
      const complexResult = { data: [1, 2, 3], status: "ok" };
      const mockPromise = jest.fn().mockResolvedValue(complexResult);

      const result = await withAbortSignal(mockPromise, controller.signal);

      expect(result).toEqual(complexResult);
      expect(result).toBe(complexResult); // Should be the exact same reference
    });

    it("should work with async function as promiseFn", async () => {
      const controller = new AbortController();
      const asyncFn = async () => {
        await new Promise((resolve) => setTimeout(resolve, 10));
        return "async result";
      };

      const result = await withAbortSignal(asyncFn, controller.signal);

      expect(result).toBe("async result");
    });
  });
});
