import { Handler } from "./cache-handler.types";
import { CreateCompositeHandlerOptions } from "./composite.types";

/**
 * Creates a composite Handler for managing cache operations using multiple handlers.
 *
 * This function initializes a composite Handler that coordinates cache operations
 * across multiple handlers, providing methods for getting, setting, and revalidating cache values.
 *
 * @param options - The configuration options for the composite handler. See {@link CreateCompositeHandlerOptions}.
 *
 * @returns An object representing the composite cache handler, with methods for cache operations.
 *
 * @remarks
 * - The `get` method retrieves a value from the first handler that returns a non-null result, in the order defined by the handlers array.
 * - The `set` method uses the `setStrategy` to determine which handler should store the data. If no strategy is provided, the first handler is used.
 * - The `revalidateTag` and `delete` methods apply the operation across all handlers in parallel.
 */
export default function createHandler({
  handlers,
  setStrategy: strategy,
}: CreateCompositeHandlerOptions): Handler {
  if (handlers?.length < 2) {
    throw new Error("Composite requires at least two handlers");
  }

  return {
    name: "composite",

    async get(key, ctx) {
      for (const handler of handlers) {
        const value = await handler.get(key, ctx);
        if (value !== null) {
          return value;
        }
      }
      return null;
    },

    async set(key, data) {
      const index = strategy?.(data) ?? 0;
      const handler = handlers[index] ?? handlers[0]!;
      await handler.set(key, data);
    },

    async revalidateTag(tag) {
      await Promise.all(handlers.map((handler) => handler.revalidateTag(tag)));
    },

    async delete(key) {
      await Promise.all(handlers.map((handler) => handler.delete?.(key)));
    },
  };
}
