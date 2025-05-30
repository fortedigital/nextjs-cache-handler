import { Handler, CacheHandlerValue } from "./cache-handler.types";

export type CreateCompositeHandlerOptions = {
  /**
   * A collection of handlers to manage the cache.
   */
  handlers: Handler[];

  /**
   * Strategy to determine which handler to use for the set operation.
   * Defaults to the first handler if not provided.
   * @param data - The data to be saved in one of the handlers. See {@link CacheHandlerValue}.
   * @returns The index of the handler for the set operation.
   */
  setStrategy: (data: CacheHandlerValue) => number;
};
