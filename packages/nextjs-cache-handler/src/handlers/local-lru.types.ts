/**
 * Configuration options for the LRU cache.
 */

export type LruCacheOptions = {
  /**
   * Optional. Maximum number of items the cache can hold.
   *
   * @default 1000
   */
  maxItemsNumber?: number;
  /**
   * Optional. Maximum size in bytes for each item in the cache.
   *
   * @default 104857600 // 100 Mb
   */
  maxItemSizeBytes?: number;
};
