export interface CacheComponentsEntry {
  value: ReadableStream<Uint8Array>;
  tags: string[];
  stale: number;
  timestamp: number;
  expire: number;
  revalidate: number;
}

export interface CacheComponentsHandler {
  get(
    cacheKey: string,
    softTags: string[],
  ): Promise<CacheComponentsEntry | undefined>;
  set(
    cacheKey: string,
    pendingEntry: Promise<CacheComponentsEntry>,
  ): Promise<void>;
  refreshTags(): Promise<void>;
  getExpiration(tags: string[]): Promise<number>;
  updateTags(tags: string[], durations?: { expire?: number }): Promise<void>;
}
