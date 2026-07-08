import { NEXT_CACHE_IMPLICIT_TAG_ID } from "./const";

/**
 * Builds the implicit path tag Next.js uses for `revalidatePath`.
 */
export function getImplicitPathTag(cachePath: string): string {
  return `${NEXT_CACHE_IMPLICIT_TAG_ID}${cachePath}`;
}
