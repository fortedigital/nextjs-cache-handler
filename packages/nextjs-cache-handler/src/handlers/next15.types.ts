import {
  CachedRedirectValue,
  CachedImageValue,
  CachedFetchValue,
} from "next/dist/server/response-cache";
import { OutgoingHttpHeaders } from "http";

export interface CachedRouteValue {
  kind: "APP_ROUTE";
  body: Buffer;
  status: number;
  headers: OutgoingHttpHeaders;
}

export interface IncrementalCachedPageValue {
  kind: "APP_PAGE";
  html: string;
  pageData: Object;
  postponed: string | undefined;
  headers: OutgoingHttpHeaders | undefined;
  status: number | undefined;
}

export type Next15IncrementalCacheValue =
  | CachedRedirectValue
  | IncrementalCachedPageValue
  | CachedImageValue
  | CachedFetchValue
  | CachedRouteValue;
