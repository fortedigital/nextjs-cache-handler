export type CachedRouteValue = {
  // See: https://github.com/vercel/next.js/blob/f5444a16ec2ef7b82d30048890b613aa3865c1f1/packages/next/src/server/response-cache/types.ts#L97
  kind: "APP_ROUTE";
  body: Buffer | undefined;
};

export type ConvertedCachedRouteValue = {
  kind: "ROUTE";
  body: string | undefined;
};

export type CachedAppPageValue = {
  // See: https://github.com/vercel/next.js/blob/f5444a16ec2ef7b82d30048890b613aa3865c1f1/packages/next/src/server/response-cache/types.ts#L76
  kind: "APP_PAGE";
  rscData: Buffer | undefined;
  segmentData: Map<string, Buffer> | undefined;
};

export type ConvertedCachedAppPageValue = {
  kind: "PAGE";
  rscData: string | undefined;
  segmentData: Record<string, string> | undefined;
};
