import { Suspense } from "react";
import { Example, Skeleton } from "./Example";

export default function Page({
  searchParams,
}: {
  searchParams: Promise<{ characterId: string }>;
}) {
  return (
    <section>
      <h1>This will be prerendered</h1>
      <Suspense fallback={<Skeleton />}>
        <Example searchParams={searchParams} />
      </Suspense>
    </section>
  );
}

export const experimental_ppr = true;
