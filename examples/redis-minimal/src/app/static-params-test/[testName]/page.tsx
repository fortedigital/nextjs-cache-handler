import React from "react";

export const dynamicParams = true;

export const revalidate = 5;

export default async function TestPage({
  params,
}: {
  params: Promise<{ testName: string }>;
}) {
  const { testName } = await params;
  return <div>{testName}</div>;
}

export async function generateStaticParams() {
  return [{ testName: "cache" }];
}
