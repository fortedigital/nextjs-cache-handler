import { ExampleLayout } from "@/components/ExampleLayout";
import { InfoCard } from "@/components/InfoCard";
import { CodeBlock } from "@/components/CodeBlock";
import { cacheLife } from "next/cache";

async function MaxCache() {
  "use cache";
  cacheLife("max");

  const timestamp = new Date().toISOString();
  return (
    <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
      <div className="font-medium text-gray-700 dark:text-gray-300 mb-1">
        cacheLife("max")
      </div>
      <div className="text-sm text-gray-600 dark:text-gray-400 mb-2">
        Cache never expires (or expires at deployment)
      </div>
      <div className="font-mono text-sm text-gray-900 dark:text-gray-100">
        {timestamp}
      </div>
    </div>
  );
}

async function HoursCache() {
  "use cache";
  cacheLife("hours");

  const timestamp = new Date().toISOString();
  return (
    <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
      <div className="font-medium text-gray-700 dark:text-gray-300 mb-1">
        cacheLife("hours")
      </div>
      <div className="text-sm text-gray-600 dark:text-gray-400 mb-2">
        Cache expires after a few hours
      </div>
      <div className="font-mono text-sm text-gray-900 dark:text-gray-100">
        {timestamp}
      </div>
    </div>
  );
}

async function DaysCache() {
  "use cache";
  cacheLife("days");

  const timestamp = new Date().toISOString();
  return (
    <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
      <div className="font-medium text-gray-700 dark:text-gray-300 mb-1">
        cacheLife("days")
      </div>
      <div className="text-sm text-gray-600 dark:text-gray-400 mb-2">
        Cache expires after a few days
      </div>
      <div className="font-mono text-sm text-gray-900 dark:text-gray-100">
        {timestamp}
      </div>
    </div>
  );
}

export default async function CacheLifeExample() {
  return (
    <ExampleLayout
      title="cacheLife"
      description="This example demonstrates different cacheLife profiles. cacheLife replaces the traditional 'revalidate' route segment config in Cache Components."
    >
      <div className="space-y-6">
        <InfoCard title="How it works">
          <ul className="list-disc list-inside space-y-1">
            <li>
              <code className="bg-blue-100 dark:bg-blue-900 px-1 rounded">cacheLife</code> controls
              how long cached data remains valid
            </li>
            <li>
              Three profiles: <code className="bg-blue-100 dark:bg-blue-900 px-1 rounded">"max"</code>,{" "}
              <code className="bg-blue-100 dark:bg-blue-900 px-1 rounded">"hours"</code>, and{" "}
              <code className="bg-blue-100 dark:bg-blue-900 px-1 rounded">"days"</code>
            </li>
            <li>
              Replaces route segment config like <code className="bg-blue-100 dark:bg-blue-900 px-1 rounded">export const revalidate = 3600</code>
            </li>
            <li>
              Profiles are primarily designed for Vercel's infrastructure
            </li>
            <li>
              Custom cache handlers may not fully differentiate between profiles
            </li>
          </ul>
        </InfoCard>

        <div>
          <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">
            Different cacheLife Profiles
          </h2>
          <div className="grid gap-4 md:grid-cols-3">
            <MaxCache />
            <HoursCache />
            <DaysCache />
          </div>
        </div>

        <div>
          <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Code Example
          </h2>
          <CodeBlock>
{`import { cacheLife } from "next/cache";

async function CachedComponent() {
  "use cache";
  cacheLife("hours");

  const data = await fetch("https://api.example.com/data");
  return <div>{data}</div>;
}`}
          </CodeBlock>
        </div>

        <div>
          <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Migration from Traditional API
          </h2>
          <CodeBlock>
{`// Before (Traditional Cache API)
export const revalidate = 3600; // 1 hour

export default async function Page() {
  const data = await fetch("https://api.example.com/data");
  return <div>{data}</div>;
}

// After (Cache Components)
import { cacheLife } from "next/cache";

export default async function Page() {
  "use cache";
  cacheLife("hours");
  
  const data = await fetch("https://api.example.com/data");
  return <div>{data}</div>;
}`}
          </CodeBlock>
        </div>
      </div>
    </ExampleLayout>
  );
}

