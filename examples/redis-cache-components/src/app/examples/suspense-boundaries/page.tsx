import { Suspense } from "react";
import { ExampleLayout } from "@/components/ExampleLayout";
import { InfoCard } from "@/components/InfoCard";
import { CodeBlock } from "@/components/CodeBlock";
import { cacheLife } from "next/cache";

async function StaticContent() {
  return (
    <div className="bg-green-50 dark:bg-green-950/20 border border-green-200 dark:border-green-900 rounded-lg p-4">
      <div className="font-semibold text-green-900 dark:text-green-100 mb-2">
        Static Content (Prerendered)
      </div>
      <p className="text-green-800 dark:text-green-200 text-sm">
        This content is part of the static HTML shell. It&apos;s rendered at build time
        and sent immediately to the browser.
      </p>
    </div>
  );
}

async function CachedContent() {
  "use cache";
  cacheLife("hours");

  const timestamp = new Date().toISOString();
  const response = await fetch("https://api.sampleapis.com/futurama/characters/5", {
    cache: "no-store",
  });
  const character = await response.json();

  return (
    <div className="bg-blue-50 dark:bg-blue-950/20 border border-blue-200 dark:border-blue-900 rounded-lg p-4">
      <div className="font-semibold text-blue-900 dark:text-blue-100 mb-2">
        Cached Content (In Static Shell)
      </div>
      <p className="text-blue-800 dark:text-blue-200 text-sm mb-2">
        This content uses &apos;use cache&apos; and is included in the static shell.
      </p>
      <div className="mt-2">
        <div className="text-sm">
          <span className="font-medium">Character:</span> {character.name.first} {character.name.last}
        </div>
        <div className="text-xs font-mono mt-1">
          Rendered: {timestamp}
        </div>
      </div>
    </div>
  );
}

async function DynamicContent() {
  await new Promise((resolve) => setTimeout(resolve, 1000));

  const timestamp = new Date().toISOString();
  const response = await fetch("https://api.sampleapis.com/futurama/characters/6", {
    cache: "no-store",
  });
  const character = await response.json();

  return (
    <div className="bg-purple-50 dark:bg-purple-950/20 border border-purple-200 dark:border-purple-900 rounded-lg p-4">
      <div className="font-semibold text-purple-900 dark:text-purple-100 mb-2">
        Dynamic Content (Streamed)
      </div>
      <p className="text-purple-800 dark:text-purple-200 text-sm mb-2">
        This content is wrapped in Suspense and streams in at request time.
        Notice the 1-second delay before it appears.
      </p>
      <div className="mt-2">
        <div className="text-sm">
          <span className="font-medium">Character:</span> {character.name.first} {character.name.last}
        </div>
        <div className="text-xs font-mono mt-1">
          Rendered: {timestamp}
        </div>
      </div>
    </div>
  );
}

export default async function SuspenseBoundariesExample() {
  return (
    <ExampleLayout
      title="Suspense Boundaries & Partial Prerendering"
      description="This example demonstrates Partial Prerendering (PPR) with Suspense boundaries. Static content is sent immediately, while dynamic content streams in."
    >
      <div className="space-y-6">
        <InfoCard title="How it works">
          <ul className="list-disc list-inside space-y-1">
            <li>
              <strong>Static content</strong> is prerendered and included in the static HTML shell
            </li>
            <li>
              <strong>Cached content</strong> (with &apos;use cache&apos;) is also included in the static shell
            </li>
            <li>
              <strong>Dynamic content</strong> wrapped in Suspense streams in at request time
            </li>
            <li>
              The fallback UI is shown while dynamic content loads
            </li>
            <li>
              This creates a fast initial page load with fresh data streaming in
            </li>
          </ul>
        </InfoCard>

        <div className="space-y-4">
          <StaticContent />
          <CachedContent />
          <Suspense
            fallback={
              <div className="bg-gray-50 dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-lg p-4">
                <div className="font-semibold text-gray-700 dark:text-gray-300 mb-2">
                  Loading dynamic content...
                </div>
                <div className="text-sm text-gray-600 dark:text-gray-400">
                  This fallback is part of the static shell and shows immediately.
                </div>
              </div>
            }
          >
            <DynamicContent />
          </Suspense>
        </div>

        <div>
          <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Code Example
          </h2>
          <CodeBlock>
{`import { Suspense } from "react";
import { cacheLife } from "next/cache";

async function StaticContent() {
  return <div>Static content in shell</div>;
}

async function CachedContent() {
  "use cache";
  cacheLife("hours");
  
  const data = await fetch("https://api.example.com/data");
  return <div>{data}</div>;
}

async function DynamicContent() {
  const data = await fetch("https://api.example.com/dynamic");
  return <div>{data}</div>;
}

export default async function Page() {
  return (
    <>
      <StaticContent />
      <CachedContent />
      <Suspense fallback={<div>Loading...</div>}>
        <DynamicContent />
      </Suspense>
    </>
  );
}`}
          </CodeBlock>
        </div>
      </div>
    </ExampleLayout>
  );
}

