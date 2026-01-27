import { ExampleLayout } from "@/components/ExampleLayout";
import { InfoCard } from "@/components/InfoCard";
import { CodeBlock } from "@/components/CodeBlock";
import { cacheLife } from "next/cache";

async function CachedData() {
  "use cache";
  cacheLife("hours");

  const timestamp = new Date().toISOString();
  
  const response = await fetch("https://api.sampleapis.com/futurama/characters/2", {
    cache: "no-store",
  });
  const character = await response.json();

  return (
    <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4 space-y-2">
      <div>
        <span className="font-medium text-gray-700 dark:text-gray-300">
          Character:
        </span>{" "}
        <span className="text-gray-900 dark:text-gray-100">
          {character.name.first} {character.name.last}
        </span>
      </div>
      <div>
        <span className="font-medium text-gray-700 dark:text-gray-300">
          Rendered at:
        </span>{" "}
        <span className="text-gray-900 dark:text-gray-100 font-mono text-sm">
          {timestamp}
        </span>
      </div>
    </div>
  );
}

export default async function UseCacheExample() {
  return (
    <ExampleLayout
      title="use cache Directive"
      description="This example demonstrates the basic &apos;use cache&apos; directive. Components marked with &apos;use cache&apos; are automatically included in the static shell during prerendering."
    >
      <div className="space-y-6">
        <InfoCard title="How it works">
          <ul className="list-disc list-inside space-y-1">
            <li>
              The <code className="bg-blue-100 dark:bg-blue-900 px-1 rounded">&apos;use cache&apos;</code> directive
              marks a component or function for caching
            </li>
            <li>
              Cached components are included in the static HTML shell during prerendering
            </li>
            <li>
              Use <code className="bg-blue-100 dark:bg-blue-900 px-1 rounded">cacheLife</code> to control
              cache expiration (replaces traditional revalidate)
            </li>
            <li>
              The timestamp shows when the component was first rendered and cached
            </li>
            <li>
              Refresh the page - the timestamp should remain the same until cache expires
            </li>
          </ul>
        </InfoCard>

        <div>
          <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Cached Data
          </h2>
          <CachedData />
        </div>

        <div>
          <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Code Example
          </h2>
          <CodeBlock>
{`async function CachedData() {
  "use cache";
  cacheLife("hours");

  const response = await fetch(
    "https://api.sampleapis.com/futurama/characters/2",
    { cache: "no-store" }
  );
  const character = await response.json();

  return <div>{character.name.first}</div>;
}`}
          </CodeBlock>
        </div>
      </div>
    </ExampleLayout>
  );
}

