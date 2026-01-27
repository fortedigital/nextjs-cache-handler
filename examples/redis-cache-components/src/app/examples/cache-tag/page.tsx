import { ExampleLayout } from "@/components/ExampleLayout";
import { InfoCard } from "@/components/InfoCard";
import { CodeBlock } from "@/components/CodeBlock";
import { cacheLife, cacheTag } from "next/cache";
import { revalidateTag } from "next/cache";

async function TaggedCache() {
  "use cache";
  cacheLife("hours");
  cacheTag("futurama");

  const timestamp = new Date().toISOString();
  
  const response = await fetch("https://api.sampleapis.com/futurama/characters/4", {
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
      <div className="mt-2">
        <span className="font-medium text-gray-700 dark:text-gray-300">
          Cache Tag:
        </span>{" "}
        <code className="bg-gray-200 dark:bg-gray-800 px-2 py-1 rounded text-sm">
          futurama
        </code>
      </div>
    </div>
  );
}

async function RevalidateButton() {
   async function handleRevalidate() {
    "use server";
     revalidateTag("futurama", "max");
  }

  return (
    <form action={handleRevalidate}>
      <button
        type="submit"
        className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-md transition-colors"
      >
        Revalidate Tag
      </button>
    </form>
  );
}

export default async function CacheTagExample() {
  return (
    <ExampleLayout
      title="cacheTag"
      description="This example demonstrates cache tagging with cacheTag. Tags allow you to selectively invalidate cached data when it changes."
    >
      <div className="space-y-6">
        <InfoCard title="How it works">
          <ul className="list-disc list-inside space-y-1">
            <li>
              Use <code className="bg-blue-100 dark:bg-blue-900 px-1 rounded">cacheTag</code> to
              associate cached data with a tag
            </li>
            <li>
              Tags allow selective cache invalidation using{" "}
              <code className="bg-blue-100 dark:bg-blue-900 px-1 rounded">revalidateTag</code>
            </li>
            <li>
              Multiple components can share the same tag
            </li>
            <li>
              When you revalidate a tag, all cached data with that tag is invalidated
            </li>
            <li>
              Perfect for content management systems where related data needs to be invalidated together
            </li>
          </ul>
        </InfoCard>

        <div>
          <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Tagged Cached Data
          </h2>
          <TaggedCache />
        </div>

        <div>
          <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Revalidate Cache
          </h2>
          <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
            <p className="text-sm text-gray-600 dark:text-gray-400 mb-3">
              Click the button below to revalidate the &quot;futurama&quot; tag. After revalidation,
              refresh the page to see fresh data.
            </p>
            <RevalidateButton />
          </div>
        </div>

        <div>
          <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Code Example
          </h2>
          <CodeBlock>
{`import { cacheLife, cacheTag } from "next/cache";
import { revalidateTag } from "next/cache";

async function TaggedComponent() {
  "use cache";
  cacheLife("hours");
  cacheTag("futurama");

  const data = await fetch("https://api.example.com/data");
  return <div>{data}</div>;
}

function RevalidateAction() {
  "use server";
   revalidateTag("futurama", "max");
}`}
          </CodeBlock>
        </div>
      </div>
    </ExampleLayout>
  );
}

