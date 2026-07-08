import { ExampleLayout } from "@/components/ExampleLayout";
import { InfoCard } from "@/components/InfoCard";
import { CodeBlock } from "@/components/CodeBlock";
import { RevalidatePathButton } from "@/components/RevalidatePathButton";
import { FuturamaCharacter } from "@/types/futurama";

export const dynamic = "force-dynamic";

export default async function NoStoreExample() {
  let name: string;
  let character: FuturamaCharacter;
  const timestamp = new Date().toISOString();

  try {
    const characterResponse = await fetch(
      "https://api.sampleapis.com/futurama/characters/3",
      {
        cache: "no-store",
      }
    );
    character = await characterResponse.json();
    name = character.name.first;
  } catch (error) {
    console.error("Error fetching character data:", error);
    return (
      <ExampleLayout
        title="No Store Example"
        description="Demonstrates fetch with no-store cache option"
      >
        <div className="text-red-600 dark:text-red-400">
          An error occurred during fetch. Please check your network connection.
        </div>
      </ExampleLayout>
    );
  }

  return (
    <ExampleLayout
      title="No Store (Always Fresh) Example"
      description="This example demonstrates fetch with 'no-store' option, which always fetches fresh data and never caches the response. Perfect for real-time or user-specific data."
      actions={
        <RevalidatePathButton path="/examples/no-store" label="Refresh Page" />
      }
    >
      <div className="space-y-6">
        <InfoCard title="How it works">
          <ul className="list-disc list-inside space-y-1">
            <li>
              Using{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                cache: &quot;no-store&quot;
              </code>{" "}
              tells Next.js to never cache the response
            </li>
            <li>
              Every request will fetch fresh data from the API, ensuring you
              always get the latest information
            </li>
            <li>
              The timestamp will change on every page load, showing that data is
              fetched fresh each time
            </li>
            <li>
              Use this for: real-time data, user-specific content, frequently
              changing data, or when you need guaranteed freshness
            </li>
            <li>
              Trade-off: No caching means every request hits the API, which can
              increase latency and API usage
            </li>
          </ul>
        </InfoCard>

        <div className="space-y-4">
          <div>
            <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
              Character Data
            </h2>
            <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4 space-y-2">
              <div>
                <span className="font-medium text-gray-700 dark:text-gray-300">
                  Name:
                </span>{" "}
                <span className="text-gray-900 dark:text-gray-100">{name}</span>
              </div>
              {character.name.middle && (
                <div>
                  <span className="font-medium text-gray-700 dark:text-gray-300">
                    Middle Name:
                  </span>{" "}
                  <span className="text-gray-900 dark:text-gray-100">
                    {character.name.middle}
                  </span>
                </div>
              )}
              <div>
                <span className="font-medium text-gray-700 dark:text-gray-300">
                  Last Name:
                </span>{" "}
                <span className="text-gray-900 dark:text-gray-100">
                  {character.name.last}
                </span>
              </div>
              {character.occupation && (
                <div>
                  <span className="font-medium text-gray-700 dark:text-gray-300">
                    Occupation:
                  </span>{" "}
                  <span className="text-gray-900 dark:text-gray-100">
                    {character.occupation}
                  </span>
                </div>
              )}
            </div>
          </div>

          <div>
            <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
              Cache Information
            </h2>
            <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
              <div>
                <span className="font-medium text-gray-700 dark:text-gray-300">
                  Rendered at:
                </span>{" "}
                <span
                  data-testid="build-timestamp"
                  className="text-gray-900 dark:text-gray-100 font-mono text-sm"
                >
                  {timestamp}
                </span>
                <span className="ml-2 text-sm text-orange-600 dark:text-orange-400">
                  (changes on every request)
                </span>
              </div>
              <div className="mt-2">
                <span className="font-medium text-gray-700 dark:text-gray-300">
                  Cache Strategy:
                </span>{" "}
                <code className="bg-gray-200 dark:bg-gray-800 px-2 py-1 rounded text-sm">
                  no-store
                </code>
              </div>
              <div className="mt-2">
                <span className="font-medium text-gray-700 dark:text-gray-300">
                  Caching:
                </span>{" "}
                <span className="text-gray-900 dark:text-gray-100">
                  Disabled (always fresh)
                </span>
              </div>
            </div>
          </div>
        </div>

        <div>
          <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Code Example
          </h2>
          <CodeBlock>
            {`const response = await fetch(
  "https://api.sampleapis.com/futurama/characters/3",
  {
    cache: "no-store",
  }
);`}
          </CodeBlock>
        </div>
      </div>
    </ExampleLayout>
  );
}
