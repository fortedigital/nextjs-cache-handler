import { ExampleLayout } from "@/components/ExampleLayout";
import { ClearCacheButton } from "@/components/ClearCacheButton";
import { InfoCard } from "@/components/InfoCard";
import { CodeBlock } from "@/components/CodeBlock";
import { FuturamaCharacter } from "@/types/futurama";

export default async function FetchTagsExample() {
  let name: string;
  let character: FuturamaCharacter;
  const timestamp = new Date().toISOString();

  try {
    const characterResponse = await fetch(
      "https://api.sampleapis.com/futurama/characters/1",
      {
        next: {
          revalidate: 86400,
          tags: ["futurama"],
        },
      }
    );
    character = await characterResponse.json();
    name = character.name.first;
  } catch (error) {
    console.error("Error fetching character data:", error);
    return (
      <ExampleLayout
        title="Fetch with Tags Example"
        description="Demonstrates fetch caching with tags and time-based revalidation"
      >
        <div className="text-red-600 dark:text-red-400">
          An error occurred during fetch. Please check your network connection.
        </div>
      </ExampleLayout>
    );
  }

  return (
    <ExampleLayout
      title="Fetch with Tags Example"
      description="This example demonstrates fetch caching with tags and time-based revalidation. The data is cached for 24 hours and can be invalidated using the 'futurama' tag."
      actions={<ClearCacheButton tag="futurama" />}
    >
      <div className="space-y-6">
        <InfoCard title="How it works">
          <ul className="list-disc list-inside space-y-1">
            <li>
              Data is fetched with a 24-hour revalidation period (
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                revalidate: 86400
              </code>
              )
            </li>
            <li>
              Cache is tagged with{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                &quot;futurama&quot;
              </code>{" "}
              for selective invalidation
            </li>
            <li>
              Click &quot;Clear Cache&quot; to invalidate the cache and see
              fresh data on reload
            </li>
            <li>
              The timestamp shows when this page was rendered (cached pages will
              show the same timestamp)
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
              <div className="mt-2">
                <span className="font-medium text-gray-700 dark:text-gray-300">
                  Revalidation:
                </span>{" "}
                <span className="text-gray-900 dark:text-gray-100">
                  24 hours
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
  "https://api.sampleapis.com/futurama/characters/1",
  {
    next: {
      revalidate: 86400,
      tags: ["futurama"],
    },
  }
);`}
          </CodeBlock>
        </div>
      </div>
    </ExampleLayout>
  );
}

