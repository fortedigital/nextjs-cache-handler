import { ExampleLayout } from "@/components/ExampleLayout";
import { InfoCard } from "@/components/InfoCard";
import { CodeBlock } from "@/components/CodeBlock";
import { RevalidatePathButton } from "@/components/RevalidatePathButton";
import { FuturamaCharacter } from "@/types/futurama";

export default async function DefaultCacheExample() {
  let name: string;
  let character: FuturamaCharacter;
  const timestamp = new Date().toISOString();

  try {
    const characterResponse = await fetch(
      "https://api.sampleapis.com/futurama/characters/2",
      {
        cache: "force-cache",
      }
    );
    character = await characterResponse.json();
    name = character.name.first;
  } catch (error) {
    console.error("Error fetching character data:", error);
    return (
      <ExampleLayout
        title="Default Caching Example"
        description="Demonstrates default fetch caching behavior with force-cache"
      >
        <div className="text-red-600 dark:text-red-400">
          An error occurred during fetch. Please check your network connection.
        </div>
      </ExampleLayout>
    );
  }

  return (
    <ExampleLayout
      title="Default Caching (force-cache) Example"
      description="This example demonstrates the default fetch caching behavior. When no cache options are specified, Next.js uses 'force-cache' which caches data indefinitely until manually invalidated."
      actions={<RevalidatePathButton path="/examples/default-cache" />}
    >
      <div className="space-y-6">
        <InfoCard title="How it works">
          <ul className="list-disc list-inside space-y-1">
            <li>
              By default, Next.js uses{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                cache: &quot;force-cache&quot;
              </code>{" "}
              for fetch requests
            </li>
            <li>
              Data is cached indefinitely until the cache is manually cleared or
              the build is redeployed
            </li>
            <li>
              This is the most aggressive caching strategy - perfect for static
              or rarely-changing data
            </li>
            <li>
              The timestamp shows when this page was first rendered (it will
              remain the same on subsequent requests)
            </li>
            <li>
              Click &quot;Refresh Cache&quot; to manually revalidate the page
              cache and see fresh data
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
                  Cache Strategy:
                </span>{" "}
                <code className="bg-gray-200 dark:bg-gray-800 px-2 py-1 rounded text-sm">
                  force-cache
                </code>
              </div>
              <div className="mt-2">
                <span className="font-medium text-gray-700 dark:text-gray-300">
                  Revalidation:
                </span>{" "}
                <span className="text-gray-900 dark:text-gray-100">
                  Never (indefinite)
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
  "https://api.sampleapis.com/futurama/characters/2",
  {
    cache: "force-cache",
  }
);

// Or simply omit cache option (default behavior):
const response = await fetch(
  "https://api.sampleapis.com/futurama/characters/2"
);`}
          </CodeBlock>
        </div>
      </div>
    </ExampleLayout>
  );
}
