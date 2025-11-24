import { ExampleLayout } from "@/components/ExampleLayout";
import { ClearCacheButton } from "@/components/ClearCacheButton";
import { RevalidatePathButton } from "@/components/RevalidatePathButton";
import { InfoCard } from "@/components/InfoCard";
import { CodeBlock } from "@/components/CodeBlock";
import { unstable_cache } from "next/cache";
import { FuturamaCharacter } from "@/types/futurama";

async function fetchCharacter(id: string): Promise<FuturamaCharacter> {
  const response = await fetch(
    `https://api.sampleapis.com/futurama/characters/${id}`,
    {
      cache: "no-store",
    }
  );
  return response.json();
}

const getCachedCharacter = unstable_cache(
  async (id: string) => {
    return fetchCharacter(id);
  },
  ["futurama-character"],
  {
    tags: ["futurama", "characters"],
    revalidate: 60,
  }
);

export default async function UnstableCacheExample() {
  const timestamp = new Date().toISOString();
  const characterId = "6";

  let character: FuturamaCharacter;
  let fetchCharacterData: FuturamaCharacter;
  let fetchTimestamp: string;

  try {
    character = await getCachedCharacter(characterId);
    fetchTimestamp = new Date().toISOString();

    const fetchResponse = await fetch(
      `https://api.sampleapis.com/futurama/characters/${characterId}`,
      {
        next: {
          revalidate: 60,
          tags: ["futurama", "characters"],
        },
      }
    );
    fetchCharacterData = await fetchResponse.json();
  } catch (error) {
    console.error("Error fetching character data:", error);
    return (
      <ExampleLayout
        title="unstable_cache Example"
        description="Demonstrates persistent caching with unstable_cache"
      >
        <div className="text-red-600 dark:text-red-400">
          An error occurred during fetch. Please check your network connection.
        </div>
      </ExampleLayout>
    );
  }

  return (
    <ExampleLayout
      title="unstable_cache Example"
      description="This example demonstrates persistent caching with unstable_cache. Compare it with fetch caching to understand when to use each approach."
      actions={
        <div className="flex gap-2">
          <ClearCacheButton tag="futurama" label="Clear Tag Cache" />
          <RevalidatePathButton path="/examples/unstable-cache" />
        </div>
      }
    >
      <div className="space-y-6">
        <InfoCard title="How it works">
          <ul className="list-disc list-inside space-y-1">
            <li>
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                unstable_cache
              </code>{" "}
              caches the result of function calls across requests
            </li>
            <li>
              Unlike fetch caching, this works for any function, not just API
              calls
            </li>
            <li>Supports tags and revalidation just like fetch caching</li>
            <li>
              Cache key is based on the function arguments and the key array
            </li>
            <li>
              Perfect for caching database queries, computed values, or any
              expensive operations
            </li>
            <li>
              Both examples below use the same revalidation (60 seconds) and
              tags for comparison
            </li>
          </ul>
        </InfoCard>

        <div className="grid md:grid-cols-2 gap-6">
          <div className="space-y-4">
            <div className="bg-blue-50 dark:bg-blue-950/20 border border-blue-200 dark:border-blue-900 rounded-lg p-4">
              <h2 className="text-lg font-semibold text-blue-900 dark:text-blue-100 mb-2">
                unstable_cache Result
              </h2>
              <div className="space-y-2 text-sm">
                <div>
                  <span className="font-medium text-blue-800 dark:text-blue-200">
                    Name:
                  </span>{" "}
                  <span className="text-blue-900 dark:text-blue-100">
                    {character.name.first} {character.name.last}
                  </span>
                </div>
                {character.occupation && (
                  <div>
                    <span className="font-medium text-blue-800 dark:text-blue-200">
                      Occupation:
                    </span>{" "}
                    <span className="text-blue-900 dark:text-blue-100">
                      {character.occupation}
                    </span>
                  </div>
                )}
                <div>
                  <span className="font-medium text-blue-800 dark:text-blue-200">
                    Rendered at:
                  </span>{" "}
                  <span className="text-blue-900 dark:text-blue-100 font-mono text-xs">
                    {timestamp}
                  </span>
                </div>
              </div>
            </div>
          </div>

          <div className="space-y-4">
            <div className="bg-green-50 dark:bg-green-950/20 border border-green-200 dark:border-green-900 rounded-lg p-4">
              <h2 className="text-lg font-semibold text-green-900 dark:text-green-100 mb-2">
                fetch Result (for comparison)
              </h2>
              <div className="space-y-2 text-sm">
                <div>
                  <span className="font-medium text-green-800 dark:text-green-200">
                    Name:
                  </span>{" "}
                  <span className="text-green-900 dark:text-green-100">
                    {fetchCharacterData.name.first}{" "}
                    {fetchCharacterData.name.last}
                  </span>
                </div>
                {fetchCharacterData.occupation && (
                  <div>
                    <span className="font-medium text-green-800 dark:text-green-200">
                      Occupation:
                    </span>{" "}
                    <span className="text-green-900 dark:text-green-100">
                      {fetchCharacterData.occupation}
                    </span>
                  </div>
                )}
                <div>
                  <span className="font-medium text-green-800 dark:text-green-200">
                    Rendered at:
                  </span>{" "}
                  <span className="text-green-900 dark:text-green-100 font-mono text-xs">
                    {fetchTimestamp}
                  </span>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
          <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Cache Information
          </h2>
          <div className="space-y-2 text-sm">
            <div>
              <span className="font-medium text-gray-700 dark:text-gray-300">
                Revalidation:
              </span>{" "}
              <span className="text-gray-900 dark:text-gray-100">
                60 seconds
              </span>
            </div>
            <div>
              <span className="font-medium text-gray-700 dark:text-gray-300">
                Cache Tags:
              </span>{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-2 py-1 rounded text-xs ml-2">
                futurama
              </code>
              <code className="bg-gray-200 dark:bg-gray-800 px-2 py-1 rounded text-xs ml-1">
                characters
              </code>
            </div>
            <div>
              <span className="font-medium text-gray-700 dark:text-gray-300">
                Character ID:
              </span>{" "}
              <span className="text-gray-900 dark:text-gray-100">
                {characterId}
              </span>
            </div>
          </div>
        </div>

        <div className="space-y-4">
          <div>
            <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
              unstable_cache Code Example
            </h2>
            <CodeBlock>
              {`import { unstable_cache } from "next/cache";

async function fetchCharacter(id: string) {
  const response = await fetch(
    \`https://api.sampleapis.com/futurama/characters/\${id}\`,
    { cache: "no-store" }
  );
  return response.json();
}

const getCachedCharacter = unstable_cache(
  async (id: string) => {
    return fetchCharacter(id);
  },
  ["futurama-character"],
  {
    tags: ["futurama", "characters"],
    revalidate: 60,
  }
);

export default async function Page() {
  const character = await getCachedCharacter("6");
  return <div>{character.name.first}</div>;
}`}
            </CodeBlock>
          </div>

          <div>
            <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
              fetch Code Example (for comparison)
            </h2>
            <CodeBlock>
              {`export default async function Page() {
  const response = await fetch(
    "https://api.sampleapis.com/futurama/characters/6",
    {
      next: {
        revalidate: 60,
        tags: ["futurama", "characters"],
      },
    }
  );
  const character = await response.json();
  return <div>{character.name.first}</div>;
}`}
            </CodeBlock>
          </div>
        </div>

        <div className="bg-yellow-50 dark:bg-yellow-950/20 border border-yellow-200 dark:border-yellow-900 rounded-lg p-4">
          <h3 className="font-semibold text-yellow-900 dark:text-yellow-100 mb-2">
            Key Differences: unstable_cache vs fetch
          </h3>
          <div className="text-yellow-800 dark:text-yellow-200 text-sm space-y-2">
            <div>
              <strong>unstable_cache:</strong>
              <ul className="list-disc list-inside ml-2 mt-1">
                <li>Works with any function, not just fetch</li>
                <li>Cache key includes function arguments</li>
                <li>Perfect for database queries, computations, etc.</li>
                <li>More flexible for complex caching scenarios</li>
              </ul>
            </div>
            <div>
              <strong>fetch:</strong>
              <ul className="list-disc list-inside ml-2 mt-1">
                <li>Built-in caching for HTTP requests</li>
                <li>Simpler API for API calls</li>
                <li>Automatic request deduplication</li>
                <li>Best for external API calls</li>
              </ul>
            </div>
            <p className="mt-2">
              <strong>When to use unstable_cache:</strong> When you need to
              cache the result of database queries, complex computations, or any
              non-fetch operations. When you need more control over cache keys.
            </p>
          </div>
        </div>
      </div>
    </ExampleLayout>
  );
}
