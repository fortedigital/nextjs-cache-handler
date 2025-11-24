import { ExampleLayout } from "@/components/ExampleLayout";
import { InfoCard } from "@/components/InfoCard";
import { CodeBlock } from "@/components/CodeBlock";
import { RevalidatePathButton } from "@/components/RevalidatePathButton";
import { FuturamaCharacter } from "@/types/futurama";

export default async function TimeBasedRevalidationExample() {
  let name: string;
  let character: FuturamaCharacter;
  const timestamp = new Date().toISOString();

  try {
    const characterResponse = await fetch(
      "https://api.sampleapis.com/futurama/characters/4",
      {
        next: {
          revalidate: 30,
        },
      }
    );
    character = await characterResponse.json();
    name = character.name.first;
  } catch (error) {
    console.error("Error fetching character data:", error);
    return (
      <ExampleLayout
        title="Time-based Revalidation Example"
        description="Demonstrates fetch with time-based revalidation"
      >
        <div className="text-red-600 dark:text-red-400">
          An error occurred during fetch. Please check your network connection.
        </div>
      </ExampleLayout>
    );
  }

  return (
    <ExampleLayout
      title="Time-based Revalidation Example"
      description="This example demonstrates fetch with time-based revalidation. Data is cached and automatically revalidated after a specified time period, balancing freshness with performance."
      actions={<RevalidatePathButton path="/examples/time-based-revalidation" />}
    >
      <div className="space-y-6">
        <InfoCard title="How it works">
          <ul className="list-disc list-inside space-y-1">
            <li>
              Using{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                next: &#123; revalidate: 30 &#125;
              </code>{" "}
              caches data for 30 seconds
            </li>
            <li>
              During the revalidation period, cached data is served instantly
              (the timestamp stays the same)
            </li>
            <li>
              After 30 seconds, the next request will trigger a background
              revalidation to fetch fresh data
            </li>
            <li>
              This provides a good balance: fast responses with automatic
              freshness updates
            </li>
            <li>
              Perfect for data that changes periodically but doesn&apos;t need
              to be real-time
            </li>
            <li>
              Try refreshing the page - if less than 30 seconds have passed,
              you&apos;ll see the same timestamp
            </li>
            <li>
              Click &quot;Refresh Cache&quot; to manually revalidate before the
              30-second period expires
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
                  Revalidation Period:
                </span>{" "}
                <span className="text-gray-900 dark:text-gray-100">
                  30 seconds
                </span>
              </div>
              <div className="mt-2">
                <span className="font-medium text-gray-700 dark:text-gray-300">
                  Cache Strategy:
                </span>{" "}
                <code className="bg-gray-200 dark:bg-gray-800 px-2 py-1 rounded text-sm">
                  Time-based ISR
                </code>
              </div>
              <div className="mt-2 text-sm text-gray-600 dark:text-gray-400">
                <p>
                  The timestamp will remain the same for 30 seconds. After that,
                  the next request will fetch fresh data.
                </p>
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
  "https://api.sampleapis.com/futurama/characters/4",
  {
    next: {
      revalidate: 30, // Revalidate every 30 seconds
    },
  }
);`}
          </CodeBlock>
        </div>
      </div>
    </ExampleLayout>
  );
}

