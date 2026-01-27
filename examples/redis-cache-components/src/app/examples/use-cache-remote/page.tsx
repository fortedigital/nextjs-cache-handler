import { ExampleLayout } from "@/components/ExampleLayout";
import { InfoCard } from "@/components/InfoCard";
import { CodeBlock } from "@/components/CodeBlock";

async function RemoteCachedData() {
  "use cache: remote";

  const timestamp = new Date().toISOString();
  
  const response = await fetch("https://api.sampleapis.com/futurama/characters/3", {
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
      <div className="mt-2 text-sm text-gray-600 dark:text-gray-400">
        This data is cached remotely in Redis using the cacheHandlers.remote configuration.
      </div>
    </div>
  );
}

export default async function UseCacheRemoteExample() {
  return (
    <ExampleLayout
      title="use cache: remote"
      description="This example demonstrates remote caching with Redis. The 'use cache: remote' directive uses the cacheHandlers.remote handler configured in next.config."
    >
      <div className="space-y-6">
        <InfoCard title="How it works">
          <ul className="list-disc list-inside space-y-1">
            <li>
              The <code className="bg-blue-100 dark:bg-blue-900 px-1 rounded">'use cache: remote'</code> directive
              uses the remote cache handler (Redis)
            </li>
            <li>
              Requires <code className="bg-blue-100 dark:bg-blue-900 px-1 rounded">cacheHandlers.remote</code> configuration
              in next.config.ts
            </li>
            <li>
              Cache is shared across all instances of your application
            </li>
            <li>
              Perfect for distributed deployments where multiple instances need shared cache
            </li>
            <li>
              The cache handler implements get, set, and delete methods for Redis
            </li>
          </ul>
        </InfoCard>

        <div>
          <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Remote Cached Data
          </h2>
          <RemoteCachedData />
        </div>

        <div>
          <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Code Example
          </h2>
          <CodeBlock>
{`async function RemoteCachedData() {
  "use cache: remote";

  const response = await fetch(
    "https://api.sampleapis.com/futurama/characters/3",
    { cache: "no-store" }
  );
  const character = await response.json();

  return <div>{character.name.first}</div>;
}`}
          </CodeBlock>
        </div>

        <div>
          <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Configuration
          </h2>
          <CodeBlock>
{`// next.config.ts
const nextConfig = {
  cacheComponents: true,
  cacheHandlers: {
    remote: require.resolve("./cache-handler.mjs"),
  },
};`}
          </CodeBlock>
        </div>
      </div>
    </ExampleLayout>
  );
}

