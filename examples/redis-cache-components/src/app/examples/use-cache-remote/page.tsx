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
        Check your console logs for &quot;[Cache Handler]&quot; messages to verify the handler is being called.
      </div>
    </div>
  );
}

export default async function UseCacheRemoteExample() {
  return (
    <ExampleLayout
      title="use cache: remote"
      description="This example demonstrates remote caching with Redis. The 'use cache: remote' directive uses the cacheHandlers.remote handler configured in next.config. Check console logs to verify the handler is being called."
    >
      <div className="space-y-6">
        <InfoCard title="Important: This uses the remote handler">
          <ul className="list-disc list-inside space-y-1">
            <li>
              The <code className="bg-blue-100 dark:bg-blue-900 px-1 rounded">&apos;use cache: remote&apos;</code> directive
              uses the <code className="bg-blue-100 dark:bg-blue-900 px-1 rounded">cacheHandlers.remote</code> handler
            </li>
            <li>
              This is different from <code className="bg-blue-100 dark:bg-blue-900 px-1 rounded">&apos;use cache&apos;</code> which
              uses the default in-memory cache
            </li>
            <li>
              Check your console for <code className="bg-blue-100 dark:bg-blue-900 px-1 rounded">[Cache Handler]</code> logs
              to verify the handler is being called
            </li>
            <li>
              If you don&apos;t see the logs, the handler might not be configured correctly
            </li>
            <li>
              The cache is shared across all instances of your application
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

        <div className="bg-yellow-50 dark:bg-yellow-950/20 border border-yellow-200 dark:border-yellow-900 rounded-lg p-4">
          <h3 className="font-semibold text-yellow-900 dark:text-yellow-100 mb-2">
            Troubleshooting
          </h3>
          <ul className="text-sm text-yellow-800 dark:text-yellow-200 space-y-1 list-disc list-inside">
            <li>Make sure you&apos;re using <code className="bg-yellow-100 dark:bg-yellow-900 px-1 rounded">&apos;use cache: remote&apos;</code> (not just <code className="bg-yellow-100 dark:bg-yellow-900 px-1 rounded">&apos;use cache&apos;</code>)</li>
            <li>Check console logs for <code className="bg-yellow-100 dark:bg-yellow-900 px-1 rounded">[Cache Handler]</code> messages</li>
            <li>Verify Redis is running and REDIS_URL is set correctly</li>
            <li>Ensure cacheHandlers.remote is configured in next.config.ts</li>
          </ul>
        </div>
      </div>
    </ExampleLayout>
  );
}

