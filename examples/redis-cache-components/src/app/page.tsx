import Link from "next/link";
import { ExampleLayout } from "@/components/ExampleLayout";

const examples = [
  {
    href: "/examples/use-cache",
    title: "use cache Directive",
    description:
      "Demonstrates the basic 'use cache' directive for caching component data. This is the foundation of Cache Components - it allows you to cache data at the component or function level.",
    features: [
      "Component-level caching with 'use cache'",
      "Automatic inclusion in static shell",
      "Prerendering support",
      "Perfect for data that doesn't change frequently",
    ],
  },
  {
    href: "/examples/use-cache-remote",
    title: "use cache: remote",
    description:
      "Shows how to use 'use cache: remote' directive with Redis for remote caching. This requires configuring cacheHandlers in next.config.",
    features: [
      "Remote caching with Redis",
      "cacheHandlers configuration",
      "Shared cache across instances",
      "Perfect for distributed deployments",
    ],
  },
  {
    href: "/examples/cache-life",
    title: "cacheLife",
    description:
      "Demonstrates cache expiration using cacheLife function. This replaces the traditional 'revalidate' route segment config in Cache Components.",
    features: [
      "Cache expiration with cacheLife",
      "Different profiles: 'max', 'hours', 'days'",
      "Replaces route segment revalidate",
      "Component-level cache control",
    ],
  },
  {
    href: "/examples/cache-tag",
    title: "cacheTag",
    description:
      "Shows how to use cacheTag for cache invalidation. Tags allow you to selectively invalidate cached data when it changes.",
    features: [
      "Cache tagging with cacheTag",
      "Selective cache invalidation",
      "Tag-based revalidation",
      "Perfect for content management",
    ],
  },
  {
    href: "/examples/suspense-boundaries",
    title: "Suspense Boundaries",
    description:
      "Demonstrates Partial Prerendering with Suspense boundaries. Shows how static and dynamic content can coexist in a single route.",
    features: [
      "Partial Prerendering (PPR)",
      "Static shell with streaming content",
      "Suspense boundaries for dynamic data",
      "Fast initial page loads",
    ],
  },
];

export default async function Home() {
  return (
    <ExampleLayout
      title="Next.js Cache Components Examples"
      description="Explore Next.js 16 Cache Components features with Redis cache handler"
    >
      <div className="space-y-6">
        <div className="bg-yellow-50 dark:bg-yellow-950/20 border border-yellow-200 dark:border-yellow-900 rounded-lg p-4 mb-6">
          <h2 className="font-semibold text-yellow-900 dark:text-yellow-100 mb-2">
            Important: Cache Components vs Traditional Cache API
          </h2>
          <p className="text-yellow-800 dark:text-yellow-200 text-sm">
            Cache Components is a different caching model from the traditional Next.js cache API.
            It requires <code className="bg-yellow-100 dark:bg-yellow-900 px-1 rounded">cacheComponents: true</code> in
            next.config and uses different APIs like <code className="bg-yellow-100 dark:bg-yellow-900 px-1 rounded">'use cache'</code>,{" "}
            <code className="bg-yellow-100 dark:bg-yellow-900 px-1 rounded">cacheLife</code>, and{" "}
            <code className="bg-yellow-100 dark:bg-yellow-900 px-1 rounded">cacheTag</code>.
            Many traditional cache APIs (like route segment configs) don't work the same way with Cache Components.
          </p>
        </div>

        <div className="grid gap-6 md:grid-cols-2">
          {examples.map((example) => (
            <Link
              key={example.href}
              href={example.href}
              className="block p-6 bg-gray-50 dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-800 hover:border-blue-500 dark:hover:border-blue-600 transition-colors"
            >
              <h2 className="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-2">
                {example.title}
              </h2>
              <p className="text-gray-600 dark:text-gray-400 mb-4">
                {example.description}
              </p>
              <ul className="space-y-1">
                {example.features.map((feature, idx) => (
                  <li
                    key={idx}
                    className="text-sm text-gray-500 dark:text-gray-500 flex items-start"
                  >
                    <span className="mr-2">•</span>
                    <span>{feature}</span>
                  </li>
                ))}
              </ul>
            </Link>
          ))}
        </div>
        <div className="mt-8 p-4 bg-gray-100 dark:bg-gray-800 rounded-lg">
          <p className="text-sm text-gray-700 dark:text-gray-300">
            <strong>Note:</strong> All examples use Redis as the remote cache handler for Cache Components.
            Make sure Redis is running and configured in your environment variables. Cache Components
            requires <code className="bg-gray-200 dark:bg-gray-700 px-1 rounded">cacheComponents: true</code> and
            uses <code className="bg-gray-200 dark:bg-gray-700 px-1 rounded">cacheHandlers.remote</code> configuration.
          </p>
        </div>
      </div>
    </ExampleLayout>
  );
}

