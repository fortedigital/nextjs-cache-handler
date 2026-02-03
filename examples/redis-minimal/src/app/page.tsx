import Link from "next/link";
import { ExampleLayout } from "@/components/ExampleLayout";

const examples = [
  {
    href: "/examples/default-cache",
    title: "Default Cache (force-cache)",
    description:
      "Demonstrates the default fetch caching behavior. Next.js uses 'force-cache' by default, which caches data indefinitely until manually invalidated.",
    features: [
      "Default caching behavior",
      "Indefinite cache duration",
      "Perfect for static data",
    ],
  },
  {
    href: "/examples/no-store",
    title: "No Store (Always Fresh)",
    description:
      "Shows fetch with 'no-store' option, which always fetches fresh data and never caches the response. Perfect for real-time or user-specific data.",
    features: [
      "Never caches responses",
      "Always fetches fresh data",
      "Real-time data updates",
    ],
  },
  {
    href: "/examples/time-based-revalidation",
    title: "Time-based Revalidation",
    description:
      "Shows fetch with time-based revalidation. Data is cached and automatically revalidated after a specified time period, balancing freshness with performance.",
    features: [
      "Automatic revalidation",
      "Configurable time period (30 seconds)",
      "Balances freshness and performance",
    ],
  },
  {
    href: "/examples/fetch-tags",
    title: "Fetch with Tags",
    description:
      "Demonstrates fetch caching with tags and time-based revalidation. Shows how to use cache tags for selective cache invalidation.",
    features: [
      "Time-based revalidation (24 hours)",
      "Cache tags for selective invalidation",
      "Clear cache button to test tag revalidation",
    ],
  },
  {
    href: "/examples/unstable-cache",
    title: "unstable_cache",
    description:
      "Demonstrates persistent caching with unstable_cache. Cache function results across requests with tags and revalidation. Compare with fetch caching.",
    features: [
      "Cache any function, not just fetch",
      "Tags and revalidation support",
      "Side-by-side comparison with fetch",
      "Perfect for database queries and computations",
    ],
  },
  {
    href: "/examples/revalidate-tag-cachelife",
    title: "revalidateTag() with cacheLife (Next.js 16)",
    description:
      "Demonstrates the updated revalidateTag() API in Next.js 16, which now requires a cacheLife profile. Note: cacheLife is primarily for Vercel; custom handlers may not differentiate between profiles.",
    features: [
      "Breaking change from Next.js 15",
      "cacheLife profiles: max, hours, days",
      "Stale-while-revalidate behavior",
      "Custom cache handler limitations explained",
    ],
  },
  {
    href: "/examples/update-tag",
    title: "updateTag() API (Next.js 16)",
    description:
      "Demonstrates the new updateTag() API for immediate cache invalidation in Server Actions, providing read-your-writes semantics and instant cache updates after mutations.",
    features: [
      "Immediate cache invalidation",
      "Read-your-writes semantics",
      "Form submission examples",
      "Comparison with revalidateTag()",
    ],
  },
  {
    href: "/examples/isr/blog/1",
    title: "ISR with Static Params",
    description:
      "Incremental Static Regeneration with generateStaticParams. Pages are statically generated at build time and regenerated on demand.",
    features: [
      "Static generation at build time",
      "On-demand regeneration",
      "Time-based revalidation (1 hour)",
    ],
  },
  {
    href: "/examples/static-params/cache",
    title: "Static Params Test",
    description:
      "Tests static params generation with dynamic routes. Shows how static params work with revalidation.",
    features: [
      "Static params generation",
      "Dynamic params support",
      "Short revalidation period (5 seconds)",
    ],
  },
  {
    href: "/examples/pages-router-navigation",
    title: "Pages Router Navigation",
    description:
      "Pages Router example with getStaticProps. Demonstrates navigation between Pages Router pages when using registerInitialCache with pages: true.",
    features: [
      "Pages Router with getStaticProps",
      "ISR with revalidation",
      "Client-side navigation between pages",
    ],
  },
];

export default async function Home() {
  return (
    <ExampleLayout
      title="Next.js Cache Handler Examples"
      description="Explore various Next.js caching functionalities with Redis cache handler"
    >
      <div className="space-y-6">
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
            <strong>Note:</strong> All examples use Redis as the cache handler.
            Make sure Redis is running and configured in your environment
            variables.
          </p>
        </div>
      </div>
    </ExampleLayout>
  );
}
