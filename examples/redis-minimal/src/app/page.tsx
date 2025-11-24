import Link from "next/link";
import { ExampleLayout } from "@/components/ExampleLayout";

const examples = [
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
