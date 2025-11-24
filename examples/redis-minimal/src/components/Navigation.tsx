"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const examples = [
  { href: "/", label: "Home", description: "Overview of all examples" },
  {
    href: "/examples/default-cache",
    label: "Default Cache",
    description: "Default force-cache behavior",
  },
  {
    href: "/examples/no-store",
    label: "No Store",
    description: "Always fetch fresh data",
  },
  {
    href: "/examples/time-based-revalidation",
    label: "Time Revalidation",
    description: "Time-based revalidation",
  },
  {
    href: "/examples/fetch-tags",
    label: "Fetch with Tags",
    description: "Cache tags and revalidation",
  },
  {
    href: "/examples/isr/blog/1",
    label: "ISR",
    description: "Incremental Static Regeneration",
  },
  {
    href: "/examples/static-params/cache",
    label: "Static Params",
    description: "Static params generation",
  },
];

export function Navigation() {
  const pathname = usePathname();

  const isActive = (href: string) => {
    if (!pathname) return false;
    if (href === "/") {
      return pathname === "/";
    }
    return pathname.startsWith(href);
  };

  return (
    <nav className="border-b border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-950 sticky top-0 z-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between h-16">
          <div className="flex">
            <div className="shrink-0 flex items-center">
              <Link
                href="/"
                className="text-xl font-bold text-gray-900 dark:text-gray-100 hover:text-gray-700 dark:hover:text-gray-300"
              >
                Next.js Cache Handler Examples
              </Link>
            </div>
            <div className="hidden sm:ml-6 sm:flex sm:space-x-8">
              {examples.map((example) => {
                const active = isActive(example.href);
                return (
                  <Link
                    key={example.href}
                    href={example.href}
                    className={`inline-flex items-center px-1 pt-1 text-sm font-medium transition-colors ${
                      active
                        ? "text-blue-600 dark:text-blue-400 border-b-2 border-blue-600 dark:border-blue-400"
                        : "text-gray-900 dark:text-gray-100 hover:text-gray-700 dark:hover:text-gray-300 border-b-2 border-transparent hover:border-gray-300 dark:hover:border-gray-700"
                    }`}
                  >
                    {example.label}
                  </Link>
                );
              })}
            </div>
          </div>
        </div>
      </div>
      <div className="sm:hidden border-t border-gray-200 dark:border-gray-800">
        <div className="pt-2 pb-3 space-y-1">
          {examples.map((example) => {
            const active = isActive(example.href);
            return (
              <Link
                key={example.href}
                href={example.href}
                className={`block pl-3 pr-4 py-2 text-base font-medium transition-colors ${
                  active
                    ? "text-blue-600 dark:text-blue-400 bg-blue-50 dark:bg-blue-950/20 border-l-4 border-blue-600 dark:border-blue-400"
                    : "text-gray-700 dark:text-gray-300 hover:text-gray-900 dark:hover:text-gray-100 hover:bg-gray-50 dark:hover:bg-gray-900"
                }`}
              >
                {example.label}
              </Link>
            );
          })}
        </div>
      </div>
    </nav>
  );
}
