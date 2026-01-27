"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState } from "react";

const examples = [
  { href: "/", label: "Home", description: "Overview of all examples" },
  {
    href: "/examples/use-cache",
    label: "use cache",
    description: "Basic use cache directive",
  },
  {
    href: "/examples/use-cache-remote",
    label: "use cache: remote",
    description: "Remote caching with Redis",
  },
  {
    href: "/examples/cache-life",
    label: "cacheLife",
    description: "Cache expiration with cacheLife",
  },
  {
    href: "/examples/cache-tag",
    label: "cacheTag",
    description: "Cache tagging and invalidation",
  },
  {
    href: "/examples/suspense-boundaries",
    label: "Suspense Boundaries",
    description: "Partial prerendering with Suspense",
  },
];

export function Navigation() {
  const pathname = usePathname();
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  const isActive = (href: string) => {
    if (!pathname) return false;
    if (href === "/") {
      return pathname === "/";
    }
    return pathname.startsWith(href);
  };

  return (
    <>
      <aside className="hidden lg:flex lg:flex-col lg:w-64 lg:fixed lg:inset-y-0 lg:border-r lg:border-gray-200 dark:lg:border-gray-800 lg:bg-white dark:lg:bg-gray-950">
        <div className="flex flex-col flex-1 overflow-y-auto">
          <div className="flex items-center h-16 px-6 border-b border-gray-200 dark:border-gray-800">
            <Link
              href="/"
              className="text-lg font-bold text-gray-900 dark:text-gray-100 hover:text-gray-700 dark:hover:text-gray-300"
            >
              Cache Components
            </Link>
          </div>
          <nav className="flex-1 px-4 py-4 space-y-1">
            {examples.map((example) => {
              const active = isActive(example.href);
              return (
                <Link
                  key={example.href}
                  href={example.href}
                  className={`block px-3 py-2 text-sm font-medium rounded-md transition-colors ${
                    active
                      ? "bg-blue-50 dark:bg-blue-950/20 text-blue-700 dark:text-blue-300 border-l-4 border-blue-600 dark:border-blue-400"
                      : "text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-900 hover:text-gray-900 dark:hover:text-gray-100"
                  }`}
                >
                  {example.label}
                </Link>
              );
            })}
          </nav>
        </div>
      </aside>

      <div className="lg:hidden">
        <div className="sticky top-0 z-50 bg-white dark:bg-gray-950 border-b border-gray-200 dark:border-gray-800">
          <div className="flex items-center justify-between h-16 px-4">
            <Link
              href="/"
              className="text-lg font-bold text-gray-900 dark:text-gray-100"
            >
              Cache Components
            </Link>
            <button
              onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
              className="p-2 rounded-md text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800"
              aria-label="Toggle menu"
            >
              <svg
                className="h-6 w-6"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                {mobileMenuOpen ? (
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M6 18L18 6M6 6l12 12"
                  />
                ) : (
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M4 6h16M4 12h16M4 18h16"
                  />
                )}
              </svg>
            </button>
          </div>
        </div>
        {mobileMenuOpen && (
          <div className="bg-white dark:bg-gray-950 border-b border-gray-200 dark:border-gray-800">
            <nav className="px-4 py-2 space-y-1">
              {examples.map((example) => {
                const active = isActive(example.href);
                return (
                  <Link
                    key={example.href}
                    href={example.href}
                    onClick={() => setMobileMenuOpen(false)}
                    className={`block px-3 py-2 text-base font-medium rounded-md transition-colors ${
                      active
                        ? "bg-blue-50 dark:bg-blue-950/20 text-blue-700 dark:text-blue-300"
                        : "text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-900"
                    }`}
                  >
                    {example.label}
                  </Link>
                );
              })}
            </nav>
          </div>
        )}
      </div>
    </>
  );
}

