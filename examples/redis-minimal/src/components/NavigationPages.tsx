"use client";

import Link from "next/link";
import { useRouter } from "next/router";
import { useState } from "react";

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
    href: "/examples/unstable-cache",
    label: "unstable_cache",
    description: "Persistent function caching",
  },
  {
    href: "/examples/revalidate-tag-cachelife",
    label: "revalidateTag cacheLife",
    description: "Next.js 16 cacheLife profiles",
  },
  {
    href: "/examples/update-tag",
    label: "updateTag",
    description: "Immediate cache invalidation in Server Actions",
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
  {
    href: "/examples/pages-router-navigation",
    label: "Pages Router",
    description: "Pages Router navigation example",
  },
];

export function NavigationPages() {
  const router = useRouter();
  const pathname = router.pathname;
  const [loading, setLoading] = useState(false);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  const isActive = (href: string) => {
    if (!pathname) return false;
    if (href === "/") {
      return pathname === "/";
    }
    return pathname.startsWith(href);
  };

  const handleRevalidateLayout = async () => {
    setLoading(true);
    try {
      const response = await fetch("/api/revalidate", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ path: pathname || "/", type: "layout" }),
      });
      const text = await response.text();
      console.log(text);
      setTimeout(() => {
        window.location.reload();
      }, 500);
    } catch (error) {
      console.error("Error revalidating layout:", error);
    } finally {
      setLoading(false);
    }
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
              Cache Examples
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
          <div className="p-4 border-t border-gray-200 dark:border-gray-800">
            <button
              onClick={handleRevalidateLayout}
              disabled={loading}
              className="w-full px-3 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 hover:text-gray-900 dark:hover:text-gray-100 hover:bg-gray-50 dark:hover:bg-gray-800 rounded-md transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              title="Revalidate layout cache"
            >
              {loading ? "Revalidating..." : "Revalidate Layout"}
            </button>
          </div>
        </div>
      </aside>

      <div className="lg:hidden">
        <div className="sticky top-0 z-50 bg-white dark:bg-gray-950 border-b border-gray-200 dark:border-gray-800">
          <div className="flex items-center justify-between h-16 px-4">
            <Link
              href="/"
              className="text-lg font-bold text-gray-900 dark:text-gray-100"
            >
              Cache Examples
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
              <button
                onClick={handleRevalidateLayout}
                disabled={loading}
                className="w-full text-left px-3 py-2 text-base font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-900 rounded-md transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {loading ? "Revalidating..." : "Revalidate Layout"}
              </button>
            </nav>
          </div>
        )}
      </div>
    </>
  );
}

