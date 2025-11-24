"use client";

import { useState } from "react";

export function RevalidatePathButton({
  path,
  label = "Refresh Cache",
}: {
  path: string;
  label?: string;
}) {
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState<string | null>(null);

  const handleRevalidate = async () => {
    setLoading(true);
    setMessage(null);
    try {
      const response = await fetch("/api/revalidate", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ path }),
      });
      const text = await response.text();
      setMessage(text);
      setTimeout(() => {
        window.location.reload();
      }, 500);
    } catch {
      setMessage("Error revalidating cache");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex items-center gap-2">
      <button
        onClick={handleRevalidate}
        disabled={loading}
        className="px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white rounded-md font-medium transition-colors"
      >
        {loading ? "Refreshing..." : label}
      </button>
      {message && (
        <span className="text-sm text-gray-600 dark:text-gray-400">
          {message}
        </span>
      )}
    </div>
  );
}

