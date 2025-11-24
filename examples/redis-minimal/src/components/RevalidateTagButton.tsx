"use client";

import { useState } from "react";

export function RevalidateTagButton({
  tag,
  cacheLife = "max",
  label = "Revalidate",
}: {
  tag: string;
  cacheLife?: "max" | "hours" | "days";
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
        body: JSON.stringify({ tag, cacheLife }),
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
    <div className="flex flex-col items-end gap-1">
      <button
        onClick={handleRevalidate}
        disabled={loading}
        className="px-3 py-1.5 text-xs font-medium bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white rounded-md transition-colors"
      >
        {loading ? "Revalidating..." : label}
      </button>
      {message && (
        <span className="text-xs text-gray-600 dark:text-gray-400">
          {message}
        </span>
      )}
    </div>
  );
}

