"use client";

import { useState } from "react";

export function ClearCacheButton({
  tag,
  label = "Clear Cache",
}: {
  tag: string;
  label?: string;
}) {
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState<string | null>(null);

  const handleClear = async () => {
    setLoading(true);
    setMessage(null);
    try {
      const response = await fetch(`/api/revalidate?tag=${tag}&cacheLife=max`);
      const text = await response.text();
      setMessage(text);
      setTimeout(() => {
        window.location.reload();
      }, 500);
    } catch {
      setMessage("Error clearing cache");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex items-center gap-2">
      <button
        onClick={handleClear}
        disabled={loading}
        className="px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white rounded-md font-medium transition-colors"
      >
        {loading ? "Clearing..." : label}
      </button>
      {message && (
        <span className="text-sm text-gray-600 dark:text-gray-400">
          {message}
        </span>
      )}
    </div>
  );
}

