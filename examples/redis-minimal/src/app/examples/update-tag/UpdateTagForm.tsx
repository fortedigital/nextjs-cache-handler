"use client";

import { useState, useTransition } from "react";
import { createPostWithUpdateTag } from "./actions";

export function UpdateTagForm() {
  const [isPending, startTransition] = useTransition();
  const [message, setMessage] = useState<string | null>(null);

  const handleSubmit = async (formData: FormData) => {
    setMessage(null);
    startTransition(async () => {
      try {
        const result = await createPostWithUpdateTag(formData);
        setMessage(result.message);
        // Reload after a short delay to show the updated cache
        setTimeout(() => {
          window.location.reload();
        }, 500);
      } catch {
        setMessage("Error creating post");
      }
    });
  };

  return (
    <form action={handleSubmit} className="space-y-4">
      <div>
        <label
          htmlFor="title"
          className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
        >
          Post Title
        </label>
        <input
          type="text"
          id="title"
          name="title"
          required
          className="w-full px-3 py-2 border border-gray-300 dark:border-gray-700 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
          placeholder="Enter post title"
        />
      </div>
      <div>
        <label
          htmlFor="content"
          className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
        >
          Post Content
        </label>
        <textarea
          id="content"
          name="content"
          required
          rows={3}
          className="w-full px-3 py-2 border border-gray-300 dark:border-gray-700 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
          placeholder="Enter post content"
        />
      </div>
      <button
        type="submit"
        disabled={isPending}
        className="w-full px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white rounded-md font-medium transition-colors"
      >
        {isPending ? "Creating..." : "Create Post (with updateTag)"}
      </button>
      {message && (
        <div className="text-sm text-green-600 dark:text-green-400">
          {message}
        </div>
      )}
    </form>
  );
}
