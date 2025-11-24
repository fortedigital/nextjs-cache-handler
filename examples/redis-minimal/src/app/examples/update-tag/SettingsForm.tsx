"use client";

import { useState, useTransition } from "react";
import { updateUserSettings } from "./actions";

export function SettingsForm() {
  const [isPending, startTransition] = useTransition();
  const [message, setMessage] = useState<string | null>(null);

  const handleSubmit = async (formData: FormData) => {
    setMessage(null);
    startTransition(async () => {
      try {
        const result = await updateUserSettings(formData);
        setMessage(result.message);
        // Reload after a short delay to show the updated cache
        setTimeout(() => {
          window.location.reload();
        }, 500);
      } catch {
        setMessage("Error updating settings");
      }
    });
  };

  return (
    <form action={handleSubmit} className="space-y-4">
      <div>
        <label
          htmlFor="theme"
          className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
        >
          Theme
        </label>
        <select
          id="theme"
          name="theme"
          className="w-full px-3 py-2 border border-gray-300 dark:border-gray-700 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          <option value="light">Light</option>
          <option value="dark">Dark</option>
          <option value="auto">Auto</option>
        </select>
      </div>
      <div className="flex items-center">
        <input
          type="checkbox"
          id="notifications"
          name="notifications"
          defaultChecked
          className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
        />
        <label
          htmlFor="notifications"
          className="ml-2 block text-sm text-gray-700 dark:text-gray-300"
        >
          Enable notifications
        </label>
      </div>
      <button
        type="submit"
        disabled={isPending}
        className="w-full px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white rounded-md font-medium transition-colors"
      >
        {isPending ? "Updating..." : "Update Settings (with updateTag)"}
      </button>
      {message && (
        <div className="text-sm text-green-600 dark:text-green-400">
          {message}
        </div>
      )}
    </form>
  );
}
