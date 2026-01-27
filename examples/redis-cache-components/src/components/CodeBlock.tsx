export function CodeBlock({ children }: { children: React.ReactNode }) {
  return (
    <pre className="bg-gray-900 dark:bg-gray-950 text-gray-100 p-4 rounded-lg overflow-x-auto border border-gray-800">
      <code className="text-sm font-mono">{children}</code>
    </pre>
  );
}

