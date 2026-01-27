export function ExampleLayout({
  children,
  title,
  description,
  actions,
}: {
  children: React.ReactNode;
  title: string;
  description: string;
  actions?: React.ReactNode;
}) {
  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="bg-white dark:bg-gray-950 rounded-lg shadow-sm border border-gray-200 dark:border-gray-800 p-6">
          <div className="mb-6">
            <div className="flex items-center justify-between mb-2">
              <h1 className="text-3xl font-bold text-gray-900 dark:text-gray-100">
                {title}
              </h1>
              {actions && <div className="flex gap-2">{actions}</div>}
            </div>
            <p className="text-gray-600 dark:text-gray-400">{description}</p>
          </div>
          <div className="border-t border-gray-200 dark:border-gray-800 pt-6">
            {children}
          </div>
        </div>
      </main>
    </div>
  );
}

