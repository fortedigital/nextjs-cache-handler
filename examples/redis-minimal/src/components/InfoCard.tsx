export function InfoCard({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div className="bg-blue-50 dark:bg-blue-950/20 border border-blue-200 dark:border-blue-900 rounded-lg p-4 mb-4">
      <h3 className="font-semibold text-blue-900 dark:text-blue-100 mb-2">
        {title}
      </h3>
      <div className="text-blue-800 dark:text-blue-200 text-sm">{children}</div>
    </div>
  );
}

