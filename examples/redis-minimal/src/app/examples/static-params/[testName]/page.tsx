import { ExampleLayout } from "@/components/ExampleLayout";
import { InfoCard } from "@/components/InfoCard";
import { CodeBlock } from "@/components/CodeBlock";

export const dynamicParams = true;

export const revalidate = 5;

export default async function TestPage({
  params,
}: {
  params: Promise<{ testName: string }>;
}) {
  const { testName } = await params;
  const timestamp = new Date().toISOString();

  return (
    <ExampleLayout
      title="Static Params Test Example"
      description="This example tests static params generation with dynamic routes and short revalidation periods."
    >
      <div className="space-y-6">
        <InfoCard title="How it works">
          <ul className="list-disc list-inside space-y-1">
            <li>
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                generateStaticParams
              </code>{" "}
              generates the &quot;cache&quot; route at build time
            </li>
            <li>
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                dynamicParams: true
              </code>{" "}
              allows other dynamic routes to be generated on demand
            </li>
            <li>
              Very short revalidation period (5 seconds) for testing purposes
            </li>
            <li>
              Try visiting different routes like{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                /examples/static-params/test1
              </code>{" "}
              or{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                /examples/static-params/test2
              </code>
            </li>
          </ul>
        </InfoCard>

        <div className="space-y-4">
          <div>
            <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
              Route Information
            </h2>
            <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4 space-y-2">
              <div>
                <span className="font-medium text-gray-700 dark:text-gray-300">
                  Route Parameter:
                </span>{" "}
                <span className="text-gray-900 dark:text-gray-100 font-mono">
                  {testName}
                </span>
              </div>
              <div>
                <span className="font-medium text-gray-700 dark:text-gray-300">
                  Rendered at:
                </span>{" "}
                <span className="text-gray-900 dark:text-gray-100 font-mono text-sm">
                  {timestamp}
                </span>
              </div>
            </div>
          </div>

          <div>
            <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
              Cache Information
            </h2>
            <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
              <div>
                <span className="font-medium text-gray-700 dark:text-gray-300">
                  Revalidation:
                </span>{" "}
                <span className="text-gray-900 dark:text-gray-100">5 seconds</span>
              </div>
              <div className="mt-2">
                <span className="font-medium text-gray-700 dark:text-gray-300">
                  Dynamic Params:
                </span>{" "}
                <span className="text-gray-900 dark:text-gray-100">Enabled</span>
              </div>
              <div className="mt-2">
                <span className="font-medium text-gray-700 dark:text-gray-300">
                  Generation:
                </span>{" "}
                <span className="text-gray-900 dark:text-gray-100">
                  {testName === "cache"
                    ? "Static (pre-generated)"
                    : "On-demand (dynamic)"}
                </span>
              </div>
            </div>
          </div>
        </div>

        <div>
          <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Code Example
          </h2>
          <CodeBlock>
{`export const dynamicParams = true;
export const revalidate = 5;

export async function generateStaticParams() {
  return [{ testName: "cache" }];
}

export default async function TestPage({ params }) {
  const { testName } = await params;
  return <div>{testName}</div>;
}`}
          </CodeBlock>
        </div>
      </div>
    </ExampleLayout>
  );
}

export async function generateStaticParams() {
  return [{ testName: "cache" }];
}

