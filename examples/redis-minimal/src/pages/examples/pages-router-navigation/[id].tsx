import { GetStaticPaths, GetStaticProps } from "next";
import Link from "next/link";
import { ExampleLayout } from "@/components/ExampleLayout";
import { InfoCard } from "@/components/InfoCard";
import { CodeBlock } from "@/components/CodeBlock";

interface Item {
  id: string;
  title: string;
  description: string;
  content: string;
  timestamp: string;
}

interface ItemPageProps {
  item: Item;
}

export const getStaticPaths: GetStaticPaths = async () => {
  return {
    paths: [
      { params: { id: "1" } },
      { params: { id: "2" } },
      { params: { id: "3" } },
    ],
    fallback: false,
  };
};

export const getStaticProps: GetStaticProps<ItemPageProps> = async ({
  params,
}) => {
  const item: Item = {
    id: params?.id as string,
    title: `Item ${params?.id}`,
    description: `Description for item ${params?.id}`,
    content: `This is the detailed content for item ${params?.id}. This page uses Pages Router with getStaticProps and ISR.`,
    timestamp: new Date().toISOString(),
  };

  return {
    props: {
      item,
    },
    revalidate: 3600,
  };
};

export default function ItemPage({ item }: ItemPageProps) {
  const itemIds = ["1", "2", "3"];
  const currentIndex = itemIds.indexOf(item.id);
  const prevId = currentIndex > 0 ? itemIds[currentIndex - 1] : null;
  const nextId =
    currentIndex < itemIds.length - 1 ? itemIds[currentIndex + 1] : null;

  return (
    <ExampleLayout
      title={`${item.title} - Pages Router Navigation`}
      description="Pages Router page with getStaticProps and ISR"
    >
      <div className="space-y-6">
        <div>
          <Link
            href="/examples/pages-router-navigation"
            className="text-blue-600 dark:text-blue-400 hover:text-blue-800 dark:hover:text-blue-300 text-sm mb-4 inline-block"
          >
            ← Back to Items
          </Link>
        </div>

        <div className="space-y-4">
          <div>
            <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
              Item Details
            </h2>
            <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-6 space-y-4">
              <div>
                <span className="text-sm font-medium text-gray-500 dark:text-gray-400">
                  Item ID:
                </span>{" "}
                <span className="text-gray-900 dark:text-gray-100">
                  {item.id}
                </span>
              </div>
              <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                {item.title}
              </h1>
              <p className="text-gray-600 dark:text-gray-400">
                {item.description}
              </p>
              <p className="text-gray-700 dark:text-gray-300 leading-relaxed">
                {item.content}
              </p>
            </div>
          </div>

          <div>
            <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
              Cache Information
            </h2>
            <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
              <div>
                <span className="font-medium text-gray-700 dark:text-gray-300">
                  Rendered at:
                </span>{" "}
                <span
                  data-testid="build-timestamp"
                  className="text-gray-900 dark:text-gray-100 font-mono text-sm"
                >
                  {item.timestamp}
                </span>
              </div>
              <div className="mt-2">
                <span className="font-medium text-gray-700 dark:text-gray-300">
                  Revalidation:
                </span>{" "}
                <span className="text-gray-900 dark:text-gray-100">1 hour</span>
              </div>
              <div className="mt-2">
                <span className="font-medium text-gray-700 dark:text-gray-300">
                  Generation:
                </span>{" "}
                <span className="text-gray-900 dark:text-gray-100">
                  Static (ISR)
                </span>
              </div>
            </div>
          </div>
        </div>

        <div className="flex justify-between items-center pt-4 border-t border-gray-200 dark:border-gray-800">
          {prevId ? (
            <Link
              href={`/examples/pages-router-navigation/${prevId}`}
              className="px-4 py-2 text-sm font-medium text-blue-600 dark:text-blue-400 hover:text-blue-800 dark:hover:text-blue-300 border border-blue-300 dark:border-blue-700 rounded-md transition-colors"
            >
              ← Previous Item
            </Link>
          ) : (
            <div></div>
          )}
          {nextId ? (
            <Link
              href={`/examples/pages-router-navigation/${nextId}`}
              className="px-4 py-2 text-sm font-medium text-blue-600 dark:text-blue-400 hover:text-blue-800 dark:hover:text-blue-300 border border-blue-300 dark:border-blue-700 rounded-md transition-colors"
            >
              Next Item →
            </Link>
          ) : (
            <div></div>
          )}
        </div>

        <div>
          <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Code Example
          </h2>
          <CodeBlock>
            {`export const getStaticPaths: GetStaticPaths = async () => {
  return {
    paths: [
      { params: { id: "1" } },
      { params: { id: "2" } },
      { params: { id: "3" } },
    ],
    fallback: false,
  };
};

export const getStaticProps: GetStaticProps = async ({ params }) => {
  const item = {
    id: params?.id,
    title: \`Item \${params?.id}\`,
    content: "Item content...",
  };

  return {
    props: { item },
    revalidate: 3600,
  };
};

export default function ItemPage({ item }) {
  return (
    <div>
      <h1>{item.title}</h1>
      <p>{item.content}</p>
    </div>
  );
}`}
          </CodeBlock>
        </div>
      </div>
    </ExampleLayout>
  );
}

