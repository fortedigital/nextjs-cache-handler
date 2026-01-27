import { GetStaticProps } from "next";
import Link from "next/link";
import { ExampleLayout } from "@/components/ExampleLayout";
import { InfoCard } from "@/components/InfoCard";
import { CodeBlock } from "@/components/CodeBlock";

interface Item {
  id: string;
  title: string;
  description: string;
}

interface IndexPageProps {
  items: Item[];
}

export const getStaticProps: GetStaticProps<IndexPageProps> = async () => {
  const items: Item[] = [
    {
      id: "1",
      title: "Item 1",
      description: "First item in the Pages Router navigation example",
    },
    {
      id: "2",
      title: "Item 2",
      description: "Second item in the Pages Router navigation example",
    },
    {
      id: "3",
      title: "Item 3",
      description: "Third item in the Pages Router navigation example",
    },
  ];

  return {
    props: {
      items,
    },
    revalidate: 3600,
  };
};

export default function PagesRouterNavigationIndex({
  items,
}: IndexPageProps) {
  return (
    <ExampleLayout
      title="Pages Router Navigation"
      description="This example demonstrates Pages Router pages using getStaticProps. Navigate between items to test client-side navigation with registerInitialCache."
    >
      <div className="space-y-6">
        <InfoCard title="How it works">
          <ul className="list-disc list-inside space-y-1">
            <li>
              Uses Pages Router with{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                getStaticProps
              </code>{" "}
              for static generation
            </li>
            <li>
              Pages are statically generated and cached for 1 hour (
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                revalidate: 3600
              </code>
              )
            </li>
            <li>
              Navigate between items using the links below to test client-side
              navigation
            </li>
            <li>
              When <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                registerInitialCache
              </code>{" "}
              has <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                pages: true
              </code>
              , navigation may trigger RSC errors
            </li>
          </ul>
        </InfoCard>

        <div>
          <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">
            Items
          </h2>
          <div className="grid gap-4">
            {items.map((item) => (
              <Link
                key={item.id}
                href={`/examples/pages-router-navigation/${item.id}`}
                className="block p-4 bg-gray-50 dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-800 hover:border-blue-500 dark:hover:border-blue-600 transition-colors"
              >
                <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-1">
                  {item.title}
                </h3>
                <p className="text-gray-600 dark:text-gray-400 text-sm">
                  {item.description}
                </p>
              </Link>
            ))}
          </div>
        </div>

        <div>
          <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Code Example
          </h2>
          <CodeBlock>
            {`export const getStaticProps: GetStaticProps = async () => {
  return {
    props: {
      items: [
        { id: "1", title: "Item 1" },
        { id: "2", title: "Item 2" },
        { id: "3", title: "Item 3" },
      ],
    },
    revalidate: 3600,
  };
};

export default function IndexPage({ items }) {
  return (
    <div>
      {items.map((item) => (
        <Link key={item.id} href={\`/items/\${item.id}\`}>
          {item.title}
        </Link>
      ))}
    </div>
  );
}`}
          </CodeBlock>
        </div>
      </div>
    </ExampleLayout>
  );
}

