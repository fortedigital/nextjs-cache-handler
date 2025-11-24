import { ExampleLayout } from "@/components/ExampleLayout";
import { InfoCard } from "@/components/InfoCard";
import { CodeBlock } from "@/components/CodeBlock";
import { RevalidatePathButton } from "@/components/RevalidatePathButton";

interface Post {
  id: string;
  title: string;
  content: string;
}

export const revalidate = 3600;

export async function generateStaticParams() {
  try {
    const posts: Post[] = await fetch("https://api.vercel.app/blog").then(
      (res) => res.json()
    );
    return posts.map((post) => ({
      id: String(post.id),
    }));
  } catch (error) {
    console.error("Error generating static params:", error);
    return [];
  }
}

export default async function Page({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  const timestamp = new Date().toISOString();

  let post: Post;
  try {
    post = await fetch(`https://api.vercel.app/blog/${id}`).then((res) =>
      res.json()
    );
  } catch {
    return (
      <ExampleLayout
        title="ISR with Static Params Example"
        description="Incremental Static Regeneration with generateStaticParams"
      >
        <div className="text-red-600 dark:text-red-400">
          An error occurred fetching the post. Please check your network
          connection.
        </div>
      </ExampleLayout>
    );
  }

  return (
    <ExampleLayout
      title="ISR with Static Params Example"
      description="This example demonstrates Incremental Static Regeneration with generateStaticParams. Pages are statically generated at build time and regenerated on demand."
      actions={<RevalidatePathButton path={`/examples/isr/blog/${id}`} />}
    >
      <div className="space-y-6">
        <InfoCard title="How it works">
          <ul className="list-disc list-inside space-y-1">
            <li>
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                generateStaticParams
              </code>{" "}
              generates static paths at build time
            </li>
            <li>
              Pages are statically generated and cached for 1 hour (
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                revalidate: 3600
              </code>
              )
            </li>
            <li>
              After the revalidation period, pages are regenerated on the next
              request
            </li>
            <li>
              Try visiting different blog IDs (1, 2, 3, etc.) to see different
              posts
            </li>
            <li>
              Click &quot;Refresh Cache&quot; to manually revalidate this page
              before the 1-hour period expires
            </li>
          </ul>
        </InfoCard>

        <div className="space-y-4">
          <div>
            <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
              Blog Post
            </h2>
            <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-6 space-y-4">
              <div>
                <span className="text-sm font-medium text-gray-500 dark:text-gray-400">
                  Post ID:
                </span>{" "}
                <span className="text-gray-900 dark:text-gray-100">{id}</span>
              </div>
              <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                {post.title}
              </h1>
              <p className="text-gray-700 dark:text-gray-300 leading-relaxed">
                {post.content}
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
                <span className="text-gray-900 dark:text-gray-100 font-mono text-sm">
                  {timestamp}
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

        <div>
          <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Code Example
          </h2>
          <CodeBlock>
            {`export async function generateStaticParams() {
  const posts = await fetch("https://api.vercel.app/blog")
    .then((res) => res.json());
  return posts.map((post) => ({
    id: String(post.id),
  }));
}

export const revalidate = 3600;

export default async function Page({ params }) {
  const { id } = await params;
  const post = await fetch(\`https://api.vercel.app/blog/\${id}\`)
    .then((res) => res.json());
  return <article>{post.title}</article>;
}`}
          </CodeBlock>
        </div>
      </div>
    </ExampleLayout>
  );
}
