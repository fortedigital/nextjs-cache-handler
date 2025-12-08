import { ExampleLayout } from "@/components/ExampleLayout";
import { InfoCard } from "@/components/InfoCard";
import { CodeBlock } from "@/components/CodeBlock";
import { UpdateTagForm } from "./UpdateTagForm";
import { SettingsForm } from "./SettingsForm";

async function fetchUserProfile(): Promise<{
  name: string;
  email: string;
  preferences: {
    theme: string;
    notifications: boolean;
  };
  lastUpdated: string;
}> {
  const response = await fetch("https://jsonplaceholder.typicode.com/users/1", {
    next: {
      tags: ["user-profile"],
      revalidate: 3600,
    },
  });
  const user = await response.json();

  // Simulate user settings
  return {
    name: user.name,
    email: user.email,
    preferences: {
      theme: "dark",
      notifications: true,
    },
    lastUpdated: new Date().toISOString(),
  };
}

async function fetchPosts(): Promise<
  Array<{ id: number; title: string; body: string; timestamp: string }>
> {
  const response = await fetch(
    "https://jsonplaceholder.typicode.com/posts?_limit=3",
    {
      next: {
        tags: ["posts"],
        revalidate: 3600,
      },
    }
  );
  const posts = await response.json();

  return posts.map((post: { id: number; title: string; body: string }) => ({
    ...post,
    timestamp: new Date().toISOString(),
  }));
}

export default async function UpdateTagExample() {
  const [userProfile, posts] = await Promise.all([
    fetchUserProfile(),
    fetchPosts(),
  ]);

  return (
    <ExampleLayout
      title="updateTag() API (Next.js 16)"
      description="Demonstrates the new updateTag() API for immediate cache invalidation in Server Actions, providing read-your-writes semantics and instant cache updates after mutations."
    >
      <div className="space-y-6">
        <InfoCard title="What is updateTag()?">
          <ul className="list-disc list-inside space-y-2 text-sm">
            <li>
              <strong>New in Next.js 16:</strong>{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                updateTag()
              </code>{" "}
              provides immediate cache invalidation within Server Actions
            </li>
            <li>
              <strong>Read-your-writes semantics:</strong> Ensures users see
              their own changes immediately after mutations
            </li>
            <li>
              <strong>Immediate expiration:</strong> Unlike{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                revalidateTag()
              </code>
              , it immediately expires the cache, forcing the next request to
              fetch fresh data
            </li>
            <li>
              <strong>Server Actions only:</strong> Can only be used within
              Server Actions, not Route Handlers
            </li>
            <li>
              <strong>Perfect for:</strong> Form submissions, user settings
              updates, and any mutation where immediate consistency is required
            </li>
          </ul>
        </InfoCard>

        <div className="bg-blue-50 dark:bg-blue-950/20 border border-blue-200 dark:border-blue-900 rounded-lg p-4">
          <h3 className="font-semibold text-blue-900 dark:text-blue-100 mb-2">
            📊 Comparison: updateTag() vs revalidateTag()
          </h3>
          <div className="overflow-x-auto">
            <table className="w-full text-sm text-left">
              <thead className="bg-blue-100 dark:bg-blue-900/50">
                <tr>
                  <th className="px-4 py-2 font-semibold text-blue-900 dark:text-blue-100">
                    Feature
                  </th>
                  <th className="px-4 py-2 font-semibold text-blue-900 dark:text-blue-100">
                    updateTag()
                  </th>
                  <th className="px-4 py-2 font-semibold text-blue-900 dark:text-blue-100">
                    revalidateTag()
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-blue-200 dark:divide-blue-800">
                <tr>
                  <td className="px-4 py-2 text-blue-800 dark:text-blue-200">
                    Usage
                  </td>
                  <td className="px-4 py-2 text-blue-900 dark:text-blue-100">
                    Server Actions only
                  </td>
                  <td className="px-4 py-2 text-blue-900 dark:text-blue-100">
                    Server Actions & Route Handlers
                  </td>
                </tr>
                <tr>
                  <td className="px-4 py-2 text-blue-800 dark:text-blue-200">
                    Cache Behavior
                  </td>
                  <td className="px-4 py-2 text-blue-900 dark:text-blue-100">
                    Immediately expires cache
                  </td>
                  <td className="px-4 py-2 text-blue-900 dark:text-blue-100">
                    Marks as stale, serves stale while revalidating
                  </td>
                </tr>
                <tr>
                  <td className="px-4 py-2 text-blue-800 dark:text-blue-200">
                    Next Request
                  </td>
                  <td className="px-4 py-2 text-blue-900 dark:text-blue-100">
                    Always fetches fresh data
                  </td>
                  <td className="px-4 py-2 text-blue-900 dark:text-blue-100">
                    May serve stale content during revalidation
                  </td>
                </tr>
                <tr>
                  <td className="px-4 py-2 text-blue-800 dark:text-blue-200">
                    Use Case
                  </td>
                  <td className="px-4 py-2 text-blue-900 dark:text-blue-100">
                    Immediate consistency (forms, settings)
                  </td>
                  <td className="px-4 py-2 text-blue-900 dark:text-blue-100">
                    Background updates (catalogs, blogs)
                  </td>
                </tr>
                <tr>
                  <td className="px-4 py-2 text-blue-800 dark:text-blue-200">
                    Read-your-writes
                  </td>
                  <td className="px-4 py-2 text-blue-900 dark:text-blue-100">
                    ✅ Guaranteed
                  </td>
                  <td className="px-4 py-2 text-blue-900 dark:text-blue-100">
                    ❌ Not guaranteed
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        <div className="grid md:grid-cols-2 gap-6">
          {/* Form Submission Example */}
          <div className="bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-lg p-6">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-4">
              📝 Form Submission with updateTag()
            </h2>
            <p className="text-sm text-gray-600 dark:text-gray-400 mb-4">
              This form uses{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                updateTag()
              </code>{" "}
              to immediately invalidate the cache after creating a new post. The
              new post appears instantly without stale content.
            </p>

            <div className="mb-4 p-3 bg-gray-50 dark:bg-gray-800 rounded">
              <p className="text-xs font-mono text-gray-600 dark:text-gray-400 mb-2">
                Current posts (cached with tag: &quot;posts&quot;)
              </p>
              <div className="space-y-2">
                {posts.map((post) => (
                  <div key={post.id} className="text-sm">
                    <div className="font-medium text-gray-900 dark:text-gray-100">
                      {post.title}
                    </div>
                    <div className="text-xs text-gray-500 dark:text-gray-400">
                      Cached: {new Date(post.timestamp).toLocaleTimeString()}
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <UpdateTagForm />
          </div>

          {/* Settings Update Example */}
          <div className="bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-lg p-6">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-4">
              ⚙️ Settings Update with updateTag()
            </h2>
            <p className="text-sm text-gray-600 dark:text-gray-400 mb-4">
              User settings are cached with the &quot;user-profile&quot; tag.
              When you update settings,{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                updateTag()
              </code>{" "}
              ensures your changes are immediately visible.
            </p>

            <div className="mb-4 p-3 bg-gray-50 dark:bg-gray-800 rounded">
              <p className="text-xs font-mono text-gray-600 dark:text-gray-400 mb-2">
                Current profile (cached with tag: &quot;user-profile&quot;)
              </p>
              <div className="text-sm space-y-1">
                <div>
                  <span className="font-medium text-gray-900 dark:text-gray-100">
                    Name:
                  </span>{" "}
                  <span className="text-gray-700 dark:text-gray-300">
                    {userProfile.name}
                  </span>
                </div>
                <div>
                  <span className="font-medium text-gray-900 dark:text-gray-100">
                    Email:
                  </span>{" "}
                  <span className="text-gray-700 dark:text-gray-300">
                    {userProfile.email}
                  </span>
                </div>
                <div>
                  <span className="font-medium text-gray-900 dark:text-gray-100">
                    Theme:
                  </span>{" "}
                  <span className="text-gray-700 dark:text-gray-300">
                    {userProfile.preferences.theme}
                  </span>
                </div>
                <div>
                  <span className="font-medium text-gray-900 dark:text-gray-100">
                    Notifications:
                  </span>{" "}
                  <span className="text-gray-700 dark:text-gray-300">
                    {userProfile.preferences.notifications
                      ? "Enabled"
                      : "Disabled"}
                  </span>
                </div>
                <div className="text-xs text-gray-500 dark:text-gray-400 mt-2">
                  Cached:{" "}
                  {new Date(userProfile.lastUpdated).toLocaleTimeString()}
                </div>
              </div>
            </div>

            <SettingsForm />
          </div>
        </div>

        <div>
          <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Server Action Example
          </h3>
          <CodeBlock>
            {`'use server'

import { updateTag } from 'next/cache'

export async function createPost(formData: FormData) {
  const title = formData.get('title') as string
  const content = formData.get('content') as string

  const post = await db.post.create({
    data: { title, content },
  })

  updateTag('posts')

  redirect(\`/posts/\${post.id}\`)
}`}
          </CodeBlock>
        </div>

        <div>
          <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
            Settings Update Example
          </h3>
          <CodeBlock>
            {`'use server'

import { updateTag } from 'next/cache'

export async function updateUserSettings(formData: FormData) {
  const theme = formData.get('theme') as string
  const notifications = formData.get('notifications') === 'on'

  await db.user.update({
    where: { id: userId },
    data: { theme, notifications },
  })

  updateTag('user-profile')
}`}
          </CodeBlock>
        </div>

        <InfoCard title="Key Takeaways">
          <ul className="list-disc list-inside space-y-2 text-sm">
            <li>
              Use{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                updateTag()
              </code>{" "}
              in Server Actions when you need immediate cache invalidation after
              mutations
            </li>
            <li>
              Perfect for form submissions, user settings, and any scenario
              requiring read-your-writes semantics
            </li>
            <li>
              Unlike{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                revalidateTag()
              </code>
              , it immediately expires the cache rather than using
              stale-while-revalidate
            </li>
            <li>
              The next request after{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                updateTag()
              </code>{" "}
              will always fetch fresh data from the source
            </li>
            <li>
              This ensures users see their own changes immediately, providing a
              better user experience
            </li>
          </ul>
        </InfoCard>
      </div>
    </ExampleLayout>
  );
}
