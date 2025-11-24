import { ExampleLayout } from "@/components/ExampleLayout";
import { InfoCard } from "@/components/InfoCard";
import { CodeBlock } from "@/components/CodeBlock";
import { RevalidateTagButton } from "@/components/RevalidateTagButton";
import { FuturamaCharacter } from "@/types/futurama";

async function fetchCharacter(
  id: string,
  tag: string
): Promise<FuturamaCharacter> {
  const response = await fetch(
    `https://api.sampleapis.com/futurama/characters/${id}`,
    {
      next: {
        revalidate: 3600,
        tags: [tag],
      },
    }
  );
  return response.json();
}

export default async function RevalidateTagCacheLifeExample() {
  const timestamp = new Date().toISOString();

  let maxProfile: FuturamaCharacter;
  let hoursProfile: FuturamaCharacter;
  let daysProfile: FuturamaCharacter;

  try {
    maxProfile = await fetchCharacter("7", "cachelife-max");
    hoursProfile = await fetchCharacter("8", "cachelife-hours");
    daysProfile = await fetchCharacter("9", "cachelife-days");
  } catch (error) {
    console.error("Error fetching character data:", error);
    return (
      <ExampleLayout
        title="revalidateTag() with cacheLife Profiles"
        description="Demonstrates the updated revalidateTag API with cacheLife profiles"
      >
        <div className="text-red-600 dark:text-red-400">
          An error occurred during fetch. Please check your network connection.
        </div>
      </ExampleLayout>
    );
  }

  return (
    <ExampleLayout
      title="revalidateTag() with cacheLife Profiles (Next.js 16)"
      description="This example demonstrates the updated revalidateTag() API in Next.js 16, which now requires a cacheLife profile as the second argument. Note: cacheLife behavior is primarily designed for Vercel's infrastructure; custom cache handlers may not fully support different cacheLife profiles."
    >
      <div className="space-y-6">
        <InfoCard title="Breaking Change in Next.js 16">
          <ul className="list-disc list-inside space-y-1">
            <li>
              <strong>Next.js 15:</strong>{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                revalidateTag(tag)
              </code>
            </li>
            <li>
              <strong>Next.js 16:</strong>{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                revalidateTag(tag, cacheLife)
              </code>{" "}
              - cacheLife is now required
            </li>
            <li>
              The{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                cacheLife
              </code>{" "}
              parameter enables stale-while-revalidate behavior
            </li>
            <li>
              Built-in profiles:{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                &apos;max&apos;
              </code>
              ,{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                &apos;hours&apos;
              </code>
              ,{" "}
              <code className="bg-gray-200 dark:bg-gray-800 px-1 rounded">
                &apos;days&apos;
              </code>
            </li>
            <li>
              You can also create custom cacheLife profiles for specific use
              cases
            </li>
          </ul>
        </InfoCard>

        <div className="bg-orange-50 dark:bg-orange-950/20 border border-orange-200 dark:border-orange-900 rounded-lg p-4">
          <h3 className="font-semibold text-orange-900 dark:text-orange-100 mb-2">
            ⚠️ Important: Custom Cache Handler Limitation
          </h3>
          <div className="text-orange-800 dark:text-orange-200 text-sm space-y-2">
            <p>
              <strong>Note:</strong> The{" "}
              <code className="bg-orange-100 dark:bg-orange-900 px-1 rounded">
                cacheLife
              </code>{" "}
              parameter is handled by Next.js internally and is not directly
              exposed to custom cache handlers.
            </p>
            <p>
              There are <strong>two different cache handler APIs</strong> in
              Next.js 16:
            </p>
            <ul className="list-disc list-inside ml-4 space-y-1">
              <li>
                <strong>Incremental Cache Handler</strong> (used by this
                package): Used for{" "}
                <code className="bg-orange-100 dark:bg-orange-900 px-1 rounded">
                  fetch
                </code>
                ,{" "}
                <code className="bg-orange-100 dark:bg-orange-900 px-1 rounded">
                  revalidateTag
                </code>
                , ISR, etc. Implements{" "}
                <code className="bg-orange-100 dark:bg-orange-900 px-1 rounded">
                  revalidateTag(tag: string)
                </code>{" "}
                - does not receive{" "}
                <code className="bg-orange-100 dark:bg-orange-900 px-1 rounded">
                  cacheLife
                </code>
                .
              </li>
              <li>
                <strong>New Cache Handlers API</strong> (for{" "}
                <code className="bg-orange-100 dark:bg-orange-900 px-1 rounded">
                  &apos;use cache&apos;
                </code>{" "}
                directive): Implements{" "}
                <code className="bg-orange-100 dark:bg-orange-900 px-1 rounded">
                  updateTags(tags, durations)
                </code>{" "}
                with{" "}
                <code className="bg-orange-100 dark:bg-orange-900 px-1 rounded">
                  durations.expire
                </code>
                , but this is a different mechanism and not the same as{" "}
                <code className="bg-orange-100 dark:bg-orange-900 px-1 rounded">
                  cacheLife
                </code>{" "}
                profiles.
              </li>
            </ul>
            <p>
              The{" "}
              <code className="bg-orange-100 dark:bg-orange-900 px-1 rounded">
                cacheLife
              </code>{" "}
              parameter for{" "}
              <code className="bg-orange-100 dark:bg-orange-900 px-1 rounded">
                revalidateTag()
              </code>{" "}
              is processed by Next.js core and primarily affects
              stale-while-revalidate behavior on Vercel&apos;s infrastructure.
              Custom handlers may not fully differentiate between different{" "}
              <code className="bg-orange-100 dark:bg-orange-900 px-1 rounded">
                cacheLife
              </code>{" "}
              profiles.
            </p>
            <p className="text-xs italic mt-2">
              Reference:{" "}
              <a
                href="https://nextjs.org/docs/app/api-reference/config/next-config-js/cacheHandlers"
                target="_blank"
                rel="noopener noreferrer"
                className="underline"
              >
                Next.js cacheHandlers documentation
              </a>
            </p>
          </div>
        </div>

        <div className="grid md:grid-cols-3 gap-4">
          <div className="bg-blue-50 dark:bg-blue-950/20 border border-blue-200 dark:border-blue-900 rounded-lg p-4">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-lg font-semibold text-blue-900 dark:text-blue-100">
                &apos;max&apos; Profile
              </h3>
              <RevalidateTagButton
                tag="cachelife-max"
                cacheLife="max"
                label="Revalidate"
              />
            </div>
            <p className="text-sm text-blue-800 dark:text-blue-200 mb-3">
              Maximum cache life - serves stale content while revalidating in
              the background
            </p>
            <div className="space-y-2 text-sm">
              <div>
                <span className="font-medium text-blue-800 dark:text-blue-200">
                  Character:
                </span>{" "}
                <span className="text-blue-900 dark:text-blue-100">
                  {maxProfile.name.first} {maxProfile.name.last}
                </span>
              </div>
              <div>
                <span className="font-medium text-blue-800 dark:text-blue-200">
                  Rendered:
                </span>{" "}
                <span className="text-blue-900 dark:text-blue-100 font-mono text-xs">
                  {timestamp}
                </span>
              </div>
            </div>
          </div>

          <div className="bg-green-50 dark:bg-green-950/20 border border-green-200 dark:border-green-900 rounded-lg p-4">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-lg font-semibold text-green-900 dark:text-green-100">
                &apos;hours&apos; Profile
              </h3>
              <RevalidateTagButton
                tag="cachelife-hours"
                cacheLife="hours"
                label="Revalidate"
              />
            </div>
            <p className="text-sm text-green-800 dark:text-green-200 mb-3">
              Hourly cache life - good balance for frequently updated content
            </p>
            <div className="space-y-2 text-sm">
              <div>
                <span className="font-medium text-green-800 dark:text-green-200">
                  Character:
                </span>{" "}
                <span className="text-green-900 dark:text-green-100">
                  {hoursProfile.name.first} {hoursProfile.name.last}
                </span>
              </div>
              <div>
                <span className="font-medium text-green-800 dark:text-green-200">
                  Rendered:
                </span>{" "}
                <span className="text-green-900 dark:text-green-100 font-mono text-xs">
                  {timestamp}
                </span>
              </div>
            </div>
          </div>

          <div className="bg-purple-50 dark:bg-purple-950/20 border border-purple-200 dark:border-purple-900 rounded-lg p-4">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-lg font-semibold text-purple-900 dark:text-purple-100">
                &apos;days&apos; Profile
              </h3>
              <RevalidateTagButton
                tag="cachelife-days"
                cacheLife="days"
                label="Revalidate"
              />
            </div>
            <p className="text-sm text-purple-800 dark:text-purple-200 mb-3">
              Daily cache life - for content that changes infrequently
            </p>
            <div className="space-y-2 text-sm">
              <div>
                <span className="font-medium text-purple-800 dark:text-purple-200">
                  Character:
                </span>{" "}
                <span className="text-purple-900 dark:text-purple-100">
                  {daysProfile.name.first} {daysProfile.name.last}
                </span>
              </div>
              <div>
                <span className="font-medium text-purple-800 dark:text-purple-200">
                  Rendered:
                </span>{" "}
                <span className="text-purple-900 dark:text-purple-100 font-mono text-xs">
                  {timestamp}
                </span>
              </div>
            </div>
          </div>
        </div>

        <div className="space-y-4">
          <div>
            <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">
              Code Examples
            </h2>
            <div className="space-y-4">
              <div>
                <h3 className="text-md font-medium text-gray-800 dark:text-gray-200 mb-2">
                  Next.js 15 (Old API - No longer works)
                </h3>
                <CodeBlock>
                  {`import { revalidateTag } from "next/cache";

// ❌ This no longer works in Next.js 16
revalidateTag("my-tag");`}
                </CodeBlock>
              </div>

              <div>
                <h3 className="text-md font-medium text-gray-800 dark:text-gray-200 mb-2">
                  Next.js 16 (New API - Required)
                </h3>
                <CodeBlock>
                  {`import { revalidateTag } from "next/cache";

// ✅ 'max' profile - maximum cache life
revalidateTag("my-tag", "max");

// ✅ 'hours' profile - hourly cache life
revalidateTag("my-tag", "hours");

// ✅ 'days' profile - daily cache life
revalidateTag("my-tag", "days");

// ✅ Custom profile (if configured)
revalidateTag("my-tag", "custom-profile");`}
                </CodeBlock>
              </div>

              <div>
                <h3 className="text-md font-medium text-gray-800 dark:text-gray-200 mb-2">
                  Using in API Routes
                </h3>
                <CodeBlock>
                  {`import { revalidateTag } from "next/cache";
import { NextRequest } from "next/server";

export async function POST(request: NextRequest) {
  const { tag, cacheLife } = await request.json();
  
  revalidateTag(tag, cacheLife || "max");
  
  return Response.json({ 
    message: \`Revalidated \${tag} with \${cacheLife} profile\` 
  });
}`}
                </CodeBlock>
              </div>

              <div>
                <h3 className="text-md font-medium text-gray-800 dark:text-gray-200 mb-2">
                  Using in Server Actions
                </h3>
                <CodeBlock>
                  {`"use server";

import { revalidateTag } from "next/cache";

export async function updateProduct(productId: string) {
  await updateProductInDatabase(productId);
  
  revalidateTag(\`product-\${productId}\`, "max");
  
  return { success: true };
}`}
                </CodeBlock>
              </div>
            </div>
          </div>
        </div>

        <div className="bg-yellow-50 dark:bg-yellow-950/20 border border-yellow-200 dark:border-yellow-900 rounded-lg p-4">
          <h3 className="font-semibold text-yellow-900 dark:text-yellow-100 mb-2">
            Understanding cacheLife Profiles
          </h3>
          <div className="text-yellow-800 dark:text-yellow-200 text-sm space-y-2">
            <p>
              <strong>&apos;max&apos;:</strong> Maximum cache life. Serves stale
              content while revalidating in the background. Best for content
              that can tolerate being slightly stale.
            </p>
            <p>
              <strong>&apos;hours&apos;:</strong> Hourly cache life. Good for
              content that updates frequently but doesn&apos;t need to be
              real-time.
            </p>
            <p>
              <strong>&apos;days&apos;:</strong> Daily cache life. Perfect for
              content that changes infrequently, like blog posts or product
              catalogs.
            </p>
            <p className="mt-2">
              <strong>Stale-while-revalidate:</strong> When you call{" "}
              <code className="bg-yellow-100 dark:bg-yellow-900 px-1 rounded">
                revalidateTag()
              </code>
              , Next.js serves the stale cached content immediately while
              revalidating in the background. This ensures fast responses while
              keeping content fresh.
            </p>
          </div>
        </div>
      </div>
    </ExampleLayout>
  );
}
