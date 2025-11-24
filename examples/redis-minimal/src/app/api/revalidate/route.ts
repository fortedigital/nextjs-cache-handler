import { revalidateTag, revalidatePath } from "next/cache";
import { NextRequest } from "next/server";

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const tag = searchParams.get("tag");
  const cacheLife = (searchParams.get("cacheLife") || "max") as
    | "max"
    | "hours"
    | "days";

  if (tag) {
    try {
      revalidateTag(tag, cacheLife);
      return new Response(
        `Cache revalidated for tag: ${tag} with profile: ${cacheLife}`,
        {
          status: 200,
        }
      );
    } catch {
      return new Response(`Error revalidating cache for tag: ${tag}`, {
        status: 500,
      });
    }
  }

  return new Response("Tag parameter is required for GET requests", {
    status: 400,
  });
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { tag, path } = body;

    if (tag) {
      const cacheLife = (body.cacheLife || "max") as "max" | "hours" | "days";
      revalidateTag(tag, cacheLife);
      return new Response(
        `Cache revalidated for tag: ${tag} with profile: ${cacheLife}`,
        {
          status: 200,
        }
      );
    }

    if (path) {
      const type = body.type as "page" | "layout" | undefined;
      if (type) {
        revalidatePath(path, type);
        return new Response(`Cache revalidated for ${type}: ${path}`, {
          status: 200,
        });
      }
      revalidatePath(path);
      return new Response(`Cache revalidated for path: ${path}`, {
        status: 200,
      });
    }

    return new Response("Either 'tag' or 'path' is required", {
      status: 400,
    });
  } catch (error) {
    return new Response(`Error revalidating cache: ${error}`, {
      status: 500,
    });
  }
}
