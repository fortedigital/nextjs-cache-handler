import { revalidateTag, revalidatePath } from "next/cache";
import { NextRequest } from "next/server";

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const tag = searchParams.get("tag");

  if (tag) {
    try {
      revalidateTag(tag, "max");
      return new Response(`Cache cleared for tag: ${tag}`, {
        status: 200,
      });
    } catch (error) {
      return new Response(`Error clearing cache for tag: ${tag}`, {
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
      revalidateTag(tag, "max");
      return new Response(`Cache cleared for tag: ${tag}`, {
        status: 200,
      });
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
