import { revalidateTag } from "next/cache";
import { NextRequest } from "next/server";

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const tag = searchParams.get("tag") || "futurama";

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
