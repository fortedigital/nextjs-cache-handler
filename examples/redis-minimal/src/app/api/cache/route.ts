import { revalidateTag } from "next/cache";

export async function GET() {
  revalidateTag("futurama", "max");
  return new Response("Cache cleared for futurama tag");
}
