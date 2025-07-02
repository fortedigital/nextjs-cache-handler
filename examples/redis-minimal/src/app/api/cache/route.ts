import { revalidateTag } from "next/cache";

export async function GET() {
  revalidateTag("futurama");
  return new Response("Cache cleared for futurama tag");
}
