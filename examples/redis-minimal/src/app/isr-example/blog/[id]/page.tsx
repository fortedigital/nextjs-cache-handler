interface Post {
  id: string;
  title: string;
  content: string;
}

export const revalidate = 3600;

export async function generateStaticParams() {
  const posts: Post[] = await fetch("https://api.vercel.app/blog").then((res) =>
    res.json()
  );
  return posts.map((post) => ({
    id: String(post.id),
  }));
}

export default async function Page({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  const post: Post = await fetch(`https://api.vercel.app/blog/${id}`).then(
    (res) => res.json()
  );
  return (
    <main>
      <h1>{post.title}</h1>
      <p>{post.content}</p>
    </main>
  );
}
