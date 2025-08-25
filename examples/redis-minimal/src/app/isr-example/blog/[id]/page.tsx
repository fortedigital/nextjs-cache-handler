interface Post {
    id: string
    title: string
    content: string
  }
   
  // Next.js will invalidate the cache when a
  // request comes in, at most once every 60 seconds.
  export const revalidate = 60
   
  export async function generateStaticParams() {
    const posts: Post[] = await fetch('https://api.vercel.app/blog').then((res) =>
      res.json()
    )
    return posts.map((post) => ({
      id: String(post.id),
    }))
  }
   
  export default async function Page({
    params,
  }: {
    params: Promise<{ id: string }>
  }) {
    const { id } = await params
    const post: Post = await fetch(`https://api.vercel.app/blog/${id}`).then(
      (res) => res.json()
    )
    return (
      <main>
        <h1>{post.title}</h1>
        <p>{post.content}</p>
      </main>
    )
  }