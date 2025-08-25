import { GetStaticPaths, GetStaticProps } from "next";

interface Post {
  id: string;
  title: string;
  content: string;
  timestamp: string;
}

interface PostPageProps {
  post: Post;
}

export const getStaticPaths: GetStaticPaths = async () => {
  return {
    paths: [
      { params: { id: "1" } },
      { params: { id: "2" } },
      { params: { id: "3" } },
    ],
    fallback: false,
  };
};

export const getStaticProps: GetStaticProps<PostPageProps> = async ({
  params,
}) => {
  const post: Post = {
    id: params?.id as string,
    title: `Post ${params?.id}`,
    content: `This is the content for post ${params?.id}`,
    timestamp: new Date().toISOString(),
  };

  return {
    props: {
      post,
    },
    revalidate: 3600,
  };
};

export default function Post({ post }: PostPageProps) {
  return (
    <div>
      <h1>{post.title}</h1>
      <p>{post.content}</p>
      <p>
        <small>Generated at: {post.timestamp}</small>
      </p>
    </div>
  );
}
