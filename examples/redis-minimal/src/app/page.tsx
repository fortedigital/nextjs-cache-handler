export default async function Home() {
  return (
    <div>
      <span>{new Date().toISOString()}</span>
    </div>
  );
}

export const revalidate = 3600;
