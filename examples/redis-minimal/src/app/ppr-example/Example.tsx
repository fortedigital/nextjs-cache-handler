export async function Example({
  searchParams,
}: {
  searchParams: Promise<{ characterId: string }>;
}) {
  const characterId = (await searchParams).characterId;
  let name: string;
  try {
    const characterResponse = await fetch(
      `https://api.sampleapis.com/futurama/characters/${characterId}`,
      {
        next: {
          revalidate: 86400, // 24 hours in seconds
          tags: ["futurama"],
        },
      }
    );
    const character = await characterResponse.json();
    name = character.name.first;
  } catch (error) {
    console.error("Error fetching character data:", error);
    return (
      <div>
        <span>An error occurred during fetch</span>
      </div>
    );
  }

  return (
    <div>
      <h1>Name: {name}</h1>
      <span>{new Date().toISOString()}</span>
    </div>
  );
}

export async function Skeleton() {
  return <div>Loading...</div>;
}
