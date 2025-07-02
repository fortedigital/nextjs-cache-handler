export default async function Home() {
  try {
    const characterResponse = await fetch(
      "https://api.sampleapis.com/futurama/characters/1",
      {
        next: {
          revalidate: 86400, // 24 hours in seconds
          tags: ["futurama"],
        },
      }
    );
    const character = await characterResponse.json();
    const name = character.name.first;
    return (
      <div>
        <h1>Name: {name}</h1>
        <span>{new Date().toISOString()}</span>
      </div>
    );
  } catch (error) {
    console.error("Error fetching character data:", error);
    return (
      <div>
        <span>An error occurred during fetch</span>
      </div>
    );
  }
}
