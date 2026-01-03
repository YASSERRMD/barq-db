import { BarqClient, Document } from 'barq-sdk';

// Mock function to simulate CLIP embedding (512 dimensions)
function mockClipEmbedding(seed: string): number[] {
    const vec: number[] = [];
    for (let i = 0; i < 512; i++) {
        vec.push(Math.random());
    }
    return vec;
}

async function main() {
    console.log("Connecting to Barq DB...");
    const client = new BarqClient('http://localhost:8080');

    const collectionName = 'image_gallery';

    // Check availability
    try {
        await client.health();
        console.log("Barq DB is healthy.");
    } catch (e) {
        console.error("Failed to connect to Barq DB. Is it running?");
        return;
    }

    // Create Collection
    console.log(`Creating collection '${collectionName}'...`);
    try {
        await client.createCollection({
            name: collectionName,
            dimension: 512,
            metric: 'Cosine'
        });
    } catch (e) {
        console.log("Collection might already exist or failed:", e);
    }

    // Sample Images
    const images = [
        { id: "img_01", desc: "A sunset over the mountains", category: "nature" },
        { id: "img_02", desc: "A cat sleeping on a sofa", category: "animals" },
        { id: "img_03", desc: "Cyberpunk city street at night", category: "art" },
        { id: "img_04", desc: "Delicious pizza with pepperoni", category: "food" },
    ];

    // Ingest
    console.log("Ingesting images...");
    for (const img of images) {
        const vector = mockClipEmbedding(img.desc);
        await client.insert(collectionName, {
            id: img.id,
            vector: vector,
            payload: {
                description: img.desc,
                category: img.category
            }
        });
    }
    console.log("Ingestion complete.");

    // Search
    console.log("Searching for 'cat'...");
    const queryVector = mockClipEmbedding("cat query");

    const results = await client.search(collectionName, {
        vector: queryVector,
        topK: 2
    });

    console.log("Results:");
    results.forEach(hit => {
        console.log(`- ${hit.id} (Score: ${hit.score}) - ${hit.payload['description']}`);
    });
}

main().catch(console.error);
