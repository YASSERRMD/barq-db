
const { GrpcClient } = require('../barq-sdk-ts/dist/index');
const path = require('path');

async function main() {
    // Path to proto file (assuming running from project root)
    const protoPath = path.join(__dirname, '../barq-sdk-ts/proto/barq.proto');

    console.log("Connecting to Barq gRPC at localhost:50051...");
    const client = new GrpcClient('localhost:50051', protoPath);

    try {
        const health = await client.health();
        console.log("Health check:", health);

        console.log("Creating collection 'grpc_ts_rag'...");
        await client.createCollection('grpc_ts_rag', 2, 'Cosine');

        console.log("Inserting document...");
        await client.insertDocument('grpc_ts_rag', 'doc_ts_1', [0.9, 0.1], { lang: 'ts', type: 'grpc' });

        // Wait for consistency
        await new Promise(r => setTimeout(r, 500));

        console.log("Searching...");
        const results = await client.search('grpc_ts_rag', [0.9, 0.1], 3);
        console.log("Found results:");
        console.log(results);

    } catch (e) {
        console.error("Error:", e);
    }
}

main();
