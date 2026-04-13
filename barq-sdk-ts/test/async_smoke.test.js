const test = require("node:test");
const assert = require("node:assert/strict");
const { BarqClient } = require("../dist/index.js");

test("typescript async insert returns request id", async () => {
    const client = new BarqClient({
        baseUrl: process.env.BARQ_BASE_URL || "http://127.0.0.1:8080",
        apiKey: process.env.BARQ_API_KEY ?? "",
    });
    const collection = process.env.BARQ_TEST_COLLECTION;

    assert.ok(collection, "BARQ_TEST_COLLECTION must be set");
    await client.createCollection({
        name: collection,
        dimension: 2,
        metric: "Cosine",
    });

    const requestId = await client
        .collection(collection)
        .insertAsync("ts-async-doc", [1.0, 0.0], { sdk: "ts", mode: "async" });

    assert.match(requestId, /^ingest-/);
});
