const test = require("node:test");
const assert = require("node:assert/strict");
const { BarqClient } = require("../dist/index.js");

test("typescript public client insert and search with options", async () => {
    const client = new BarqClient({
        baseUrl: process.env.BARQ_BASE_URL || "http://127.0.0.1:8080",
        apiKey: process.env.BARQ_API_KEY ?? "",
    });
    const collection = process.env.BARQ_TEST_COLLECTION;

    assert.ok(collection, "BARQ_TEST_COLLECTION must be set");
    assert.equal(await client.health(), true);

    await client.createCollection({
        name: collection,
        dimension: 2,
        metric: "Cosine",
    });

    const sdkCollection = client.collection(collection);
    await sdkCollection.insert(
        "ts-options-doc",
        [1.0, 0.0],
        { sdk: "ts", mode: "options" },
        { waitForCommit: true },
    );

    const results = await sdkCollection.search(
        [1.0, 0.0],
        undefined,
        1,
        undefined,
        { consistency: "primary", allowFallback: true },
    );

    assert.equal(results.length, 1);
    assert.deepEqual(results[0].id, { Str: "ts-options-doc" });
});
