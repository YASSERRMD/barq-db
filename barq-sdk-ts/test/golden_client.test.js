const test = require("node:test");
const assert = require("node:assert/strict");
const { BarqClient } = require("../dist/index.js");

test("typescript public client matches rest baseline", async () => {
    const client = new BarqClient({
        baseUrl: process.env.BARQ_BASE_URL || "http://127.0.0.1:8080",
        apiKey: process.env.BARQ_API_KEY,
    });
    const collection = process.env.BARQ_TEST_COLLECTION || "sdk-ts-golden";
    const expected = JSON.parse(process.env.BARQ_EXPECTED_RESULTS);

    assert.equal(await client.health(), true);
    await client.createCollection({
        name: collection,
        dimension: 2,
        metric: "Cosine",
    });

    const sdkCollection = client.collection(collection);
    await sdkCollection.insert("golden-primary", [1.0, 0.0], { sdk: "ts", mode: "golden" });
    await sdkCollection.insert("golden-secondary", [0.0, 1.0], { sdk: "ts", mode: "golden" });

    const results = await sdkCollection.search([1.0, 0.0], undefined, 2);
    assert.deepEqual(results, expected);
});
