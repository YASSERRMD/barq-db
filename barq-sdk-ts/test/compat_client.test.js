const test = require("node:test");
const assert = require("node:assert/strict");
const { BarqClient } = require("../dist/index.js");

test("typescript public client remains compatible", async () => {
    const client = new BarqClient({
        baseUrl: process.env.BARQ_BASE_URL || "http://127.0.0.1:8080",
        apiKey: process.env.BARQ_API_KEY,
    });

    assert.equal(await client.health(), true);
    await client.createCollection({
        name: "sdk-ts-compat",
        dimension: 2,
        metric: "Cosine",
    });

    const collection = client.collection("sdk-ts-compat");
    await collection.insert("ts-doc", [1.0, 0.0], { sdk: "ts", mode: "compat" });

    const results = await collection.search([1.0, 0.0], undefined, 1);
    assert.equal(results.length, 1);
    assert.deepEqual(results[0].id, { Str: "ts-doc" });
});
