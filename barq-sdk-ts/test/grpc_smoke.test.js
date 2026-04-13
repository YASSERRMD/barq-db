const test = require("node:test");
const assert = require("node:assert/strict");
const { GrpcClient } = require("../dist/index.js");

test("typescript grpc smoke", async () => {
    const target = process.env.BARQ_GRPC_ADDR;
    const collection = process.env.BARQ_TEST_COLLECTION;

    assert.ok(target, "BARQ_GRPC_ADDR must be set");
    assert.ok(collection, "BARQ_TEST_COLLECTION must be set");

    const client = new GrpcClient(target);

    assert.equal(await client.status(), true);
    await client.createCollection(collection, 2, "Cosine");
    await client.insert(collection, "ts-doc", [1.0, 0.0], { sdk: "ts", mode: "grpc" });

    const results = await client.search(collection, [1.0, 0.0], 1);
    assert.equal(results.length, 1);
    assert.equal(results[0].id, "ts-doc");
});
