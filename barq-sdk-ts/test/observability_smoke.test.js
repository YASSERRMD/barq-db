const test = require("node:test");
const assert = require("node:assert/strict");
const { BarqClient } = require("../dist/index.js");

test("typescript public client reads metrics and cluster status", async () => {
    const client = new BarqClient({
        baseUrl: process.env.BARQ_BASE_URL || "http://127.0.0.1:8080",
        apiKey: process.env.BARQ_API_KEY ?? "",
    });
    const collection = process.env.BARQ_TEST_COLLECTION;

    const metrics = await client.getMetrics();
    assert.ok(metrics.definitions.length > 0);
    assert.ok(Number(metrics.storage.totalResidentVectorMemoryBytes) > 0);
    assert.ok(metrics.storage.collectionWal.some((sample) => sample.collection === collection));

    const status = await client.getClusterStatus();
    assert.equal(status.mode, "single_node");
    assert.equal(Number(status.nodeCount), 1);
    assert.equal(Number(status.shardCount), 1);

    const segmentInfo = await client.getSegmentInfo(collection);
    assert.equal(segmentInfo.collections[0].collection, collection);
});
