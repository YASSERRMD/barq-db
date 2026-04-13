const test = require("node:test");
const assert = require("node:assert/strict");
const { GrpcClient } = require("../dist/index.js");

test("typescript grpc client maps metrics to structured model", async () => {
    const client = new GrpcClient("127.0.0.1:50051");

    client.client = {
        getMetrics(request, metadata, callback) {
            callback(null, {
                definitions: [
                    {
                        name: "ingestion_queue_size",
                        kind: "METRIC_KIND_GAUGE",
                        description: "queue depth",
                        labels: ["tenant"],
                    },
                ],
                storage: {
                    refreshCount: 1,
                    totalResidentVectorMemoryBytes: 64,
                    walAppendsTotal: 2,
                    walBytesWrittenTotal: 32,
                    compactionsTotal: 0,
                    tenantMemoryBytes: [],
                    collectionMemoryBytes: [],
                    collectionWal: [{ tenant: "default", collection: "docs", entries: 2, bytes: 32 }],
                    collectionSegmentFiles: [{ tenant: "default", collection: "docs", state: "SEGMENT_STATE_SEALED", count: 1 }],
                    collectionSegmentStates: [{ tenant: "default", collection: "docs", state: "SEGMENT_STATE_GROWING", active: true }],
                },
            });
        },
    };

    const metrics = await client.getMetrics();
    assert.equal(metrics.definitions[0].kind, "gauge");
    assert.equal(metrics.storage.collectionSegmentStates[0].state, "growing");
});

test("typescript grpc client validates cluster status enums", async () => {
    const client = new GrpcClient("127.0.0.1:50051");

    client.client = {
        getClusterStatus(request, metadata, callback) {
            callback(null, {
                nodeId: "local",
                mode: "CLUSTER_MODE_SINGLE_NODE",
                writeDurability: "WRITE_DURABILITY_NODE_LOCAL",
                shardCount: 1,
                nodeCount: 1,
            });
        },
    };

    const status = await client.getClusterStatus();
    assert.equal(status.mode, "single_node");
    assert.equal(status.writeDurability, "node_local");
});

test("typescript grpc client rejects unspecified cluster mode", async () => {
    const client = new GrpcClient("127.0.0.1:50051");

    client.client = {
        getClusterStatus(request, metadata, callback) {
            callback(null, {
                nodeId: "local",
                mode: "CLUSTER_MODE_UNSPECIFIED",
                writeDurability: "WRITE_DURABILITY_NODE_LOCAL",
                shardCount: 1,
                nodeCount: 1,
            });
        },
    };

    await assert.rejects(() => client.getClusterStatus(), /invalid cluster mode/);
});

test("typescript grpc client maps segment info to structured model", async () => {
    const client = new GrpcClient("127.0.0.1:50051");

    client.client = {
        getSegmentInfo(request, metadata, callback) {
            callback(null, {
                collections: [
                    {
                        tenant: "default",
                        collection: "docs",
                        currentState: "SEGMENT_STATE_GROWING",
                        indexState: "INDEX_STATE_READY",
                        segmentCounts: [{ state: "SEGMENT_STATE_SEALED", count: 1 }],
                    },
                ],
            });
        },
    };

    const info = await client.getSegmentInfo("docs");
    assert.equal(info.collections[0].collection, "docs");
    assert.equal(info.collections[0].currentState, "growing");
    assert.equal(info.collections[0].segmentCounts[0].state, "sealed");
});
