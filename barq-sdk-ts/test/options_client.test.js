const test = require("node:test");
const assert = require("node:assert/strict");
const { BarqClient, GrpcClient } = require("../dist/index.js");

test("typescript grpc client encodes insert options", async () => {
    const client = new GrpcClient("127.0.0.1:50051");
    let seenRequest;

    client.client = {
        insert(request, metadata, callback) {
            seenRequest = request;
            callback(null, {});
        },
    };

    await client.insert("docs", "ts-doc", [1.0, 0.0], { sdk: "ts" }, { waitForCommit: false });

    assert.equal(seenRequest.options.waitForCommit, false);
});

test("typescript grpc client encodes search options with default fallback", async () => {
    const client = new GrpcClient("127.0.0.1:50051");
    let seenRequest;

    client.client = {
        search(request, metadata, callback) {
            seenRequest = request;
            callback(null, { results: [] });
        },
    };

    await client.search("docs", [1.0, 0.0], 1, { consistency: "followers" });

    assert.equal(seenRequest.options.consistency, "CONSISTENCY_FOLLOWERS");
    assert.equal(seenRequest.options.allowFallback, true);
});

test("typescript public client rejects options on rest fallback", async () => {
    const client = new BarqClient({
        baseUrl: "http://127.0.0.1:8080",
        apiKey: "test-key",
    });

    await assert.rejects(
        () => client.collection("docs").search([1.0, 0.0], "hello", 1, undefined, { allowFallback: false }),
        /advanced search options are only supported for vector-only gRPC search/,
    );
});
