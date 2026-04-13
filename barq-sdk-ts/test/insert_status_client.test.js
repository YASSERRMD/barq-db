const test = require("node:test");
const assert = require("node:assert/strict");
const { GrpcClient } = require("../dist/index.js");

test("typescript grpc client maps insert status response", async () => {
    const client = new GrpcClient("127.0.0.1:50051");

    client.client = {
        getInsertStatus(request, metadata, callback) {
            callback(null, {
                requestId: request.requestId,
                state: "INSERT_STATUS_STATE_PROCESSING",
                errorMessage: "",
            });
        },
    };

    const status = await client.getInsertStatus("ingest-88");
    assert.deepEqual(status, {
        requestId: "ingest-88",
        state: "processing",
        errorMessage: undefined,
    });
});
