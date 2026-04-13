"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.GrpcClient = exports.Collection = exports.BarqClient = void 0;
function ensureSupportedApiVersion() {
    const version = process.env.API_VERSION ?? "v1";
    if (version !== "v1") {
        throw new Error(`unsupported API_VERSION: ${version}`);
    }
}
function compatDocumentId(value) {
    const numeric = Number(value);
    if (Number.isInteger(numeric) && String(numeric) === value) {
        return { U64: numeric };
    }
    return { Str: value };
}
function grpcInsertOptions(options) {
    if (!options || options.waitForCommit === undefined) {
        return undefined;
    }
    return {
        waitForCommit: options.waitForCommit,
    };
}
function grpcSearchOptions(options) {
    if (!options || (options.consistency === undefined && options.allowFallback === undefined)) {
        return undefined;
    }
    const consistency = (() => {
        switch (options.consistency) {
            case "primary":
                return "CONSISTENCY_PRIMARY";
            case "followers":
                return "CONSISTENCY_FOLLOWERS";
            case "any":
                return "CONSISTENCY_ANY";
            default:
                return "CONSISTENCY_UNSPECIFIED";
        }
    })();
    return {
        consistency,
        allowFallback: options.allowFallback ?? true,
    };
}
function insertStateFromProto(state) {
    switch (state) {
        case 2:
        case "INSERT_STATUS_STATE_PROCESSING":
            return "processing";
        case 3:
        case "INSERT_STATUS_STATE_SUCCEEDED":
            return "succeeded";
        case 4:
        case "INSERT_STATUS_STATE_FAILED":
            return "failed";
        case 1:
        case "INSERT_STATUS_STATE_QUEUED":
        case 0:
        case "INSERT_STATUS_STATE_UNSPECIFIED":
        default:
            return "queued";
    }
}
function enumSuffix(value, prefix) {
    if (typeof value !== "string" || !value.startsWith(prefix)) {
        throw new Error(`invalid enum value: ${String(value)}`);
    }
    return value.slice(prefix.length).toLowerCase();
}
function metricKindFromProto(kind) {
    const suffix = enumSuffix(kind, "METRIC_KIND_");
    if (suffix === "counter" || suffix === "gauge" || suffix === "histogram") {
        return suffix;
    }
    throw new Error(`invalid metric kind: ${String(kind)}`);
}
function segmentStateFromProto(state) {
    const suffix = enumSuffix(state, "SEGMENT_STATE_");
    if (suffix === "growing" || suffix === "sealed" || suffix === "compacted") {
        return suffix;
    }
    throw new Error(`invalid segment state: ${String(state)}`);
}
function clusterModeFromProto(mode) {
    const suffix = enumSuffix(mode, "CLUSTER_MODE_");
    if (suffix === "single_node" || suffix === "routed_replication" || suffix === "consensus_backed") {
        return suffix;
    }
    throw new Error(`invalid cluster mode: ${String(mode)}`);
}
function writeDurabilityFromProto(value) {
    const suffix = enumSuffix(value, "WRITE_DURABILITY_");
    if (suffix === "node_local" || suffix === "primary_only" || suffix === "consensus_quorum") {
        return suffix;
    }
    throw new Error(`invalid write durability: ${String(value)}`);
}
function indexStateFromProto(value) {
    const suffix = enumSuffix(value, "INDEX_STATE_");
    if (suffix === "building" || suffix === "ready" || suffix === "stale") {
        return suffix;
    }
    throw new Error(`invalid index state: ${String(value)}`);
}
function metricsFromProto(response) {
    if (!response?.storage) {
        throw new Error("metrics response missing storage payload");
    }
    return {
        definitions: (response.definitions ?? []).map((definition) => ({
            name: definition.name ?? "",
            kind: metricKindFromProto(definition.kind),
            description: definition.description ?? "",
            unit: definition.unit || undefined,
            labels: definition.labels ?? [],
        })),
        storage: {
            refreshCount: Number(response.storage.refreshCount ?? 0),
            totalResidentVectorMemoryBytes: Number(response.storage.totalResidentVectorMemoryBytes ?? 0),
            walAppendsTotal: Number(response.storage.walAppendsTotal ?? 0),
            walBytesWrittenTotal: Number(response.storage.walBytesWrittenTotal ?? 0),
            compactionsTotal: Number(response.storage.compactionsTotal ?? 0),
            tenantMemoryBytes: (response.storage.tenantMemoryBytes ?? []).map((sample) => ({
                tenant: sample.tenant ?? "",
                residentVectorMemoryBytes: Number(sample.residentVectorMemoryBytes ?? 0),
            })),
            collectionMemoryBytes: (response.storage.collectionMemoryBytes ?? []).map((sample) => ({
                tenant: sample.tenant ?? "",
                collection: sample.collection ?? "",
                residentVectorMemoryBytes: Number(sample.residentVectorMemoryBytes ?? 0),
            })),
            collectionWal: (response.storage.collectionWal ?? []).map((sample) => ({
                tenant: sample.tenant ?? "",
                collection: sample.collection ?? "",
                entries: Number(sample.entries ?? 0),
                bytes: Number(sample.bytes ?? 0),
            })),
            collectionSegmentFiles: (response.storage.collectionSegmentFiles ?? []).map((sample) => ({
                tenant: sample.tenant ?? "",
                collection: sample.collection ?? "",
                state: segmentStateFromProto(sample.state),
                count: Number(sample.count ?? 0),
            })),
            collectionSegmentStates: (response.storage.collectionSegmentStates ?? []).map((sample) => ({
                tenant: sample.tenant ?? "",
                collection: sample.collection ?? "",
                state: segmentStateFromProto(sample.state),
                active: Boolean(sample.active),
            })),
        },
    };
}
function clusterStatusFromProto(response) {
    if (!response) {
        throw new Error("cluster status response missing payload");
    }
    return {
        nodeId: response.nodeId ?? "",
        mode: clusterModeFromProto(response.mode),
        writeDurability: writeDurabilityFromProto(response.writeDurability),
        shardCount: Number(response.shardCount ?? 0),
        nodeCount: Number(response.nodeCount ?? 0),
    };
}
function segmentInfoFromProto(response) {
    return {
        collections: (response?.collections ?? []).map((collection) => ({
            tenant: collection.tenant ?? "",
            collection: collection.collection ?? "",
            currentState: segmentStateFromProto(collection.currentState),
            indexState: indexStateFromProto(collection.indexState),
            segmentCounts: (collection.segmentCounts ?? []).map((count) => ({
                state: segmentStateFromProto(count.state),
                count: Number(count.count ?? 0),
            })),
        })),
    };
}
function grpcTargetFromBaseUrl(baseUrl) {
    const override = process.env.BARQ_GRPC_ADDR;
    if (override) {
        if (override.includes("://")) {
            return new URL(override).host;
        }
        return override;
    }
    const url = new URL(baseUrl);
    return `${url.hostname}:50051`;
}
class BarqClient {
    constructor(config) {
        this.config = config;
        if (this.config.baseUrl.endsWith("/")) {
            this.config.baseUrl = this.config.baseUrl.slice(0, -1);
        }
    }
    async request(path, options = {}) {
        const url = `${this.config.baseUrl}${path}`;
        const headers = {
            "Content-Type": "application/json",
            "x-api-key": this.config.apiKey,
            ...options.headers,
        };
        const res = await fetch(url, { ...options, headers });
        if (!res.ok) {
            const text = await res.text();
            throw new Error(`Barq API Error ${res.status}: ${text}`);
        }
        // Handle 204 No Content
        if (res.status === 204)
            return null;
        const text = await res.text();
        return text ? JSON.parse(text) : null;
    }
    async health() {
        try {
            ensureSupportedApiVersion();
            return await this.grpc().status();
        }
        catch {
            return false;
        }
    }
    async createCollection(req) {
        ensureSupportedApiVersion();
        if (!req.index && !(req.text_fields && req.text_fields.length > 0)) {
            await this.grpc().createCollection(req.name, req.dimension, req.metric);
            return;
        }
        await this.request("/collections", {
            method: "POST",
            body: JSON.stringify(req),
        });
    }
    async getMetrics() {
        ensureSupportedApiVersion();
        return this.grpc().getMetrics();
    }
    async getClusterStatus() {
        ensureSupportedApiVersion();
        return this.grpc().getClusterStatus();
    }
    async getSegmentInfo(collection) {
        ensureSupportedApiVersion();
        return this.grpc().getSegmentInfo(collection);
    }
    collection(name) {
        return new Collection(this, name);
    }
    grpc() {
        if (!this.grpcCompat) {
            this.grpcCompat = new GrpcClient(grpcTargetFromBaseUrl(this.config.baseUrl), undefined, this.config.apiKey);
        }
        return this.grpcCompat;
    }
}
exports.BarqClient = BarqClient;
class Collection {
    constructor(client, name) {
        this.client = client;
        this.name = name;
    }
    async insert(id, vector, payload, options) {
        ensureSupportedApiVersion();
        await this.client.grpc().insert(this.name, id, vector, payload ?? {}, options);
    }
    async insertAsync(id, vector, payload, options) {
        ensureSupportedApiVersion();
        return this.client.grpc().insertAsync(this.name, id, vector, payload ?? {}, options);
    }
    async getInsertStatus(requestId) {
        ensureSupportedApiVersion();
        return this.client.grpc().getInsertStatus(requestId);
    }
    async search(vector, query, topK = 10, filter, options) {
        ensureSupportedApiVersion();
        if (vector && !query && !filter) {
            const results = await this.client.grpc().search(this.name, vector, topK, options);
            return results.map((result) => ({
                id: compatDocumentId(String(result.id)),
                score: result.score,
            }));
        }
        if (options) {
            throw new Error("advanced search options are only supported for vector-only gRPC search");
        }
        let path = `/collections/${this.name}/search`;
        if (vector && query)
            path += "/hybrid";
        else if (query)
            path += "/text";
        const body = {
            vector,
            query,
            top_k: topK,
            filter,
        };
        const res = await this.client.request(path, {
            method: "POST",
            body: JSON.stringify(body),
        });
        return res.results;
    }
}
exports.Collection = Collection;
const grpc = __importStar(require("@grpc/grpc-js"));
const protoLoader = __importStar(require("@grpc/proto-loader"));
const path = __importStar(require("path"));
class GrpcClient {
    constructor(address, protoPath = path.resolve(__dirname, "../proto/barq.proto"), apiKey, tenantId) {
        const packageDefinition = protoLoader.loadSync(protoPath, {
            longs: String,
            enums: String,
            defaults: true,
            oneofs: true
        });
        const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
        const BarqService = protoDescriptor.barq.Barq;
        this.client = new BarqService(address, grpc.credentials.createInsecure());
        this.metadata = new grpc.Metadata();
        if (apiKey) {
            this.metadata.set("x-api-key", apiKey);
        }
        if (tenantId) {
            this.metadata.set("x-tenant-id", tenantId);
        }
    }
    status() {
        return new Promise((resolve, reject) => {
            this.client.status({}, this.metadata, (err, response) => {
                if (err)
                    return reject(err);
                resolve(Boolean(response?.ok));
            });
        });
    }
    health() {
        return this.status();
    }
    getMetrics() {
        return new Promise((resolve, reject) => {
            this.client.getMetrics({}, this.metadata, (err, response) => {
                if (err)
                    return reject(err);
                resolve(metricsFromProto(response));
            });
        });
    }
    getClusterStatus() {
        return new Promise((resolve, reject) => {
            this.client.getClusterStatus({}, this.metadata, (err, response) => {
                if (err)
                    return reject(err);
                resolve(clusterStatusFromProto(response));
            });
        });
    }
    getSegmentInfo(collection) {
        return new Promise((resolve, reject) => {
            this.client.getSegmentInfo({ collection: collection ?? "" }, this.metadata, (err, response) => {
                if (err)
                    return reject(err);
                resolve(segmentInfoFromProto(response));
            });
        });
    }
    createCollection(name, dimension, metric = "L2") {
        return new Promise((resolve, reject) => {
            this.client.createCollection({ name, dimension, metric }, this.metadata, (err) => {
                if (err)
                    return reject(err);
                resolve();
            });
        });
    }
    insert(collection, id, vector, payload = {}, options) {
        const request = {
            collection,
            id: String(id),
            vector,
            payloadJson: JSON.stringify(payload),
            options: grpcInsertOptions(options),
        };
        return new Promise((resolve, reject) => {
            this.client.insert(request, this.metadata, (err) => {
                if (err)
                    return reject(err);
                resolve();
            });
        });
    }
    insertAsync(collection, id, vector, payload = {}, options) {
        const request = {
            collection,
            id: String(id),
            vector,
            payloadJson: JSON.stringify(payload),
            options: grpcInsertOptions(options),
        };
        return new Promise((resolve, reject) => {
            this.client.insertAsync(request, this.metadata, (err, response) => {
                if (err)
                    return reject(err);
                resolve(response?.requestId ?? "");
            });
        });
    }
    getInsertStatus(requestId) {
        return new Promise((resolve, reject) => {
            this.client.getInsertStatus({ requestId }, this.metadata, (err, response) => {
                if (err)
                    return reject(err);
                resolve({
                    requestId: response?.requestId ?? requestId,
                    state: insertStateFromProto(response?.state),
                    errorMessage: response?.errorMessage || undefined,
                });
            });
        });
    }
    insertDocument(collection, id, vector, payload = {}, options) {
        return this.insert(collection, id, vector, payload, options);
    }
    search(collection, vector, topK = 10, options) {
        return new Promise((resolve, reject) => {
            this.client.search({
                collection,
                vector,
                topK,
                options: grpcSearchOptions(options),
            }, this.metadata, (err, response) => {
                if (err)
                    return reject(err);
                const results = (response?.results ?? []).map((r) => ({
                    id: r.id ?? "",
                    score: r.score ?? 0,
                    payload: JSON.parse(r.payloadJson || "{}")
                }));
                resolve(results);
            });
        });
    }
}
exports.GrpcClient = GrpcClient;
