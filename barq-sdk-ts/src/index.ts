export interface BarqConfig {
    baseUrl: string;
    apiKey: string;
}

export interface CreateCollectionRequest {
    name: string;
    dimension: number;
    metric: "L2" | "Cosine" | "Dot";
    index?: any;
    text_fields?: Array<{ name: string; indexed: boolean; required: boolean }>;
}

export interface SearchResult {
    id: string | number | Record<string, string | number>;
    score: number;
    payload?: any;
}

export interface InsertOptions {
    waitForCommit?: boolean;
}

export type InsertState = "queued" | "processing" | "succeeded" | "failed";

export interface InsertStatus {
    requestId: string;
    state: InsertState;
    errorMessage?: string;
}

export type SearchConsistency = "primary" | "followers" | "any";

export interface SearchOptions {
    consistency?: SearchConsistency;
    allowFallback?: boolean;
}

export type MetricKind = "counter" | "gauge" | "histogram";

export interface MetricDefinition {
    name: string;
    kind: MetricKind;
    description: string;
    unit?: string;
    labels: string[];
}

export type SegmentState = "growing" | "sealed" | "compacted";

export interface TenantMemorySample {
    tenant: string;
    residentVectorMemoryBytes: number;
}

export interface CollectionMemorySample {
    tenant: string;
    collection: string;
    residentVectorMemoryBytes: number;
}

export interface CollectionWalSample {
    tenant: string;
    collection: string;
    entries: number;
    bytes: number;
}

export interface CollectionSegmentFileSample {
    tenant: string;
    collection: string;
    state: SegmentState;
    count: number;
}

export interface CollectionSegmentStateSample {
    tenant: string;
    collection: string;
    state: SegmentState;
    active: boolean;
}

export interface StorageMetrics {
    refreshCount: number;
    totalResidentVectorMemoryBytes: number;
    walAppendsTotal: number;
    walBytesWrittenTotal: number;
    compactionsTotal: number;
    tenantMemoryBytes: TenantMemorySample[];
    collectionMemoryBytes: CollectionMemorySample[];
    collectionWal: CollectionWalSample[];
    collectionSegmentFiles: CollectionSegmentFileSample[];
    collectionSegmentStates: CollectionSegmentStateSample[];
}

export interface Metrics {
    definitions: MetricDefinition[];
    storage: StorageMetrics;
}

export type ClusterMode = "single_node" | "routed_replication" | "consensus_backed";
export type WriteDurability = "node_local" | "primary_only" | "consensus_quorum";

export interface ClusterStatus {
    nodeId: string;
    mode: ClusterMode;
    writeDurability: WriteDurability;
    shardCount: number;
    nodeCount: number;
}

export type IndexState = "building" | "ready" | "stale";

export interface SegmentCount {
    state: SegmentState;
    count: number;
}

export interface CollectionSegmentInfo {
    tenant: string;
    collection: string;
    currentState: SegmentState;
    indexState: IndexState;
    segmentCounts: SegmentCount[];
}

export interface SegmentInfo {
    collections: CollectionSegmentInfo[];
}

function ensureSupportedApiVersion(): void {
    const version = process.env.API_VERSION ?? "v1";
    if (version !== "v1") {
        throw new Error(`unsupported API_VERSION: ${version}`);
    }
}

function compatDocumentId(value: string): Record<string, string | number> {
    const numeric = Number(value);
    if (Number.isInteger(numeric) && String(numeric) === value) {
        return { U64: numeric };
    }
    return { Str: value };
}

function grpcInsertOptions(options?: InsertOptions): { waitForCommit: boolean } | undefined {
    if (!options || options.waitForCommit === undefined) {
        return undefined;
    }

    return {
        waitForCommit: options.waitForCommit,
    };
}

function grpcSearchOptions(
    options?: SearchOptions,
): { consistency: "CONSISTENCY_UNSPECIFIED" | "CONSISTENCY_PRIMARY" | "CONSISTENCY_FOLLOWERS" | "CONSISTENCY_ANY"; allowFallback: boolean } | undefined {
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

function insertStateFromProto(
    state?: "INSERT_STATUS_STATE_UNSPECIFIED" | 0 | "INSERT_STATUS_STATE_QUEUED" | 1 | "INSERT_STATUS_STATE_PROCESSING" | 2 | "INSERT_STATUS_STATE_SUCCEEDED" | 3 | "INSERT_STATUS_STATE_FAILED" | 4,
): InsertState {
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

function enumSuffix(value: string | number | undefined, prefix: string): string {
    if (typeof value !== "string" || !value.startsWith(prefix)) {
        throw new Error(`invalid enum value: ${String(value)}`);
    }
    return value.slice(prefix.length).toLowerCase();
}

function metricKindFromProto(
    kind?: "METRIC_KIND_UNSPECIFIED" | "METRIC_KIND_COUNTER" | "METRIC_KIND_GAUGE" | "METRIC_KIND_HISTOGRAM" | number,
): MetricKind {
    const suffix = enumSuffix(kind, "METRIC_KIND_");
    if (suffix === "counter" || suffix === "gauge" || suffix === "histogram") {
        return suffix;
    }
    throw new Error(`invalid metric kind: ${String(kind)}`);
}

function segmentStateFromProto(
    state?: "SEGMENT_STATE_UNSPECIFIED" | "SEGMENT_STATE_GROWING" | "SEGMENT_STATE_SEALED" | "SEGMENT_STATE_COMPACTED" | number,
): SegmentState {
    const suffix = enumSuffix(state, "SEGMENT_STATE_");
    if (suffix === "growing" || suffix === "sealed" || suffix === "compacted") {
        return suffix;
    }
    throw new Error(`invalid segment state: ${String(state)}`);
}

function clusterModeFromProto(
    mode?: "CLUSTER_MODE_UNSPECIFIED" | "CLUSTER_MODE_SINGLE_NODE" | "CLUSTER_MODE_ROUTED_REPLICATION" | "CLUSTER_MODE_CONSENSUS_BACKED" | number,
): ClusterMode {
    const suffix = enumSuffix(mode, "CLUSTER_MODE_");
    if (suffix === "single_node" || suffix === "routed_replication" || suffix === "consensus_backed") {
        return suffix;
    }
    throw new Error(`invalid cluster mode: ${String(mode)}`);
}

function writeDurabilityFromProto(
    value?: "WRITE_DURABILITY_UNSPECIFIED" | "WRITE_DURABILITY_NODE_LOCAL" | "WRITE_DURABILITY_PRIMARY_ONLY" | "WRITE_DURABILITY_CONSENSUS_QUORUM" | number,
): WriteDurability {
    const suffix = enumSuffix(value, "WRITE_DURABILITY_");
    if (suffix === "node_local" || suffix === "primary_only" || suffix === "consensus_quorum") {
        return suffix;
    }
    throw new Error(`invalid write durability: ${String(value)}`);
}

function indexStateFromProto(
    value?: "INDEX_STATE_UNSPECIFIED" | "INDEX_STATE_BUILDING" | "INDEX_STATE_READY" | "INDEX_STATE_STALE" | number,
): IndexState {
    const suffix = enumSuffix(value, "INDEX_STATE_");
    if (suffix === "building" || suffix === "ready" || suffix === "stale") {
        return suffix;
    }
    throw new Error(`invalid index state: ${String(value)}`);
}

function metricsFromProto(response?: GetMetricsResponse__Output): Metrics {
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

function clusterStatusFromProto(response?: GetClusterStatusResponse__Output): ClusterStatus {
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

function segmentInfoFromProto(response?: GetSegmentInfoResponse__Output): SegmentInfo {
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

function grpcTargetFromBaseUrl(baseUrl: string): string {
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

export class BarqClient {
    private config: BarqConfig;
    private grpcCompat?: GrpcClient;

    constructor(config: BarqConfig) {
        this.config = config;
        if (this.config.baseUrl.endsWith("/")) {
            this.config.baseUrl = this.config.baseUrl.slice(0, -1);
        }
    }

    private async request(path: string, options: RequestInit = {}): Promise<any> {
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
        if (res.status === 204) return null;

        const text = await res.text();
        return text ? JSON.parse(text) : null;
    }

    async health(): Promise<boolean> {
        try {
            ensureSupportedApiVersion();
            return await this.grpc().status();
        } catch {
            return false;
        }
    }

    async createCollection(req: CreateCollectionRequest): Promise<void> {
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

    async getMetrics(): Promise<Metrics> {
        ensureSupportedApiVersion();
        return this.grpc().getMetrics();
    }

    async getClusterStatus(): Promise<ClusterStatus> {
        ensureSupportedApiVersion();
        return this.grpc().getClusterStatus();
    }

    async getSegmentInfo(collection?: string): Promise<SegmentInfo> {
        ensureSupportedApiVersion();
        return this.grpc().getSegmentInfo(collection);
    }

    collection(name: string) {
        return new Collection(this, name);
    }

    grpc(): GrpcClient {
        if (!this.grpcCompat) {
            this.grpcCompat = new GrpcClient(
                grpcTargetFromBaseUrl(this.config.baseUrl),
                undefined,
                this.config.apiKey,
            );
        }
        return this.grpcCompat;
    }
}

export class Collection {
    constructor(private client: BarqClient, private name: string) { }

    async insert(id: string | number, vector: number[], payload?: any, options?: InsertOptions): Promise<void> {
        ensureSupportedApiVersion();
        await this.client.grpc().insert(this.name, id, vector, payload ?? {}, options);
    }

    async insertAsync(id: string | number, vector: number[], payload?: any, options?: InsertOptions): Promise<string> {
        ensureSupportedApiVersion();
        return this.client.grpc().insertAsync(this.name, id, vector, payload ?? {}, options);
    }

    async getInsertStatus(requestId: string): Promise<InsertStatus> {
        ensureSupportedApiVersion();
        return this.client.grpc().getInsertStatus(requestId);
    }

    async search(
        vector?: number[],
        query?: string,
        topK: number = 10,
        filter?: any,
        options?: SearchOptions,
    ): Promise<SearchResult[]> {
        ensureSupportedApiVersion();
        if (vector && !query && !filter) {
            const results = await this.client.grpc().search(this.name, vector, topK, options);
            return results.map((result) => ({
                id: compatDocumentId(String(result.id)),
                score: result.score,
            })) as SearchResult[];
        }

        if (options) {
            throw new Error("advanced search options are only supported for vector-only gRPC search");
        }

        let path = `/collections/${this.name}/search`;
        if (vector && query) path += "/hybrid";
        else if (query) path += "/text";

        const body = {
            vector,
            query,
            top_k: topK,
            filter,
        };

        const res = await (this.client as any).request(path, {
            method: "POST",
            body: JSON.stringify(body),
        });
        return res.results;
    }
}

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as path from 'path';
import type { ProtoGrpcType } from './generated/barq';
import type { BarqClient as GeneratedBarqClient } from './generated/barq/Barq';
import type { GetClusterStatusResponse__Output } from './generated/barq/GetClusterStatusResponse';
import type { GetInsertStatusResponse__Output } from './generated/barq/GetInsertStatusResponse';
import type { GetMetricsResponse__Output } from './generated/barq/GetMetricsResponse';
import type { GetSegmentInfoResponse__Output } from './generated/barq/GetSegmentInfoResponse';
import type { InsertRequest } from './generated/barq/InsertRequest';
import type { SearchResponse__Output } from './generated/barq/SearchResponse';
import type { StatusResponse__Output } from './generated/barq/StatusResponse';

export class GrpcClient {
    private client: GeneratedBarqClient;
    private readonly metadata: grpc.Metadata;

    constructor(
        address: string,
        protoPath: string = path.resolve(__dirname, "../proto/barq.proto"),
        apiKey?: string,
        tenantId?: string,
    ) {
        const packageDefinition = protoLoader.loadSync(protoPath, {
            longs: String,
            enums: String,
            defaults: true,
            oneofs: true
        });
        const protoDescriptor = grpc.loadPackageDefinition(packageDefinition) as unknown as ProtoGrpcType;
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

    status(): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.client.status({}, this.metadata, (err, response?: StatusResponse__Output) => {
                if (err) return reject(err);
                resolve(Boolean(response?.ok));
            });
        });
    }

    health(): Promise<boolean> {
        return this.status();
    }

    getMetrics(): Promise<Metrics> {
        return new Promise((resolve, reject) => {
            this.client.getMetrics({}, this.metadata, (err, response?: GetMetricsResponse__Output) => {
                if (err) return reject(err);
                resolve(metricsFromProto(response));
            });
        });
    }

    getClusterStatus(): Promise<ClusterStatus> {
        return new Promise((resolve, reject) => {
            this.client.getClusterStatus({}, this.metadata, (err, response?: GetClusterStatusResponse__Output) => {
                if (err) return reject(err);
                resolve(clusterStatusFromProto(response));
            });
        });
    }

    getSegmentInfo(collection?: string): Promise<SegmentInfo> {
        return new Promise((resolve, reject) => {
            this.client.getSegmentInfo({ collection: collection ?? "" }, this.metadata, (err, response?: GetSegmentInfoResponse__Output) => {
                if (err) return reject(err);
                resolve(segmentInfoFromProto(response));
            });
        });
    }

    createCollection(name: string, dimension: number, metric: string = "L2"): Promise<void> {
        return new Promise((resolve, reject) => {
            this.client.createCollection({ name, dimension, metric }, this.metadata, (err) => {
                if (err) return reject(err);
                resolve();
            });
        });
    }

    insert(collection: string, id: string | number, vector: number[], payload: any = {}, options?: InsertOptions): Promise<void> {
        const request: InsertRequest = {
            collection,
            id: String(id),
            vector,
            payloadJson: JSON.stringify(payload),
            options: grpcInsertOptions(options),
        };

        return new Promise((resolve, reject) => {
            this.client.insert(request, this.metadata, (err) => {
                if (err) return reject(err);
                resolve();
            });
        });
    }

    insertAsync(collection: string, id: string | number, vector: number[], payload: any = {}, options?: InsertOptions): Promise<string> {
        const request: InsertRequest = {
            collection,
            id: String(id),
            vector,
            payloadJson: JSON.stringify(payload),
            options: grpcInsertOptions(options),
        };

        return new Promise((resolve, reject) => {
            this.client.insertAsync(request, this.metadata, (err, response?: { requestId?: string }) => {
                if (err) return reject(err);
                resolve(response?.requestId ?? "");
            });
        });
    }

    getInsertStatus(requestId: string): Promise<InsertStatus> {
        return new Promise((resolve, reject) => {
            this.client.getInsertStatus({ requestId }, this.metadata, (err, response?: GetInsertStatusResponse__Output) => {
                if (err) return reject(err);
                resolve({
                    requestId: response?.requestId ?? requestId,
                    state: insertStateFromProto(response?.state),
                    errorMessage: response?.errorMessage || undefined,
                });
            });
        });
    }

    insertDocument(
        collection: string,
        id: string | number,
        vector: number[],
        payload: any = {},
        options?: InsertOptions,
    ): Promise<void> {
        return this.insert(collection, id, vector, payload, options);
    }

    search(collection: string, vector: number[], topK: number = 10, options?: SearchOptions): Promise<SearchResult[]> {
        return new Promise((resolve, reject) => {
            this.client.search({
                collection,
                vector,
                topK,
                options: grpcSearchOptions(options),
            }, this.metadata, (err, response?: SearchResponse__Output) => {
                if (err) return reject(err);
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
