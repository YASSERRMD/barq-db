export interface BarqConfig {
    baseUrl: string;
    apiKey: string;
}
export interface CreateCollectionRequest {
    name: string;
    dimension: number;
    metric: "L2" | "Cosine" | "Dot";
    index?: any;
    text_fields?: Array<{
        name: string;
        indexed: boolean;
        required: boolean;
    }>;
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
export declare class BarqClient {
    private config;
    private grpcCompat?;
    constructor(config: BarqConfig);
    private request;
    health(): Promise<boolean>;
    createCollection(req: CreateCollectionRequest): Promise<void>;
    getMetrics(): Promise<Metrics>;
    getClusterStatus(): Promise<ClusterStatus>;
    getSegmentInfo(collection?: string): Promise<SegmentInfo>;
    collection(name: string): Collection;
    grpc(): GrpcClient;
}
export declare class Collection {
    private client;
    private name;
    constructor(client: BarqClient, name: string);
    insert(id: string | number, vector: number[], payload?: any, options?: InsertOptions): Promise<void>;
    insertAsync(id: string | number, vector: number[], payload?: any, options?: InsertOptions): Promise<string>;
    getInsertStatus(requestId: string): Promise<InsertStatus>;
    search(vector?: number[], query?: string, topK?: number, filter?: any, options?: SearchOptions): Promise<SearchResult[]>;
}
export declare class GrpcClient {
    private client;
    private readonly metadata;
    constructor(address: string, protoPath?: string, apiKey?: string, tenantId?: string);
    status(): Promise<boolean>;
    health(): Promise<boolean>;
    getMetrics(): Promise<Metrics>;
    getClusterStatus(): Promise<ClusterStatus>;
    getSegmentInfo(collection?: string): Promise<SegmentInfo>;
    createCollection(name: string, dimension: number, metric?: string): Promise<void>;
    insert(collection: string, id: string | number, vector: number[], payload?: any, options?: InsertOptions): Promise<void>;
    insertAsync(collection: string, id: string | number, vector: number[], payload?: any, options?: InsertOptions): Promise<string>;
    getInsertStatus(requestId: string): Promise<InsertStatus>;
    insertDocument(collection: string, id: string | number, vector: number[], payload?: any, options?: InsertOptions): Promise<void>;
    search(collection: string, vector: number[], topK?: number, options?: SearchOptions): Promise<SearchResult[]>;
}
