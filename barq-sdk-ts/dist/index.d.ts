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
export declare class BarqClient {
    private config;
    private grpcCompat?;
    constructor(config: BarqConfig);
    private request;
    health(): Promise<boolean>;
    createCollection(req: CreateCollectionRequest): Promise<void>;
    getMetrics(): Promise<any>;
    getClusterStatus(): Promise<any>;
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
    getMetrics(): Promise<any>;
    getClusterStatus(): Promise<any>;
    createCollection(name: string, dimension: number, metric?: string): Promise<void>;
    insert(collection: string, id: string | number, vector: number[], payload?: any, options?: InsertOptions): Promise<void>;
    insertAsync(collection: string, id: string | number, vector: number[], payload?: any, options?: InsertOptions): Promise<string>;
    getInsertStatus(requestId: string): Promise<InsertStatus>;
    insertDocument(collection: string, id: string | number, vector: number[], payload?: any, options?: InsertOptions): Promise<void>;
    search(collection: string, vector: number[], topK?: number, options?: SearchOptions): Promise<SearchResult[]>;
}
