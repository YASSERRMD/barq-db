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
    id: string | number;
    score: number;
    payload?: any;
}
export declare class BarqClient {
    private config;
    constructor(config: BarqConfig);
    private request;
    health(): Promise<boolean>;
    createCollection(req: CreateCollectionRequest): Promise<void>;
    collection(name: string): Collection;
}
export declare class Collection {
    private client;
    private name;
    constructor(client: BarqClient, name: string);
    insert(id: string | number, vector: number[], payload?: any): Promise<void>;
    search(vector?: number[], query?: string, topK?: number, filter?: any): Promise<SearchResult[]>;
}
export declare class GrpcClient {
    private client;
    private packageDefinition;
    private protoDescriptor;
    constructor(address: string, protoPath: string);
    health(): Promise<boolean>;
    createCollection(name: string, dimension: number, metric?: string): Promise<void>;
    insertDocument(collection: string, id: string | number, vector: number[], payload?: any): Promise<void>;
    search(collection: string, vector: number[], topK?: number): Promise<SearchResult[]>;
}
