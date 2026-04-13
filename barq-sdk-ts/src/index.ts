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

export type SearchConsistency = "primary" | "followers" | "any";

export interface SearchOptions {
    consistency?: SearchConsistency;
    allowFallback?: boolean;
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
