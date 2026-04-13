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
    id: string | number;
    score: number;
    payload?: any;
}

export class BarqClient {
    private config: BarqConfig;

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
            await this.request("/health", { method: "GET" });
            return true;
        } catch {
            return false;
        }
    }

    async createCollection(req: CreateCollectionRequest): Promise<void> {
        await this.request("/collections", {
            method: "POST",
            body: JSON.stringify(req),
        });
    }

    collection(name: string) {
        return new Collection(this, name);
    }
}

export class Collection {
    constructor(private client: BarqClient, private name: string) { }

    async insert(id: string | number, vector: number[], payload?: any): Promise<void> {
        await (this.client as any).request(`/collections/${this.name}/documents`, {
            method: "POST",
            body: JSON.stringify({ id, vector, payload }),
        });
    }

    async search(
        vector?: number[],
        query?: string,
        topK: number = 10,
        filter?: any
    ): Promise<SearchResult[]> {
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

    constructor(address: string, protoPath: string = path.resolve(__dirname, "../proto/barq.proto")) {
        const packageDefinition = protoLoader.loadSync(protoPath, {
            longs: String,
            enums: String,
            defaults: true,
            oneofs: true
        });
        const protoDescriptor = grpc.loadPackageDefinition(packageDefinition) as unknown as ProtoGrpcType;
        const BarqService = protoDescriptor.barq.Barq;
        this.client = new BarqService(address, grpc.credentials.createInsecure());
    }

    status(): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.client.status({}, (err, response?: StatusResponse__Output) => {
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
            this.client.createCollection({ name, dimension, metric }, (err) => {
                if (err) return reject(err);
                resolve();
            });
        });
    }

    insert(collection: string, id: string | number, vector: number[], payload: any = {}): Promise<void> {
        const request: InsertRequest = {
            collection,
            id: String(id),
            vector,
            payloadJson: JSON.stringify(payload),
        };

        return new Promise((resolve, reject) => {
            this.client.insert(request, (err) => {
                if (err) return reject(err);
                resolve();
            });
        });
    }

    insertDocument(collection: string, id: string | number, vector: number[], payload: any = {}): Promise<void> {
        return this.insert(collection, id, vector, payload);
    }

    search(collection: string, vector: number[], topK: number = 10): Promise<SearchResult[]> {
        return new Promise((resolve, reject) => {
            this.client.search({
                collection,
                vector,
                topK
            }, (err, response?: SearchResponse__Output) => {
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
