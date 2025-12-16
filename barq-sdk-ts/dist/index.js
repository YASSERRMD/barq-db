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
            await this.request("/health", { method: "GET" });
            return true;
        }
        catch {
            return false;
        }
    }
    async createCollection(req) {
        await this.request("/collections", {
            method: "POST",
            body: JSON.stringify(req),
        });
    }
    collection(name) {
        return new Collection(this, name);
    }
}
exports.BarqClient = BarqClient;
class Collection {
    constructor(client, name) {
        this.client = client;
        this.name = name;
    }
    async insert(id, vector, payload) {
        await this.client.request(`/collections/${this.name}/documents`, {
            method: "POST",
            body: JSON.stringify({ id, vector, payload }),
        });
    }
    async search(vector, query, topK = 10, filter) {
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
class GrpcClient {
    constructor(address, protoPath) {
        // Resolve proto path relative to current execution or package
        // For SDK, user might pass absolute path or we can try to resolve it
        this.packageDefinition = protoLoader.loadSync(protoPath, {
            keepCase: true,
            longs: String,
            enums: String,
            defaults: true,
            oneofs: true
        });
        this.protoDescriptor = grpc.loadPackageDefinition(this.packageDefinition);
        const BarqService = this.protoDescriptor.barq.Barq;
        this.client = new BarqService(address, grpc.credentials.createInsecure());
    }
    health() {
        return new Promise((resolve, reject) => {
            this.client.Health({}, (err, response) => {
                if (err)
                    return reject(err);
                resolve(response.ok);
            });
        });
    }
    createCollection(name, dimension, metric = "L2") {
        return new Promise((resolve, reject) => {
            this.client.CreateCollection({ name, dimension, metric }, (err, response) => {
                if (err)
                    return reject(err);
                resolve();
            });
        });
    }
    insertDocument(collection, id, vector, payload = {}) {
        const payloadJson = JSON.stringify(payload);
        return new Promise((resolve, reject) => {
            this.client.InsertDocument({
                collection,
                id: String(id),
                vector,
                payload_json: payloadJson
            }, (err, response) => {
                if (err)
                    return reject(err);
                resolve();
            });
        });
    }
    search(collection, vector, topK = 10) {
        return new Promise((resolve, reject) => {
            this.client.Search({
                collection,
                vector,
                top_k: topK
            }, (err, response) => {
                if (err)
                    return reject(err);
                const results = response.results.map((r) => ({
                    id: r.id,
                    score: r.score,
                    payload: JSON.parse(r.payload_json || "{}")
                }));
                resolve(results);
            });
        });
    }
}
exports.GrpcClient = GrpcClient;
