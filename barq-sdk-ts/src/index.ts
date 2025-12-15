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
        elif(query) path += "/text";

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
