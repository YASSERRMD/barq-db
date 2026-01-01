# Barq SDK for TypeScript / Node.js

<p align="center">
  <a href="https://www.npmjs.com/package/barq-sdk-ts"><img src="https://img.shields.io/npm/v/barq-sdk-ts.svg" alt="npm"></a>
  <a href="https://github.com/YASSERRMD/barq-db/blob/main/LICENSE"><img src="https://img.shields.io/github/license/YASSERRMD/barq-db" alt="License"></a>
</p>

The official TypeScript/Node.js SDK for [Barq DB](https://github.com/YASSERRMD/barq-db) - a high-performance vector database built in Rust.

---

## Installation

```bash
npm install barq-sdk-ts
# or
yarn add barq-sdk-ts
# or
pnpm add barq-sdk-ts
```

---

## Quick Start

```typescript
import { BarqClient } from 'barq-sdk-ts';

const client = new BarqClient({
  baseUrl: 'http://localhost:8080',
  apiKey: 'your-api-key'
});

// Create collection
await client.createCollection({
  name: 'products',
  dimension: 384,
  metric: 'Cosine'
});

// Insert documents
const collection = client.collection('products');
await collection.insert(1, [0.1, 0.2, ...], { name: 'Widget', price: 29.99 });

// Search
const results = await collection.search([0.1, 0.2, ...], undefined, 10);
results.forEach(r => console.log(`${r.id}: ${r.score}`));
```

---

## HTTP Client

### Initialization

```typescript
import { BarqClient, BarqConfig } from 'barq-sdk-ts';

const config: BarqConfig = {
  baseUrl: 'http://localhost:8080',
  apiKey: 'your-api-key'
};

const client = new BarqClient(config);
```

### Health Check

```typescript
const isHealthy = await client.health();
console.log('Server healthy:', isHealthy);
```

### Create Collection

```typescript
// Basic collection
await client.createCollection({
  name: 'embeddings',
  dimension: 768,
  metric: 'L2'  // 'L2' | 'Cosine' | 'Dot'
});

// With text fields for hybrid search
await client.createCollection({
  name: 'articles',
  dimension: 384,
  metric: 'Cosine',
  text_fields: [
    { name: 'title', indexed: true, required: true },
    { name: 'content', indexed: true, required: false }
  ]
});

// With custom index
await client.createCollection({
  name: 'products',
  dimension: 256,
  metric: 'Cosine',
  index: { type: 'hnsw', m: 16, ef_construction: 200 }
});
```

### Collection Operations

```typescript
// Get collection reference
const collection = client.collection('products');

// Insert document
await collection.insert(
  'doc-001',                    // id: string | number
  [0.12, 0.34, ...],            // vector: number[]
  { name: 'Widget', price: 99 } // payload: any (optional)
);

// Batch insert
const documents = [
  { id: 1, vector: embedding1, payload: { text: 'First' } },
  { id: 2, vector: embedding2, payload: { text: 'Second' } },
];

for (const doc of documents) {
  await collection.insert(doc.id, doc.vector, doc.payload);
}
```

### Vector Search

```typescript
const results = await collection.search(
  queryVector,  // vector: number[]
  undefined,    // query: string (for text/hybrid)
  10            // topK: number
);

results.forEach(result => {
  console.log(`ID: ${result.id}`);
  console.log(`Score: ${result.score}`);
  console.log(`Payload:`, result.payload);
});
```

### Text Search (BM25)

```typescript
const results = await collection.search(
  undefined,                      // no vector
  'machine learning tutorial',    // text query
  10
);
```

### Hybrid Search

```typescript
const results = await collection.search(
  queryVector,        // vector embedding
  'neural networks',  // text query
  10
);
```

### Filtered Search

```typescript
const results = await collection.search(
  queryVector,
  undefined,
  10,
  {
    must: [
      { field: 'category', match: 'electronics' },
      { field: 'price', range: { lte: 100 } }
    ]
  }
);
```

---

## gRPC Client

For high-throughput applications:

```typescript
import { GrpcClient } from 'barq-sdk-ts';
import * as path from 'path';

const protoPath = path.join(__dirname, 'proto/barq.proto');
const client = new GrpcClient('localhost:50051', protoPath);

// Health check
const isHealthy = await client.health();

// Create collection
await client.createCollection('vectors', 384, 'L2');

// Insert document
await client.insertDocument('vectors', 'doc-001', [0.1, ...], { label: 'example' });

// Search
const results = await client.search('vectors', [0.1, ...], 10);
results.forEach(r => console.log(`${r.id}: ${r.score}`));
```

---

## API Reference

### Types

```typescript
interface BarqConfig {
  baseUrl: string;
  apiKey: string;
}

interface CreateCollectionRequest {
  name: string;
  dimension: number;
  metric: 'L2' | 'Cosine' | 'Dot';
  index?: any;
  text_fields?: Array<{ name: string; indexed: boolean; required: boolean }>;
}

interface SearchResult {
  id: string | number;
  score: number;
  payload?: any;
}
```

### `BarqClient`

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| `health()` | - | `Promise<boolean>` | Check server health |
| `createCollection()` | `CreateCollectionRequest` | `Promise<void>` | Create collection |
| `collection()` | `name: string` | `Collection` | Get collection reference |

### `Collection`

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| `insert()` | `id`, `vector`, `payload?` | `Promise<void>` | Insert document |
| `search()` | `vector?`, `query?`, `topK`, `filter?` | `Promise<SearchResult[]>` | Search |

### `GrpcClient`

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| `health()` | - | `Promise<boolean>` | Check health |
| `createCollection()` | `name`, `dimension`, `metric` | `Promise<void>` | Create collection |
| `insertDocument()` | `collection`, `id`, `vector`, `payload` | `Promise<void>` | Insert |
| `search()` | `collection`, `vector`, `topK` | `Promise<SearchResult[]>` | Search |

---

## Examples

### Express.js API

```typescript
import express from 'express';
import { BarqClient } from 'barq-sdk-ts';

const app = express();
const barq = new BarqClient({ baseUrl: 'http://localhost:8080', apiKey: 'key' });

app.post('/search', express.json(), async (req, res) => {
  const { vector, topK = 10 } = req.body;
  const results = await barq.collection('products').search(vector, undefined, topK);
  res.json(results);
});

app.listen(3000);
```

### With Embeddings (OpenAI)

```typescript
import { BarqClient } from 'barq-sdk-ts';
import OpenAI from 'openai';

const barq = new BarqClient({ baseUrl: 'http://localhost:8080', apiKey: 'barq-key' });
const openai = new OpenAI({ apiKey: 'openai-key' });

async function embed(text: string): Promise<number[]> {
  const response = await openai.embeddings.create({
    model: 'text-embedding-3-small',
    input: text
  });
  return response.data[0].embedding;
}

// Index document
const vector = await embed('Hello world');
await barq.collection('docs').insert('doc-1', vector, { text: 'Hello world' });

// Search
const queryVec = await embed('greeting');
const results = await barq.collection('docs').search(queryVec, undefined, 5);
```

---

## Requirements

- Node.js 16+
- TypeScript 4.5+ (if using TypeScript)

### Dependencies

- `@grpc/grpc-js` - gRPC client
- `@grpc/proto-loader` - Proto file loading

---

## Contributing

We welcome contributions! See the [main repository](https://github.com/YASSERRMD/barq-db) for guidelines.

### Areas for Improvement

- Streaming support for large result sets
- Automatic retry with exponential backoff
- Connection pooling
- Browser-compatible HTTP client
- Comprehensive test coverage

---

## License

MIT License - see [LICENSE](https://github.com/YASSERRMD/barq-db/blob/main/LICENSE)

---

<p align="center">
  <a href="https://github.com/YASSERRMD/barq-db">Barq DB</a> - Vector search at lightning speed
</p>
