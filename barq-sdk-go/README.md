# Barq SDK for Go

<p align="center">
  <a href="https://pkg.go.dev/github.com/YASSERRMD/barq-db/barq-sdk-go"><img src="https://pkg.go.dev/badge/github.com/YASSERRMD/barq-db/barq-sdk-go.svg" alt="Go Reference"></a>
  <a href="https://github.com/YASSERRMD/barq-db/blob/main/LICENSE"><img src="https://img.shields.io/github/license/YASSERRMD/barq-db" alt="License"></a>
</p>

The official Go SDK for [Barq DB](https://github.com/YASSERRMD/barq-db) - a high-performance vector database built in Rust.

---

## Installation

```bash
go get github.com/YASSERRMD/barq-db/barq-sdk-go
```

---

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"log"

	barq "github.com/YASSERRMD/barq-db/barq-sdk-go"
)

func main() {
	ctx := context.Background()

	// Initialize client
	client := barq.NewClient(barq.Config{
		BaseURL: "http://localhost:8080",
		APIKey:  "your-api-key",
	})

	// Create collection
	err := client.CreateCollection(ctx, barq.CreateCollectionRequest{
		Name:      "products",
		Dimension: 384,
		Metric:    "Cosine",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Insert document
	vector := make([]float32, 384)
	for i := range vector {
		vector[i] = 0.1
	}

	err = client.Insert(ctx, "products", barq.InsertRequest{
		ID:     1,
		Vector: vector,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Search
	results, err := client.Search(ctx, "products", barq.SearchRequest{
		Vector: vector,
		TopK:   10,
	})
	if err != nil {
		log.Fatal(err)
	}

	for _, r := range results {
		fmt.Printf("ID: %v, Score: %.4f\n", r.ID, r.Score)
	}
}
```

---

## HTTP Client

### Initialization

```go
import barq "github.com/YASSERRMD/barq-db/barq-sdk-go"

client := barq.NewClient(barq.Config{
	BaseURL: "http://localhost:8080",
	APIKey:  "your-api-key",
})
```

### Create Collection

```go
// Basic collection
err := client.CreateCollection(ctx, barq.CreateCollectionRequest{
	Name:      "embeddings",
	Dimension: 768,
	Metric:    "L2",  // "L2", "Cosine", "Dot"
})

// With text fields for hybrid search
err := client.CreateCollection(ctx, barq.CreateCollectionRequest{
	Name:      "articles",
	Dimension: 384,
	Metric:    "Cosine",
	TextFields: []barq.TextField{
		{Name: "title", Indexed: true, Required: true},
		{Name: "content", Indexed: true, Required: false},
	},
})

// With custom index
err := client.CreateCollection(ctx, barq.CreateCollectionRequest{
	Name:      "products",
	Dimension: 256,
	Metric:    "Cosine",
	Index:     map[string]interface{}{"type": "hnsw", "m": 16},
})
```

### Insert Documents

```go
import "encoding/json"

// Simple insert
err := client.Insert(ctx, "products", barq.InsertRequest{
	ID:     "doc-001",
	Vector: embedding,
})

// With payload
payload, _ := json.Marshal(map[string]interface{}{
	"name":  "Widget",
	"price": 29.99,
})

err := client.Insert(ctx, "products", barq.InsertRequest{
	ID:      1,
	Vector:  embedding,
	Payload: payload,
})

// Batch insert
documents := []struct {
	ID      interface{}
	Vector  []float32
	Payload json.RawMessage
}{
	{ID: 1, Vector: vec1, Payload: payload1},
	{ID: 2, Vector: vec2, Payload: payload2},
}

for _, doc := range documents {
	client.Insert(ctx, "products", barq.InsertRequest{
		ID:      doc.ID,
		Vector:  doc.Vector,
		Payload: doc.Payload,
	})
}
```

### Vector Search

```go
results, err := client.Search(ctx, "products", barq.SearchRequest{
	Vector: queryVector,
	TopK:   10,
})

for _, result := range results {
	fmt.Printf("ID: %v\n", result.ID)
	fmt.Printf("Score: %.4f\n", result.Score)
}
```

### Text Search (BM25)

```go
results, err := client.Search(ctx, "articles", barq.SearchRequest{
	Query: "machine learning tutorial",
	TopK:  10,
})
```

### Hybrid Search

```go
results, err := client.Search(ctx, "articles", barq.SearchRequest{
	Vector: queryEmbedding,
	Query:  "neural networks",
	TopK:   10,
})
```

### Filtered Search

```go
results, err := client.Search(ctx, "products", barq.SearchRequest{
	Vector: queryVector,
	TopK:   10,
	Filter: map[string]interface{}{
		"must": []map[string]interface{}{
			{"field": "category", "match": "electronics"},
			{"field": "price", "range": map[string]float64{"lte": 100}},
		},
	},
})
```

---

## gRPC Client

For high-throughput applications:

```go
import (
	"context"
	barq "github.com/YASSERRMD/barq-db/barq-sdk-go"
)

// Connect
client, err := barq.NewGrpcClient("localhost:50051")
if err != nil {
	log.Fatal(err)
}
defer client.Close()

// Health check
ok, err := client.Health(ctx)
fmt.Println("Healthy:", ok)

// Create collection
err = client.CreateCollection(ctx, "vectors", 384, "L2")

// Insert document
err = client.InsertDocument(ctx, "vectors", "doc-001", vector, map[string]string{
	"label": "example",
})

// Search
results, err := client.Search(ctx, "vectors", queryVector, 10)
for _, r := range results {
	fmt.Printf("%v: %.4f\n", r.ID, r.Score)
}
```

---

## API Reference

### Types

```go
type Config struct {
	BaseURL string
	APIKey  string
}

type CreateCollectionRequest struct {
	Name       string      `json:"name"`
	Dimension  int         `json:"dimension"`
	Metric     string      `json:"metric"`
	Index      interface{} `json:"index,omitempty"`
	TextFields []TextField `json:"text_fields,omitempty"`
}

type TextField struct {
	Name     string `json:"name"`
	Indexed  bool   `json:"indexed"`
	Required bool   `json:"required"`
}

type InsertRequest struct {
	ID      interface{}     `json:"id"`
	Vector  []float32       `json:"vector"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type SearchRequest struct {
	Vector []float32   `json:"vector,omitempty"`
	Query  string      `json:"query,omitempty"`
	TopK   int         `json:"top_k"`
	Filter interface{} `json:"filter,omitempty"`
}

type SearchResult struct {
	ID    interface{} `json:"id"`
	Score float32     `json:"score"`
}
```

### `Client` (HTTP)

| Method | Signature | Description |
|--------|-----------|-------------|
| `CreateCollection` | `(ctx, CreateCollectionRequest) error` | Create collection |
| `Insert` | `(ctx, collection string, InsertRequest) error` | Insert document |
| `Search` | `(ctx, collection string, SearchRequest) ([]SearchResult, error)` | Search |

### `GrpcClient`

| Method | Signature | Description |
|--------|-----------|-------------|
| `Health` | `(ctx) (bool, error)` | Health check |
| `CreateCollection` | `(ctx, name, dimension, metric) error` | Create collection |
| `InsertDocument` | `(ctx, collection, id, vector, payload) error` | Insert |
| `Search` | `(ctx, collection, vector, topK) ([]SearchResult, error)` | Search |
| `Close` | `() error` | Close connection |

---

## Examples

### HTTP Server with Gin

```go
package main

import (
	"context"
	"github.com/gin-gonic/gin"
	barq "github.com/YASSERRMD/barq-db/barq-sdk-go"
)

var client = barq.NewClient(barq.Config{
	BaseURL: "http://localhost:8080",
	APIKey:  "your-key",
})

func main() {
	r := gin.Default()

	r.POST("/search", func(c *gin.Context) {
		var req struct {
			Vector []float32 `json:"vector"`
			TopK   int       `json:"top_k"`
		}
		c.BindJSON(&req)

		results, err := client.Search(context.Background(), "products", barq.SearchRequest{
			Vector: req.Vector,
			TopK:   req.TopK,
		})
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, results)
	})

	r.Run(":3000")
}
```

### Concurrent Inserts

```go
import (
	"sync"
	barq "github.com/YASSERRMD/barq-db/barq-sdk-go"
)

func batchInsert(client *barq.Client, documents []Document) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(documents))

	for _, doc := range documents {
		wg.Add(1)
		go func(d Document) {
			defer wg.Done()
			err := client.Insert(context.Background(), "products", barq.InsertRequest{
				ID:     d.ID,
				Vector: d.Vector,
			})
			if err != nil {
				errChan <- err
			}
		}(doc)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		return err
	}
	return nil
}
```

---

## Requirements

- Go 1.19+
- gRPC dependencies (for gRPC client):
  - `google.golang.org/grpc`
  - `google.golang.org/protobuf`

---

## Contributing

We welcome contributions! See the [main repository](https://github.com/YASSERRMD/barq-db) for guidelines.

### Areas for Improvement

- Context timeout handling
- Connection pooling
- Retry with exponential backoff
- Batch operations API
- Comprehensive test suite
- godoc documentation

---

## License

MIT License - see [LICENSE](https://github.com/YASSERRMD/barq-db/blob/main/LICENSE)

---

<p align="center">
  <a href="https://github.com/YASSERRMD/barq-db">Barq DB</a> - Vector search at lightning speed
</p>
