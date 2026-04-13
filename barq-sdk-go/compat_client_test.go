package barq

import (
	"context"
	"os"
	"reflect"
	"testing"
)

func TestCompatClientCreateInsertSearch(t *testing.T) {
	client := NewClient(Config{
		BaseURL: getenv("BARQ_BASE_URL", "http://127.0.0.1:8080"),
		APIKey:  os.Getenv("BARQ_API_KEY"),
	})

	ctx := context.Background()

	if err := client.CreateCollection(ctx, CreateCollectionRequest{
		Name:      "sdk-go-compat",
		Dimension: 2,
		Metric:    "Cosine",
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}

	if err := client.Insert(ctx, "sdk-go-compat", InsertRequest{
		ID:      "go-doc",
		Vector:  []float32{1.0, 0.0},
		Payload: []byte(`{"sdk":"go","mode":"compat"}`),
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	results, err := client.Search(ctx, "sdk-go-compat", SearchRequest{
		Vector: []float32{1.0, 0.0},
		TopK:   1,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if !reflect.DeepEqual(results[0].ID, map[string]interface{}{"Str": "go-doc"}) {
		t.Fatalf("unexpected id %#v", results[0].ID)
	}
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
