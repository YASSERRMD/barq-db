package barq

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestGoldenClientMatchesRestBaseline(t *testing.T) {
	client := NewClient(Config{
		BaseURL: getenv("BARQ_BASE_URL", "http://127.0.0.1:8080"),
		APIKey:  os.Getenv("BARQ_API_KEY"),
	})
	collection := getenv("BARQ_TEST_COLLECTION", "sdk-go-golden")

	var expected []SearchResult
	if err := json.Unmarshal([]byte(os.Getenv("BARQ_EXPECTED_RESULTS")), &expected); err != nil {
		t.Fatalf("decode expected results: %v", err)
	}

	ctx := context.Background()

	if err := client.CreateCollection(ctx, CreateCollectionRequest{
		Name:      collection,
		Dimension: 2,
		Metric:    "Cosine",
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if err := client.Insert(ctx, collection, InsertRequest{
		ID:      "golden-primary",
		Vector:  []float32{1.0, 0.0},
		Payload: []byte(`{"sdk":"go","mode":"golden"}`),
	}); err != nil {
		t.Fatalf("insert primary: %v", err)
	}
	if err := client.Insert(ctx, collection, InsertRequest{
		ID:      "golden-secondary",
		Vector:  []float32{0.0, 1.0},
		Payload: []byte(`{"sdk":"go","mode":"golden"}`),
	}); err != nil {
		t.Fatalf("insert secondary: %v", err)
	}

	results, err := client.Search(ctx, collection, SearchRequest{
		Vector: []float32{1.0, 0.0},
		TopK:   2,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}

	if !reflect.DeepEqual(results, expected) {
		t.Fatalf("unexpected results %#v", results)
	}
}
