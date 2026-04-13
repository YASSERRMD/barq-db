package barq

import (
	"context"
	"os"
	"reflect"
	"testing"
)

func TestOptionsClientInsertAndSearch(t *testing.T) {
	client := NewClient(Config{
		BaseURL: getenv("BARQ_BASE_URL", "http://127.0.0.1:8080"),
		APIKey:  os.Getenv("BARQ_API_KEY"),
	})
	collection := getenv("BARQ_TEST_COLLECTION", "sdk-go-options")
	waitForCommit := true
	consistency := ConsistencyPrimary
	allowFallback := true

	ctx := context.Background()

	if err := client.CreateCollection(ctx, CreateCollectionRequest{
		Name:      collection,
		Dimension: 2,
		Metric:    "Cosine",
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}

	if err := client.Insert(ctx, collection, InsertRequest{
		ID:      "go-options-doc",
		Vector:  []float32{1.0, 0.0},
		Payload: []byte(`{"sdk":"go","mode":"options"}`),
		Options: &InsertOptions{WaitForCommit: &waitForCommit},
	}); err != nil {
		t.Fatalf("insert with options: %v", err)
	}

	results, err := client.Search(ctx, collection, SearchRequest{
		Vector: []float32{1.0, 0.0},
		TopK:   1,
		Options: &SearchOptions{
			Consistency:   &consistency,
			AllowFallback: &allowFallback,
		},
	})
	if err != nil {
		t.Fatalf("search with options: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if !reflect.DeepEqual(results[0].ID, map[string]interface{}{"Str": "go-options-doc"}) {
		t.Fatalf("unexpected id %#v", results[0].ID)
	}
}
