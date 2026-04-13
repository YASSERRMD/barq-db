package barq

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestAsyncInsertReturnsRequestID(t *testing.T) {
	client := NewClient(Config{
		BaseURL: getenv("BARQ_BASE_URL", "http://127.0.0.1:8080"),
		APIKey:  os.Getenv("BARQ_API_KEY"),
	})
	collection := getenv("BARQ_TEST_COLLECTION", "sdk-go-async")

	ctx := context.Background()

	if err := client.CreateCollection(ctx, CreateCollectionRequest{
		Name:      collection,
		Dimension: 2,
		Metric:    "Cosine",
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}

	requestID, err := client.InsertAsync(ctx, collection, InsertRequest{
		ID:      "go-async-doc",
		Vector:  []float32{1.0, 0.0},
		Payload: []byte(`{"sdk":"go","mode":"async"}`),
	})
	if err != nil {
		t.Fatalf("async insert: %v", err)
	}

	if !strings.HasPrefix(requestID, "ingest-") {
		t.Fatalf("unexpected request id %q", requestID)
	}
}
