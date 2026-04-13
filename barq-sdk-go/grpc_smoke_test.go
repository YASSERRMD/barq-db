package barq

import (
	"context"
	"os"
	"testing"
)

func TestGrpcSmoke(t *testing.T) {
	target := os.Getenv("BARQ_GRPC_ADDR")
	collection := os.Getenv("BARQ_TEST_COLLECTION")
	if target == "" || collection == "" {
		t.Fatal("BARQ_GRPC_ADDR and BARQ_TEST_COLLECTION must be set")
	}

	client, err := NewGrpcClient(target)
	if err != nil {
		t.Fatalf("connect grpc client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	ok, err := client.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !ok {
		t.Fatal("status returned not ok")
	}

	if err := client.CreateCollection(ctx, collection, 2, "Cosine"); err != nil {
		t.Fatalf("create collection: %v", err)
	}

	if err := client.Insert(ctx, collection, "go-doc", []float32{1.0, 0.0}, map[string]string{
		"sdk":  "go",
		"mode": "grpc",
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	results, err := client.Search(ctx, collection, []float32{1.0, 0.0}, 1)
	if err != nil {
		t.Fatalf("search: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ID != "go-doc" {
		t.Fatalf("unexpected id %v", results[0].ID)
	}
}
