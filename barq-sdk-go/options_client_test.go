package barq

import (
	"context"
	"strings"
	"testing"

	pb "github.com/YASSERRMD/barq-db/barq-sdk-go/proto"
)

func TestProtoInsertOptionsEncodeWaitForCommit(t *testing.T) {
	waitForCommit := false

	options := protoInsertOptions(&InsertOptions{WaitForCommit: &waitForCommit})
	if options == nil {
		t.Fatal("expected insert options to be encoded")
	}
	if options.WaitForCommit {
		t.Fatal("expected wait_for_commit to remain false")
	}
}

func TestProtoSearchOptionsDefaultAllowFallback(t *testing.T) {
	consistency := ConsistencyFollowers

	options := protoSearchOptions(&SearchOptions{Consistency: &consistency})
	if options == nil {
		t.Fatal("expected search options to be encoded")
	}
	if options.Consistency != pb.Consistency_CONSISTENCY_FOLLOWERS {
		t.Fatalf("unexpected consistency %v", options.Consistency)
	}
	if !options.AllowFallback {
		t.Fatal("expected allow_fallback default to remain true")
	}
}

func TestClientSearchRejectsAdvancedOptionsOnRestFallback(t *testing.T) {
	allowFallback := false
	client := NewClient(Config{BaseURL: "http://127.0.0.1:8080", APIKey: "test-key"})

	_, err := client.Search(context.Background(), "docs", SearchRequest{
		Vector:  []float32{1.0, 0.0},
		Query:   "hello",
		TopK:    1,
		Options: &SearchOptions{AllowFallback: &allowFallback},
	})
	if err == nil {
		t.Fatal("expected search to reject advanced options on REST fallback")
	}
	if !strings.Contains(err.Error(), "advanced search options are only supported for vector-only gRPC search") {
		t.Fatalf("unexpected error: %v", err)
	}
}
