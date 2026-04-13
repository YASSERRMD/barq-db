package barq

import (
	"context"
	"os"
	"testing"

	pb "github.com/YASSERRMD/barq-db/barq-sdk-go/proto"
)

func TestObservabilityClientReadsMetricsAndClusterStatus(t *testing.T) {
	client := NewClient(Config{
		BaseURL: getenv("BARQ_BASE_URL", "http://127.0.0.1:8080"),
		APIKey:  os.Getenv("BARQ_API_KEY"),
	})

	metrics, err := client.GetMetrics(context.Background())
	if err != nil {
		t.Fatalf("get metrics: %v", err)
	}
	if len(metrics.Definitions) == 0 {
		t.Fatal("expected metrics definitions to be populated")
	}
	if metrics.Storage == nil || metrics.Storage.TotalResidentVectorMemoryBytes == 0 {
		t.Fatal("expected storage metrics to include resident memory")
	}

	status, err := client.GetClusterStatus(context.Background())
	if err != nil {
		t.Fatalf("get cluster status: %v", err)
	}
	if status.Mode != pb.ClusterMode_CLUSTER_MODE_SINGLE_NODE {
		t.Fatalf("unexpected cluster mode %v", status.Mode)
	}
	if status.NodeCount != 1 {
		t.Fatalf("unexpected node count %d", status.NodeCount)
	}
	if status.ShardCount != 1 {
		t.Fatalf("unexpected shard count %d", status.ShardCount)
	}
}
