package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/YASSERRMD/barq-db/barq-sdk-go"
)

func main() {
	// 1. Connect
	fmt.Println("Connecting to Barq gRPC at localhost:50051...")
	client, err := barq.NewGrpcClient("localhost:50051")
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// 2. Health
	ok, err := client.Health(ctx)
	if err != nil {
		log.Fatalf("Health check failed: %v", err)
	}
	fmt.Printf("Health check: %v\n", ok)

	// 3. Create Collection
	fmt.Println("Creating collection 'grpc_go_rag'...")
	err = client.CreateCollection(ctx, "grpc_go_rag", 2, "Cosine")
	if err != nil {
		fmt.Printf("Create collection error (maybe exists): %v\n", err)
	}

	// 4. Insert
	fmt.Println("Inserting document...")
	vector := []float32{0.2, 0.8}
	payload := map[string]string{"lang": "go", "protocol": "grpc"}
	err = client.InsertDocument(ctx, "grpc_go_rag", "doc_go_1", vector, payload)
	if err != nil {
		log.Fatalf("Insert failed: %v", err)
	}

	// Wait
	time.Sleep(500 * time.Millisecond)

	// 5. Search
	fmt.Println("Searching...")
	results, err := client.Search(ctx, "grpc_go_rag", vector, 3)
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}

	fmt.Printf("Found %d results:\n", len(results))
	for _, r := range results {
		fmt.Printf("- ID: %v, Score: %f\n", r.ID, r.Score)
	}
}
