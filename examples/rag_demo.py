import sys
import os
import random
import time

# Add sdk path to sys.path to simulate installed package
sys.path.append(os.path.join(os.path.dirname(__file__), "../barq-sdk-python"))

from barq import BarqClient

def get_dummy_embedding(text: str, dim: int = 128) -> list:
    """Mock embedding function returning random normalized vectors."""
    vec = [random.random() for _ in range(dim)]
    norm = sum(x*x for x in vec) ** 0.5
    return [x/norm for x in vec]

def main():
    print("Initializing Barq Client...")
    # Assumes Barq is running locally on 8000
    client = BarqClient(base_url="http://localhost:8000", api_key="secret-key")

    if not client.health():
        print("Error: Barq server is not reachable")
        sys.exit(1)
    
    col_name = "notes_collection"
    dim = 128
    
    print(f"Creating collection '{col_name}'...")
    try:
        client.create_collection(col_name, dim, metric="Cosine", text_fields=[
            {"name": "content", "indexed": True, "required": True}
        ])
    except Exception as e:
        print(f"Collection might already exist: {e}")

    # Data to insert
    documents = [
        {"id": 1, "text": "Barq is a Rust-native vector database."},
        {"id": 2, "text": "It supports hybrid search with BM25 and vectors."},
        {"id": 3, "text": "HNSW and IVF indexes are available for ANN search."},
        {"id": 4, "text": "Rust guarantees memory safety and high performance."},
    ]

    print("Indexing documents...")
    for doc in documents:
        vector = get_dummy_embedding(doc["text"], dim)
        client.insert_document(
            col_name, 
            id=doc["id"], 
            vector=vector, 
            payload={"content": doc["text"]}
        )
    
    # Allow some time for async indexers (less issue in tests but good practice)
    time.sleep(1)

    query = "vector database features"
    print(f"\nSearching for: '{query}'")
    query_vec = get_dummy_embedding(query, dim)

    results = client.search(
        col_name,
        vector=query_vec,
        query=query, # Trigger hybrid search
        top_k=2
    )

    print("Results:")
    for res in results:
        print(f"- ID: {res['id']}, Score: {res['score']}")
        # Note: Payload retrieval is planned for next phase
        # print(f"  Content: {res.get('payload', {}).get('content')}")

if __name__ == "__main__":
    main()
