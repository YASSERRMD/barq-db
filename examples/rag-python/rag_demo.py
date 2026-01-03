import os
import time
from typing import List
from barq import BarqClient, Document
from sentence_transformers import SentenceTransformer

# Initialize Embedding Model
print("Loading embedding model...")
embedder = SentenceTransformer('all-MiniLM-L6-v2')

def get_embedding(text: str) -> List[float]:
    return embedder.encode(text).tolist()

def generate_answer(query: str, context: List[str]) -> str:
    # Mock LLM generation for demo purposes
    context_block = "\n".join([f"- {c}" for c in context])
    return f"Based on the context:\n{context_block}\n\nI can answer that: '{query}' relates to Barq's capabilities."

def main():
    # Connect to Barq DB
    client = BarqClient("http://localhost:8080")
    
    collection_name = "knowledge_base"
    
    # Create Collection
    if not client.collection_exists(collection_name):
        print(f"Creating collection '{collection_name}'...")
        client.create_collection(
            name=collection_name,
            dimension=384, # Output dim of all-MiniLM-L6-v2
            metric="Cosine"
        )
    
    # Sample Data
    documents = [
        "Barq DB is a distributed, cloud-native vector database.",
        "It supports storage tiering to S3 and GCS to reduce costs.",
        "The query engine uses HNSW for fast vector search and BM25 for text search.",
        "Barq DB includes a Kubernetes Operator for easy deployment.",
        "Hybrid search combines vector and keyword scores using Reciprocal Rank Fusion (RRF)."
    ]
    
    # Ingest Data
    print("Ingesting documents...")
    for i, text in enumerate(documents):
        doc_id = f"doc_{i}"
        vector = get_embedding(text)
        client.insert(
            collection_name,
            Document(
                id=doc_id,
                vector=vector,
                payload={"text": text, "source": "manual"}
            )
        )
        
    # Search
    query = "How does Barq handle storage?"
    print(f"\nQuerying: '{query}'")
    
    query_vector = get_embedding(query)
    
    # Hybrid Search
    results = client.search(
        collection_name,
        vector=query_vector,
        top_k=3,
        filter=None # Optional filter
    )
    
    print("\nSearch Results:")
    contexts = []
    for hit in results:
        text = hit.payload['text']
        print(f"- [Score: {hit.score:.4f}] {text}")
        contexts.append(text)
        
    # Generate Answer
    answer = generate_answer(query, contexts)
    print(f"\nGenerated Answer:\n{answer}")

if __name__ == "__main__":
    main()
