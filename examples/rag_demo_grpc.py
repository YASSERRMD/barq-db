
import time
from barq import GrpcClient

def main():
    # 1. Connect
    print("Connecting to Barq gRPC at localhost:50051...")
    client = GrpcClient(target="localhost:50051")
    
    # 2. Health
    print(f"Health check: {client.health()}")
    
    # 3. Create Collection
    try:
        client.create_collection("grpc_py_rag", 2, "Cosine")
        print("Collection 'grpc_py_rag' created.")
    except Exception as e:
        print(f"Collection creation failed (maybe exists): {e}")

    # 4. Insert
    print("Inserting document...")
    client.insert_document(
        collection="grpc_py_rag",
        id="doc_py_1",
        vector=[0.5, 0.5],
        payload={"lang": "python", "protocol": "grpc"}
    )
    
    # Wait
    time.sleep(0.5)
    
    # 5. Search
    print("Searching...")
    results = client.search(
        collection="grpc_py_rag",
        vector=[0.5, 0.5],
        top_k=3
    )
    
    print(f"Found {len(results)} results:")
    for r in results:
        print(r)

if __name__ == "__main__":
    main()
