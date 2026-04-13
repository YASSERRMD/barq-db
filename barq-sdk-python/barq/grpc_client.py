
import grpc
import json
from . import barq_pb2
from . import barq_pb2_grpc
from typing import Optional, List, Any, Dict

class GrpcClient:
    def __init__(self, target: str, api_key: Optional[str] = None, tenant_id: Optional[str] = None):
        self.channel = grpc.insecure_channel(target)
        self.stub = barq_pb2_grpc.BarqStub(self.channel)
        self.metadata = []
        if api_key:
            self.metadata.append(("x-api-key", api_key))
        if tenant_id:
            self.metadata.append(("x-tenant-id", tenant_id))

    def status(self) -> bool:
        response = self.stub.Status(barq_pb2.StatusRequest(), metadata=self.metadata)
        return response.ok

    def health(self) -> bool:
        return self.status()

    def create_collection(
        self,
        name: str,
        dimension: int,
        metric: str = "L2"
    ):
        req = barq_pb2.CreateCollectionRequest(
            name=name,
            dimension=dimension,
            metric=metric
        )
        self.stub.CreateCollection(req, metadata=self.metadata)
        
    def insert(
        self,
        collection: str,
        id: Any,
        vector: List[float],
        payload: Optional[Dict] = None
    ):
        payload_json = json.dumps(payload) if payload else "{}"
        req = barq_pb2.InsertRequest(
            collection=collection,
            id=str(id),
            vector=vector,
            payload_json=payload_json
        )
        self.stub.Insert(req, metadata=self.metadata)

    def insert_document(
        self,
        collection: str,
        id: Any,
        vector: List[float],
        payload: Optional[Dict] = None
    ):
        self.insert(collection, id, vector, payload)

    def search(
        self,
        collection: str,
        vector: List[float],
        top_k: int = 10
    ) -> List[Dict]:
        req = barq_pb2.SearchRequest(
            collection=collection,
            vector=vector,
            top_k=top_k
        )
        response = self.stub.Search(req, metadata=self.metadata)
        
        results = []
        for res in response.results:
            try:
                payload = json.loads(res.payload_json)
            except:
                payload = {}
                
            results.append({
                "id": res.id,
                "score": res.score,
                "payload": payload
            })
        return results

    def close(self) -> None:
        self.channel.close()
