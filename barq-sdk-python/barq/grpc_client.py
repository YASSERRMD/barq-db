
import grpc
import json
from . import barq_pb2
from . import barq_pb2_grpc
from typing import Optional, List, Any, Dict

class GrpcClient:
    def __init__(self, target: str):
        self.channel = grpc.insecure_channel(target)
        self.stub = barq_pb2_grpc.BarqStub(self.channel)
        
    def health(self) -> bool:
        response = self.stub.Health(barq_pb2.HealthRequest())
        return response.ok
        
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
        self.stub.CreateCollection(req)
        
    def insert_document(
        self,
        collection: str,
        id: Any,
        vector: List[float],
        payload: Optional[Dict] = None
    ):
        payload_json = json.dumps(payload) if payload else "{}"
        req = barq_pb2.InsertDocumentRequest(
            collection=collection,
            id=str(id),
            vector=vector,
            payload_json=payload_json
        )
        self.stub.InsertDocument(req)
        
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
        response = self.stub.Search(req)
        
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
