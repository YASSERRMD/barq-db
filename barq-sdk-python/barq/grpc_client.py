
import grpc
import json
from . import barq_pb2
from . import barq_pb2_grpc
from .observability import (
    cluster_status_from_proto,
    metrics_from_proto,
    segment_info_from_proto,
)
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
        payload: Optional[Dict] = None,
        options: Optional[Dict[str, Any]] = None,
    ):
        payload_json = json.dumps(payload) if payload else "{}"
        req = barq_pb2.InsertRequest(
            collection=collection,
            id=str(id),
            vector=vector,
            payload_json=payload_json,
        )
        insert_options = self._insert_options(options)
        if insert_options is not None:
            req.options.CopyFrom(insert_options)
        self.stub.Insert(req, metadata=self.metadata)

    def insert_async(
        self,
        collection: str,
        id: Any,
        vector: List[float],
        payload: Optional[Dict] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> str:
        payload_json = json.dumps(payload) if payload else "{}"
        req = barq_pb2.InsertRequest(
            collection=collection,
            id=str(id),
            vector=vector,
            payload_json=payload_json,
        )
        insert_options = self._insert_options(options)
        if insert_options is not None:
            req.options.CopyFrom(insert_options)
        response = self.stub.InsertAsync(req, metadata=self.metadata)
        return response.request_id

    def insert_document(
        self,
        collection: str,
        id: Any,
        vector: List[float],
        payload: Optional[Dict] = None,
        options: Optional[Dict[str, Any]] = None,
    ):
        self.insert(collection, id, vector, payload, options=options)

    def search(
        self,
        collection: str,
        vector: List[float],
        top_k: int = 10,
        options: Optional[Dict[str, Any]] = None,
    ) -> List[Dict]:
        req = barq_pb2.SearchRequest(
            collection=collection,
            vector=vector,
            top_k=top_k,
        )
        search_options = self._search_options(options)
        if search_options is not None:
            req.options.CopyFrom(search_options)
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

    def get_insert_status(self, request_id: str) -> Dict[str, Any]:
        response = self.stub.GetInsertStatus(
            barq_pb2.GetInsertStatusRequest(request_id=request_id),
            metadata=self.metadata,
        )
        state_name = barq_pb2.InsertStatusState.Name(response.state)
        if state_name.startswith("INSERT_STATUS_STATE_"):
            state_name = state_name[len("INSERT_STATUS_STATE_") :]
        return {
            "request_id": response.request_id,
            "state": state_name.lower(),
            "error_message": response.error_message or None,
        }

    def get_metrics(self):
        response = self.stub.GetMetrics(
            barq_pb2.GetMetricsRequest(),
            metadata=self.metadata,
        )
        return metrics_from_proto(response)

    def get_cluster_status(self):
        response = self.stub.GetClusterStatus(
            barq_pb2.GetClusterStatusRequest(),
            metadata=self.metadata,
        )
        return cluster_status_from_proto(response)

    def get_segment_info(self, collection: Optional[str] = None):
        response = self.stub.GetSegmentInfo(
            barq_pb2.GetSegmentInfoRequest(collection=collection or ""),
            metadata=self.metadata,
        )
        return segment_info_from_proto(response)

    def _insert_options(self, options: Optional[Dict[str, Any]]):
        if not options or "wait_for_commit" not in options:
            return None
        return barq_pb2.InsertOptions(
            wait_for_commit=bool(options["wait_for_commit"])
        )

    def _search_options(self, options: Optional[Dict[str, Any]]):
        if not options:
            return None

        has_consistency = "consistency" in options
        has_allow_fallback = "allow_fallback" in options
        if not has_consistency and not has_allow_fallback:
            return None

        consistency = barq_pb2.CONSISTENCY_UNSPECIFIED
        if has_consistency:
            value = str(options["consistency"]).lower()
            consistency = {
                "primary": barq_pb2.CONSISTENCY_PRIMARY,
                "followers": barq_pb2.CONSISTENCY_FOLLOWERS,
                "any": barq_pb2.CONSISTENCY_ANY,
            }.get(value, barq_pb2.CONSISTENCY_UNSPECIFIED)

        allow_fallback = bool(options.get("allow_fallback", True))
        return barq_pb2.SearchOptions(
            consistency=consistency,
            allow_fallback=allow_fallback,
        )

    def close(self) -> None:
        self.channel.close()
