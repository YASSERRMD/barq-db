import asyncio
import json
import os
from typing import Optional, List, Dict, Any, Union
from urllib.parse import urlparse

from .grpc_client import GrpcClient


def _compat_document_id(value: str) -> Dict[str, Union[int, str]]:
    try:
        return {"U64": int(value)}
    except ValueError:
        return {"Str": value}


def _require_supported_api_version() -> None:
    version = os.getenv("API_VERSION", "v1")
    if version != "v1":
        raise ValueError(f"unsupported API_VERSION: {version}")


class BarqClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self._http_client = None
        self._grpc_client: Optional[GrpcClient] = None

    def _grpc_target(self) -> str:
        override = os.getenv("BARQ_GRPC_ADDR")
        if override:
            if "://" in override:
                return urlparse(override).netloc or override
            return override

        parsed = urlparse(self.base_url)
        host = parsed.hostname
        if not host:
            return self.base_url

        return f"{host}:50051"

    def _grpc(self) -> GrpcClient:
        if self._grpc_client is None:
            self._grpc_client = GrpcClient(self._grpc_target(), api_key=self.api_key)
        return self._grpc_client

    def _http(self):
        if self._http_client is None:
            import httpx

            self._http_client = httpx.Client(
                headers={"x-api-key": self.api_key},
                timeout=10.0,
            )
        return self._http_client

    def health(self) -> bool:
        _require_supported_api_version()
        return self._grpc().status()

    def create_collection(
        self,
        name: str,
        dimension: int,
        metric: str = "L2",
        index: Optional[Union[str, Dict]] = None,
        text_fields: list = None,
    ) -> Dict:
        _require_supported_api_version()
        if index is None and not text_fields:
            self._grpc().create_collection(name=name, dimension=dimension, metric=metric)
            return {}

        url = f"{self.base_url}/collections"
        payload = {
            "name": name,
            "dimension": dimension,
            "metric": metric,
            "index": index,
            "text_fields": text_fields or [],
        }
        resp = self._http().post(url, json=payload)
        resp.raise_for_status()
        return resp.json() if resp.text else {}

    def insert_document(
        self,
        collection: str,
        id: Union[int, str],
        vector: List[float],
        payload: Optional[Dict] = None,
        options: Optional[Dict[str, Any]] = None,
    ):
        _require_supported_api_version()
        self._grpc().insert(collection, id, vector, payload or {}, options=options)
        return {}

    async def insert_async(
        self,
        collection: str,
        id: Union[int, str],
        vector: List[float],
        payload: Optional[Dict] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> str:
        _require_supported_api_version()
        return await asyncio.to_thread(
            self._grpc().insert_async,
            collection,
            id,
            vector,
            payload or {},
            options,
        )

    def get_insert_status(self, request_id: str) -> Dict[str, Any]:
        _require_supported_api_version()
        return self._grpc().get_insert_status(request_id)

    def search(
        self,
        collection: str,
        vector: Optional[List[float]] = None,
        query: Optional[str] = None,
        top_k: int = 10,
        filter: Optional[Dict] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> List[Dict]:
        _require_supported_api_version()
        if vector and not query and filter is None:
            results = self._grpc().search(
                collection=collection,
                vector=vector,
                top_k=top_k,
                options=options,
            )
            return [
                {
                    "id": _compat_document_id(str(result["id"])),
                    "score": result["score"],
                }
                for result in results
            ]

        if options:
            raise ValueError(
                "advanced search options are only supported for vector-only gRPC search"
            )

        url = f"{self.base_url}/collections/{collection}/search"
        if vector and query:
            url += "/hybrid"
        elif query:
            url += "/text"

        body = {
            "vector": vector,
            "query": query,
            "top_k": top_k,
            "filter": filter,
        }
        resp = self._http().post(url, json=body)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])

    def close(self):
        if self._grpc_client is not None:
            self._grpc_client.close()
            self._grpc_client = None
        if self._http_client is not None:
            self._http_client.close()
            self._http_client = None
