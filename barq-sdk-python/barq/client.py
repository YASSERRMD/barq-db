import httpx
from typing import Optional, List, Dict, Any, Union

class BarqClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.client = httpx.Client(
            headers={"x-api-key": api_key},
            timeout=10.0
        )

    def health(self) -> bool:
        resp = self.client.get(f"{self.base_url}/health")
        return resp.status_code == 200

    def create_collection(
        self, 
        name: str, 
        dimension: int, 
        metric: str = "L2", 
        index: Optional[Union[str, Dict]] = None,
        text_fields: list = None
    ) -> Dict:
        url = f"{self.base_url}/collections"
        payload = {
            "name": name,
            "dimension": dimension,
            "metric": metric,
            "index": index,
            "text_fields": text_fields or []
        }
        resp = self.client.post(url, json=payload)
        resp.raise_for_status()
        return resp.json() if resp.text else {}

    def insert_document(
        self, 
        collection: str, 
        id: Union[int, str], 
        vector: List[float], 
        payload: Optional[Dict] = None
    ):
        url = f"{self.base_url}/collections/{collection}/documents"
        body = {
            "id": id,
            "vector": vector,
            "payload": payload
        }
        resp = self.client.post(url, json=body)
        resp.raise_for_status()
        return resp.json() if resp.text else {}

    def search(
        self, 
        collection: str, 
        vector: Optional[List[float]] = None, 
        query: Optional[str] = None, 
        top_k: int = 10,
        filter: Optional[Dict] = None
    ) -> List[Dict]:
        
        url = f"{self.base_url}/collections/{collection}/search"
        if vector and query:
            url += "/hybrid"
        elif query:
            url += "/text"
        
        body = {
            "vector": vector,
            "query": query,
            "top_k": top_k,
            "filter": filter
        }
        resp = self.client.post(url, json=body)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])

    def close(self):
        self.client.close()
