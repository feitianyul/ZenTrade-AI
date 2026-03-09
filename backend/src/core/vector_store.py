"""向量存储客户端 (Qdrant)

TODO: 部署 Qdrant/Milvus 后接入。
      当前在无向量数据库时返回空结果（设置 VECTOR_STORE_MOCK=true）。
      部署步骤:
        1. docker run -d -p 6333:6333 qdrant/qdrant
        2. 设置环境变量 VECTOR_STORE_URL=http://localhost:6333
        3. 去掉 VECTOR_STORE_MOCK 或设为 false
"""

import os
from typing import Any, Dict, List, Optional

import httpx
from pydantic import BaseModel


class VectorSearchResult(BaseModel):
    id: str | int
    score: float
    payload: Dict[str, Any]

class VectorStore:
    def __init__(self, url: str = None, api_key: str = None):
        self.url = url or os.getenv("VECTOR_STORE_URL", "http://localhost:6333")
        self.api_key = api_key or os.getenv("VECTOR_STORE_API_KEY")
        self.headers = {"Content-Type": "application/json"}
        if self.api_key:
            self.headers["api-key"] = self.api_key

    async def _request(
        self,
        method: str,
        path: str,
        json_data: Any = None,
    ) -> Dict[str, Any]:
        async with httpx.AsyncClient() as client:
            try:
                response = await client.request(
                    method,
                    f"{self.url}{path}",
                    json=json_data,
                    headers=self.headers,
                    timeout=10.0,
                )
                response.raise_for_status()
                return response.json()
            except httpx.RequestError as e:
                # Fallback for mock/testing if needed
                if os.getenv("VECTOR_STORE_MOCK", "false").lower() == "true":
                    return {"result": [], "status": "ok"}
                raise e

    async def create_collection(
        self,
        collection_name: str,
        vector_size: int = 1536,
        distance: str = "Cosine",
    ) -> bool:
        """Create a collection in Qdrant"""
        payload = {
            "vectors": {
                "size": vector_size,
                "distance": distance
            }
        }
        try:
            await self._request("PUT", f"/collections/{collection_name}", payload)
            return True
        except Exception:
            return False

    async def upsert(
        self,
        collection_name: str,
        points: List[Dict[str, Any]],
    ) -> bool:
        """
        Upsert points. Each point should be:
        { "id": 1, "vector": [...], "payload": {...} }
        """
        payload = {"points": points}
        try:
            await self._request(
                "PUT",
                f"/collections/{collection_name}/points",
                payload,
            )
            return True
        except Exception:
            return False

    async def search(
        self,
        collection_name: str,
        vector: List[float],
        limit: int = 5,
        filter: Optional[Dict] = None,
    ) -> List[VectorSearchResult]:
        """Search for nearest neighbors"""
        payload = {
            "vector": vector,
            "limit": limit,
            "with_payload": True
        }
        if filter:
            payload["filter"] = filter
            
        try:
            data = await self._request(
                "POST",
                f"/collections/{collection_name}/points/search",
                payload,
            )
            results = []
            for item in data.get("result", []):
                results.append(
                    VectorSearchResult(
                        id=item["id"],
                        score=item["score"],
                        payload=item.get("payload", {}),
                    )
                )
            return results
        except Exception:
            return []

async def init_vector_index(collection: str) -> dict[str, Any]:
    store = VectorStore()
    # Attempt to create collection if not exists (lazy init)
    # Defaulting to 1536 for OpenAI embeddings or similar
    success = await store.create_collection(collection, vector_size=1536)
    return {"collection": collection, "status": "ready" if success else "error"}
