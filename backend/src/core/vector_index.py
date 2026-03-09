"""T227 - 向量联合索引优化

TODO: 部署 Milvus/Qdrant 后接入真实向量搜索。
      当前使用内存 dict 模拟，不支持真正的 ANN 搜索。
      接入步骤:
        1. 部署 Milvus: docker-compose up -d milvus
        2. pip install pymilvus
        3. 替换 upsert/search/delete 为真实 Milvus API 调用
"""

from typing import Any, Optional


class VectorIndex:
    """向量索引管理器 (内存模拟，TODO: 接入 Milvus)"""

    def __init__(self, collection_name: str = "default"):
        self.collection_name = collection_name
        self._index: dict[str, dict[str, Any]] = {}

    async def upsert(
        self, doc_id: str, embedding: list[float], metadata: dict[str, Any]
    ) -> dict[str, Any]:
        """更新或插入向量"""
        self._index[doc_id] = {
            "embedding": embedding,
            "metadata": metadata,
        }
        return {"doc_id": doc_id, "status": "upserted"}

    async def search(
        self,
        query_embedding: list[float],
        top_k: int = 5,
        filters: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        """向量搜索（占位 - 生产使用 Qdrant）"""
        results = []
        for doc_id, data in list(self._index.items())[:top_k]:
            if filters:
                match = all(
                    data["metadata"].get(k) == v for k, v in filters.items()
                )
                if not match:
                    continue
            results.append({
                "doc_id": doc_id,
                "score": 0.95,  # mock score
                "metadata": data["metadata"],
            })
        return results

    async def delete(self, doc_id: str) -> bool:
        if doc_id in self._index:
            del self._index[doc_id]
            return True
        return False

    async def count(self) -> int:
        return len(self._index)

    async def get_stats(self) -> dict[str, Any]:
        return {
            "collection": self.collection_name,
            "total_vectors": len(self._index),
        }
