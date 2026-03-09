"""Knowledge Document Service: Upload, parse, chunk, embed, and store in vector DB."""

import hashlib
import logging
import re
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.vector_store import VectorStore
from src.services.ai_config_service import AIConfigService
from src.services.llm_service.llm_router import LLMRouter

logger = logging.getLogger(__name__)

KB_COLLECTION = "knowledge_base"
CHUNK_SIZE = 500  # characters per chunk
CHUNK_OVERLAP = 50


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> List[str]:
    """Split text into overlapping chunks."""
    chunks = []
    # Split by paragraphs first
    paragraphs = re.split(r'\n\s*\n', text)
    current_chunk = ""
    
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        if len(current_chunk) + len(para) + 1 <= chunk_size:
            current_chunk += ("\n" + para if current_chunk else para)
        else:
            if current_chunk:
                chunks.append(current_chunk)
            # If paragraph itself is too long, split by sentences
            if len(para) > chunk_size:
                sentences = re.split(r'[。！？\.\!\?]', para)
                current_chunk = ""
                for sent in sentences:
                    sent = sent.strip()
                    if not sent:
                        continue
                    if len(current_chunk) + len(sent) + 1 <= chunk_size:
                        current_chunk += ("。" + sent if current_chunk else sent)
                    else:
                        if current_chunk:
                            chunks.append(current_chunk)
                        current_chunk = sent
            else:
                current_chunk = para

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


def parse_document(content: str, filename: str) -> str:
    """Parse document content based on file type. Returns plain text."""
    lower = filename.lower()
    if lower.endswith('.md') or lower.endswith('.markdown'):
        # Strip markdown syntax
        text = re.sub(r'#{1,6}\s+', '', content)
        text = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', text)
        text = re.sub(r'[*_`~]', '', text)
        return text
    elif lower.endswith('.json'):
        import json
        try:
            data = json.loads(content)
            if isinstance(data, list):
                return "\n\n".join(str(item) for item in data)
            elif isinstance(data, dict):
                return "\n".join(f"{k}: {v}" for k, v in data.items())
        except json.JSONDecodeError:
            pass
    # Default: treat as plain text
    return content


async def get_embedding(text: str, db: AsyncSession, tenant_id: str) -> Optional[List[float]]:
    """Get embedding vector via LLM API (OpenAI-compatible embeddings endpoint)."""
    from src.services.ai_service import get_llm_router
    
    router = await get_llm_router(db, tenant_id)
    if not router:
        # Return mock embedding for development
        logger.warning("No LLM configured, using mock embedding")
        return _mock_embedding(text)

    # Try to use embeddings endpoint
    key = router._select_key()
    if not key:
        return _mock_embedding(text)

    import httpx
    endpoint = key.get("endpoint", "").rstrip("/")
    api_key = key.get("api_key", "")
    url = f"{endpoint}/embeddings"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    body = {"model": "text-embedding-3-small", "input": text[:8000]}

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(url, json=body, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            return data["data"][0]["embedding"]
        else:
            logger.warning("Embedding API failed: %s", resp.status_code)
            return _mock_embedding(text)
    except Exception as e:
        logger.warning("Embedding exception: %s", e)
        return _mock_embedding(text)


def _mock_embedding(text: str) -> List[float]:
    """Generate deterministic mock embedding for development.
    
    TODO: 配置 LLM API Key 后用真实 embedding 模型替换此 Mock。
    当 LLM 未配置时此函数作为 fallback 使用。
    """
    import hashlib
    h = hashlib.sha256(text.encode()).digest()
    # Generate 1536-dim vector from hash (repeating)
    vec = []
    for i in range(1536):
        byte_idx = i % 32
        vec.append((h[byte_idx] - 128) / 128.0)
    return vec


async def ingest_document(
    content: str,
    filename: str,
    kb_type: str,  # strategy | market | trade | compliance
    tenant_id: str,
    db: AsyncSession,
) -> Dict[str, Any]:
    """Full pipeline: parse -> chunk -> embed -> store in Qdrant."""
    # Parse
    text = parse_document(content, filename)
    if not text.strip():
        return {"status": "error", "message": "文档内容为空"}

    # Chunk
    chunks = chunk_text(text)
    if not chunks:
        return {"status": "error", "message": "无法分块"}

    # Embed and store
    store = VectorStore()
    # Ensure collection exists
    await store.create_collection(KB_COLLECTION, vector_size=1536)

    points = []
    for i, chunk in enumerate(chunks):
        embedding = await get_embedding(chunk, db, tenant_id)
        if not embedding:
            continue
        point_id = str(uuid.uuid4())
        points.append({
            "id": point_id,
            "vector": embedding,
            "payload": {
                "text": chunk,
                "filename": filename,
                "kb_type": kb_type,
                "tenant_id": tenant_id,
                "chunk_index": i,
                "total_chunks": len(chunks),
            },
        })

    if points:
        # Batch upsert (max 100 per batch)
        for batch_start in range(0, len(points), 100):
            batch = points[batch_start:batch_start + 100]
            await store.upsert(KB_COLLECTION, batch)

    return {
        "status": "ok",
        "filename": filename,
        "kb_type": kb_type,
        "total_chunks": len(chunks),
        "stored_chunks": len(points),
    }


async def search_knowledge(
    query: str,
    tenant_id: str,
    db: AsyncSession,
    kb_type: str = None,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """Search knowledge base using vector similarity."""
    embedding = await get_embedding(query, db, tenant_id)
    if not embedding:
        return []

    store = VectorStore()
    filter_cond = {"must": [{"key": "tenant_id", "match": {"value": tenant_id}}]}
    if kb_type:
        filter_cond["must"].append({"key": "kb_type", "match": {"value": kb_type}})

    results = await store.search(KB_COLLECTION, embedding, limit=limit, filter=filter_cond)
    return [{"text": r.payload.get("text", ""), "score": r.score,
             "filename": r.payload.get("filename", ""), "kb_type": r.payload.get("kb_type", "")}
            for r in results]


async def list_kb_documents(tenant_id: str) -> List[Dict]:
    """List all documents in knowledge base (via Qdrant scroll)."""
    store = VectorStore()
    try:
        data = await store._request("POST", f"/collections/{KB_COLLECTION}/points/scroll", {
            "filter": {"must": [{"key": "tenant_id", "match": {"value": tenant_id}}]},
            "limit": 1000,
            "with_payload": True,
            "with_vector": False,
        })
        # Group by filename
        docs = {}
        for p in data.get("result", {}).get("points", []):
            fn = p.get("payload", {}).get("filename", "unknown")
            if fn not in docs:
                docs[fn] = {"filename": fn, "kb_type": p["payload"].get("kb_type", ""),
                            "chunks": 0, "id": p["id"]}
            docs[fn]["chunks"] += 1
        return list(docs.values())
    except Exception:
        return []


async def delete_kb_document(filename: str, tenant_id: str) -> bool:
    """Delete all chunks of a document."""
    store = VectorStore()
    try:
        await store._request("POST", f"/collections/{KB_COLLECTION}/points/delete", {
            "filter": {"must": [
                {"key": "tenant_id", "match": {"value": tenant_id}},
                {"key": "filename", "match": {"value": filename}},
            ]}
        })
        return True
    except Exception:
        return False
