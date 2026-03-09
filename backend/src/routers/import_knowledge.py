from typing import Any, List

from fastapi import APIRouter, Depends, File, Header, HTTPException, Query, UploadFile
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db import get_db
from src.core.errors import ValidationError
from src.schemas.response import BaseResponse, ok
from src.schemas.user import UserOut
from src.services.auth_service import get_user_from_token
from src.services.import_knowledge_service import import_knowledge
from src.services.knowledge_doc_service import (
    ingest_document, list_kb_documents, delete_kb_document, search_knowledge,
)

router = APIRouter()


async def _require_user(authorization: str | None) -> UserOut:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1]
    user = await get_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid token")
    return user


@router.post("/import/knowledge", response_model=BaseResponse[dict[str, Any]])
async def import_knowledge_endpoint(
    file: UploadFile = File(...),
    authorization: str | None = Header(default=None),
) -> BaseResponse[dict[str, Any]]:
    user = await _require_user(authorization)
    content = await file.read()
    try:
        result = await import_knowledge(user.tenant_id, user.user_id, file.filename, content)
    except ValidationError as exc:
        raise exc.as_http_exception() from exc
    return ok(result)


# ---- Knowledge Base Vector Document APIs ----

@router.post("/knowledge/upload", response_model=BaseResponse[dict[str, Any]],
             summary="上传知识库文档", description="上传文档自动解析、分块、向量化并存入 Qdrant")
async def upload_kb_document(
    file: UploadFile = File(...),
    kb_type: str = Query("strategy", description="知识库类型: strategy|market|trade|compliance"),
    authorization: str | None = Header(default=None),
    db: AsyncSession = Depends(get_db),
):
    user = await _require_user(authorization)
    content_bytes = await file.read()
    try:
        content_str = content_bytes.decode("utf-8")
    except UnicodeDecodeError:
        content_str = content_bytes.decode("gbk", errors="replace")
    
    result = await ingest_document(content_str, file.filename, kb_type, user.tenant_id, db)
    return ok(result)


@router.get("/knowledge/documents", response_model=BaseResponse[List[dict]],
            summary="知识库文档列表")
async def list_kb_docs(
    authorization: str | None = Header(default=None),
):
    user = await _require_user(authorization)
    docs = await list_kb_documents(user.tenant_id)
    return ok(docs)


@router.delete("/knowledge/documents/{filename}", response_model=BaseResponse[dict],
               summary="删除知识库文档")
async def delete_kb_doc(
    filename: str,
    authorization: str | None = Header(default=None),
):
    user = await _require_user(authorization)
    success = await delete_kb_document(filename, user.tenant_id)
    return ok({"deleted": success, "filename": filename})


class KBSearchRequest(BaseModel):
    query: str
    kb_type: str = None
    limit: int = 5

@router.post("/knowledge/search", response_model=BaseResponse[List[dict]],
             summary="知识库语义搜索")
async def kb_search(
    req: KBSearchRequest,
    authorization: str | None = Header(default=None),
    db: AsyncSession = Depends(get_db),
):
    user = await _require_user(authorization)
    results = await search_knowledge(req.query, user.tenant_id, db, req.kb_type, req.limit)
    return ok(results)
