"""同意记录服务

TODO: 当前使用内存存储，需新建 consents 表持久化。
数据库迁移步骤:
  1. 创建 src/models/consent.py 定义 Consent 模型
  2. 在 src/models/__init__.py 中注册
  3. 运行 alembic revision 生成迁移
  4. 将本文件的内存存储替换为数据库查询
"""

import logging
import time
from typing import List

from src.schemas.masking import ConsentRecord

logger = logging.getLogger(__name__)

# TODO: 替换为数据库持久化。当前内存存储仅供原型使用，重启后数据丢失。
_consents: List[ConsentRecord] = []


async def grant_consent(user_id: str, scope: str, consent_id: str) -> ConsentRecord:
    # TODO: INSERT INTO consents 表
    record = ConsentRecord(
        consent_id=consent_id,
        user_id=user_id,
        scope=scope,
        status="granted",
        timestamp=time.time()
    )
    _consents.append(record)
    logger.info("grant_consent: user=%s scope=%s (in-memory, TODO: persist to DB)", user_id, scope)
    return record


async def revoke_consent(user_id: str, consent_id: str) -> bool:
    # TODO: UPDATE consents SET status='revoked' WHERE ...
    for c in _consents:
        if c.user_id == user_id and c.consent_id == consent_id:
            c.status = "revoked"
            return True
    return False


async def get_user_consents(user_id: str) -> List[ConsentRecord]:
    # TODO: SELECT * FROM consents WHERE user_id = ?
    return [c for c in _consents if c.user_id == user_id]


async def check_consent(user_id: str, scope: str) -> bool:
    # TODO: SELECT 1 FROM consents WHERE user_id = ? AND scope = ? AND status = 'granted'
    for c in _consents:
        if c.user_id == user_id and c.scope == scope and c.status == "granted":
            return True
    return False
