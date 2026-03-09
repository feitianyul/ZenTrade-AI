"""备份服务 — 按 content/destination 执行，一期仅本地，二期云盘占位"""

import asyncio
import gzip
import json
import logging
import os
import shutil
import subprocess
import tempfile
import zipfile
from datetime import datetime
from typing import Any, List, Tuple

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db import get_engine
from src.models.backup import Backup, BackupStatus

logger = logging.getLogger(__name__)

BACKUP_DIR = os.getenv("BACKUP_DIR", "backups")


def _project_root_for_config() -> str:
    """项目根目录（backend 的上一级），用于扫描 deploy 配置。"""
    # __file__ = .../backend/src/services/backup_service.py -> 4 层 dirname 到项目根
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


def _scan_compose_for_redis_qdrant() -> Tuple[str | None, str | None]:
    """
    扫描 deploy 目录下 docker-compose*.yml，识别 Redis 与 Qdrant 的服务/容器名。
    返回 (redis_service_or_container_name, qdrant_service_or_container_name)。
    通过 image 含 redis/qdrant 识别；若有 container_name 则优先返回容器名（如 trading_redis）。
    """
    root = _project_root_for_config()
    deploy_dir = os.path.join(root, "deploy")
    redis_name: str | None = None
    qdrant_name: str | None = None
    for name in os.listdir(deploy_dir) if os.path.isdir(deploy_dir) else []:
        if not name.endswith(".yml") and not name.endswith(".yaml"):
            continue
        if "docker-compose" not in name.lower():
            continue
        path = os.path.join(deploy_dir, name)
        if not os.path.isfile(path):
            continue
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
        except OSError as exc:
            logger.debug("scan compose %s: %s", path, exc)
            continue
        in_services = False
        current_svc = ""
        current_image = ""
        current_container_name = ""
        for line in lines:
            stripped = line.rstrip()
            if stripped == "services:":
                in_services = True
                continue
            if in_services and stripped.startswith("  ") and not stripped.startswith("    "):
                if current_svc and current_image:
                    img_lower = current_image.lower()
                    if "redis" in img_lower and redis_name is None:
                        redis_name = (current_container_name or current_svc).strip()
                    if "qdrant" in img_lower and qdrant_name is None:
                        qdrant_name = (current_container_name or current_svc).strip()
                current_svc = stripped.strip().rstrip(":").strip()
                current_image = ""
                current_container_name = ""
                continue
            if in_services and stripped.startswith("    "):
                if "image:" in stripped:
                    current_image = stripped.split("image:", 1)[1].strip()
                if "container_name:" in stripped:
                    current_container_name = stripped.split("container_name:", 1)[1].strip()
        if current_svc and current_image:
            img_lower = current_image.lower()
            if "redis" in img_lower and redis_name is None:
                redis_name = (current_container_name or current_svc).strip()
            if "qdrant" in img_lower and qdrant_name is None:
                qdrant_name = (current_container_name or current_svc).strip()
    return redis_name, qdrant_name


def _redis_rdb_path_from_docker(container_name: str) -> str | None:
    """
    通过 docker inspect 获取 Redis 容器的 RDB 路径（宿主机卷路径 + /dump.rdb）。
    若无法获取或未挂载卷则返回 None。
    """
    try:
        out = subprocess.run(
            ["docker", "inspect", container_name, "--format", "{{json .Mounts}}"],
            capture_output=True,
            timeout=5,
            text=True,
        )
        if out.returncode != 0 or not out.stdout.strip():
            return None
        mounts = json.loads(out.stdout)
        for m in mounts:
            dest = (m.get("Destination") or m.get("destination") or "").strip()
            if dest == "/data" or dest.rstrip("/").endswith("/data"):
                src = (m.get("Source") or m.get("source") or "").strip()
                if src:
                    return os.path.join(src, "dump.rdb")
        return None
    except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError, OSError) as exc:
        logger.debug("docker inspect %s: %s", container_name, exc)
        return None


def _temp_backup_dir(backup_id: str) -> str:
    """返回本次备份的临时目录绝对路径（不创建）。规格 4.5.2"""
    return os.path.abspath(os.path.join(BACKUP_DIR, ".tmp", backup_id))


def _ensure_temp_backup_dir(backup_id: str) -> str:
    """创建 BACKUP_DIR 与 .tmp/{backup_id}，返回临时目录绝对路径。规格 4.5.2"""
    path = _temp_backup_dir(backup_id)
    os.makedirs(BACKUP_DIR, exist_ok=True)
    os.makedirs(path, exist_ok=True)
    return path


def _remove_temp_backup_dir(backup_id: str) -> None:
    """删除 .tmp/{backup_id}，忽略目录不存在。规格 4.5.2"""
    path = _temp_backup_dir(backup_id)
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        pass


# 用于 in_progress 任务的可取消通知：backup_id -> asyncio.Event，cancel 时 set()
_backup_cancel_events: dict[str, asyncio.Event] = {}

# 备份内容与展示名
CONTENT_LABELS = {
    "mysql": "MySQL",
    "qdrant": "Qdrant",
    "ai_config": "AI配置",
    "redis": "Redis",
    "system_config": "系统配置",
    "clickhouse": "ClickHouse",
}


def _content_summary(content_list: List[str]) -> str:
    if not content_list:
        return "—"
    return "、".join(CONTENT_LABELS.get(c, c) for c in content_list)


def _ensure_content(content: List[str]) -> List[str]:
    """未传或空时默认：mysql + ai_config + system_config"""
    if not content:
        return ["mysql", "ai_config", "system_config"]
    return content


async def get_last_successful_full_backup_id(session: AsyncSession, tenant_id: str) -> str | None:
    """返回该租户最近一次成功的全量备份 ID，用于增量备份的 parent_id。"""
    from sqlalchemy import and_
    stmt = (
        select(Backup.id)
        .where(
            and_(
                Backup.tenant_id == tenant_id,
                Backup.type == "full",
                Backup.status == BackupStatus.SUCCESS,
            )
        )
        .order_by(Backup.completed_at.is_(None), Backup.completed_at.desc(), Backup.created_at.desc())
        .limit(1)
    )
    result = await session.execute(stmt)
    row = result.scalar_one_or_none()
    return str(row) if row else None


async def create_backup_task(
    session: AsyncSession,
    tenant_id: str,
    name: str,
    type: str = "full",
    content: List[str] | None = None,
    destination: str = "local",
    parent_id: str | None = None,
) -> Backup:
    content = _ensure_content(content or [])
    # 一期仅支持 local；二期支持 aliyundrive / baidupan
    if destination not in ("local", "aliyundrive", "baidupan"):
        destination = "local"
    content_summary = _content_summary(content)
    if type == "incremental" and not parent_id:
        parent_id = await get_last_successful_full_backup_id(session, tenant_id)

    backup = Backup(
        tenant_id=tenant_id,
        parent_id=parent_id,
        name=name,
        type=type,
        status=BackupStatus.PENDING,
        content=json.dumps(content, ensure_ascii=False),
        content_summary=content_summary,
        destination=destination,
    )
    session.add(backup)
    await session.commit()
    await session.refresh(backup)

    asyncio.create_task(process_backup(backup.id, tenant_id))
    return backup


def _quote_identifier(engine: Any, name: str) -> str:
    """按数据库类型引用表名"""
    safe = "".join(c for c in name if c.isalnum() or c in "._")
    if not safe or safe != name:
        raise ValueError(f"invalid table name: {name}")
    dialect = engine.dialect.name
    if dialect == "mysql":
        return f"`{safe}`"
    return f"[{safe}]"


async def _get_table_names(engine: Any) -> List[str]:
    """获取业务表名（排除 alembic 等）"""
    dialect = engine.dialect.name
    async with engine.connect() as conn:
        if dialect == "mysql":
            r = await conn.execute(text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'"
            ))
            tables = [row[0] for row in r.fetchall()]
        else:
            r = await conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
            tables = [row[0] for row in r.fetchall()]
    return [t for t in tables if not t.startswith("_") and t != "alembic_version"]


def _sql_literal(val: Any) -> str:
    """将 Python 值转为 SQL 字面量（INSERT 用）。"""
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "1" if val else "0"
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, datetime):
        return "'" + val.strftime("%Y-%m-%d %H:%M:%S") + "'"
    s = str(val)
    return "'" + s.replace("\\", "\\\\").replace("'", "''") + "'"


async def _export_mysql_to_temp(temp_dir: str, content_list: List[str]) -> str:
    """导出 MySQL/SQLite 到临时目录 mysql/backup.sql.gz。规格 4.5.3。若 content 不含 mysql 返回空串。"""
    if "mysql" not in content_list:
        return ""
    engine = get_engine()
    tables = await _get_table_names(engine)
    if "system_config" in content_list or "ai_config" in content_list:
        for t in ("config_entries", "ai_configs"):
            if t not in tables:
                tables.append(t)
    mysql_dir = os.path.join(temp_dir, "mysql")
    os.makedirs(mysql_dir, exist_ok=True)
    sql_path = os.path.join(mysql_dir, "backup.sql")
    gz_path = os.path.join(mysql_dir, "backup.sql.gz")
    with open(sql_path, "w", encoding="utf-8") as f:
        async with engine.connect() as conn:
            for table_name in tables:
                try:
                    q = text(f"SELECT * FROM {_quote_identifier(engine, table_name)}")
                    r = await conn.execute(q)
                    rows = r.fetchall()
                    keys = list(r.keys())
                    for row in rows:
                        row_map = row._mapping if hasattr(row, "_mapping") else dict(zip(keys, row))
                        vals = [_sql_literal(row_map[k]) for k in keys]
                        cols = ", ".join(_quote_identifier(engine, k) for k in keys)
                        f.write(f"INSERT INTO {_quote_identifier(engine, table_name)} ({cols}) VALUES ({', '.join(vals)});\n")
                except Exception as exc:
                    logger.warning("backup table %s failed: %s", table_name, exc)
    with open(sql_path, "rb") as src:
        with gzip.open(gz_path, "wb") as dst:
            dst.write(src.read())
    try:
        os.remove(sql_path)
    except OSError:
        pass
    return "mysql/backup.sql.gz"


async def check_binlog_for_incremental() -> dict:
    """
    检测当前 MySQL 是否开启 binlog，用于判断是否具备增量备份条件。
    返回 {"binlog_enabled": bool, "message": str, "database_type": str, "log_bin_value": str}，
    供备份策略页「检测增量备份条件」使用；database_type、log_bin_value 便于排查连接与检测结果。
    """
    engine = get_engine()
    dialect_name = engine.dialect.name if engine.dialect else "unknown"
    if dialect_name != "mysql":
        return {
            "binlog_enabled": False,
            "message": "当前数据库非 MySQL，仅支持全量备份。",
            "database_type": dialect_name,
            "log_bin_value": "",
        }
    try:
        async with engine.connect() as conn:
            r = await conn.execute(text("SHOW VARIABLES LIKE 'log_bin'"))
            row = r.fetchone()
        if not row:
            log_bin = ""
        else:
            row_map = row._mapping if hasattr(row, "_mapping") else None
            if row_map is not None:
                log_bin = (row_map.get("Value") or row_map.get("value") or "").strip().upper()
            else:
                log_bin = (row[1] or "").strip().upper() if len(row) > 1 else ""
    except Exception as exc:
        logger.warning("check_binlog_for_incremental failed: %s", exc)
        return {
            "binlog_enabled": False,
            "message": f"检测失败：{str(exc)[:200]}",
            "database_type": dialect_name,
            "log_bin_value": "",
        }
    if log_bin == "ON":
        return {
            "binlog_enabled": True,
            "message": "MySQL Binlog 已开启，支持增量备份。",
            "database_type": "mysql",
            "log_bin_value": log_bin,
        }
    return {
        "binlog_enabled": False,
        "message": "MySQL Binlog 未开启，仅支持全量备份。请在 MySQL 配置文件（my.ini / my.cnf）的 [mysqld] 下添加 log_bin、server_id 等并重启服务。",
        "database_type": "mysql",
        "log_bin_value": log_bin or "(空)",
    }


def check_redis_backup_readiness() -> dict:
    """
    检测 Redis 是否具备备份条件：REDIS_RDB_PATH 已配置且可访问，或通过扫描配置得到容器名并用 docker inspect 解析出 RDB 路径。
    返回 {"ready": bool, "message": str, "redis_container": str | None, "suggested_rdb_path": str | None, "source": str}。
    """
    rdb_path = os.getenv("REDIS_RDB_PATH", "").strip()
    if rdb_path:
        if os.path.isfile(rdb_path):
            return {
                "ready": True,
                "message": "已配置 REDIS_RDB_PATH 且文件存在，可备份 Redis。",
                "redis_container": None,
                "suggested_rdb_path": rdb_path,
                "source": "REDIS_RDB_PATH",
            }
        parent = os.path.dirname(rdb_path)
        if os.path.isdir(parent):
            return {
                "ready": True,
                "message": "已配置 REDIS_RDB_PATH，目录存在（RDB 将在备份时由 BGSAVE 生成）。",
                "redis_container": None,
                "suggested_rdb_path": rdb_path,
                "source": "REDIS_RDB_PATH",
            }
        return {
            "ready": False,
            "message": f"REDIS_RDB_PATH 已设置为 {rdb_path}，但路径不存在或不可访问。",
            "redis_container": None,
            "suggested_rdb_path": rdb_path,
            "source": "REDIS_RDB_PATH",
        }
    redis_name, _ = _scan_compose_for_redis_qdrant()
    if redis_name:
        suggested = _redis_rdb_path_from_docker(redis_name)
        if suggested and os.path.isfile(suggested):
            return {
                "ready": True,
                "message": f"已从配置识别 Redis 容器 {redis_name}，并解析出 RDB 路径，可备份。",
                "redis_container": redis_name,
                "suggested_rdb_path": suggested,
                "source": "docker",
            }
        if suggested:
            return {
                "ready": False,
                "message": f"已从配置识别 Redis 容器 {redis_name}，解析出路径 {suggested}，但当前文件不存在（可能尚未 BGSAVE）。可设置 REDIS_RDB_PATH={suggested} 后备份。",
                "redis_container": redis_name,
                "suggested_rdb_path": suggested,
                "source": "docker",
            }
        return {
            "ready": False,
            "message": f"已从配置识别 Redis 容器 {redis_name}，但无法解析宿主机 RDB 路径（需卷挂载 /data）。请设置环境变量 REDIS_RDB_PATH 为宿主机上 dump.rdb 的路径。",
            "redis_container": redis_name,
            "suggested_rdb_path": None,
            "source": "docker",
        }
    return {
        "ready": False,
        "message": "未配置 REDIS_RDB_PATH，且未在 deploy 配置中识别到 Redis 容器。请设置 REDIS_RDB_PATH 或使用含 redis 服务的 docker-compose。",
        "redis_container": None,
        "suggested_rdb_path": None,
        "source": "none",
    }


async def check_qdrant_backup_readiness() -> dict:
    """
    检测 Qdrant 是否具备备份条件：VECTOR_STORE_URL 可访问且能列出 collections。
    返回 {"ready": bool, "message": str, "url": str, "collections_count": int}。
    """
    import httpx
    url = os.getenv("VECTOR_STORE_URL", "http://127.0.0.1:6333").rstrip("/")
    _, qdrant_name = _scan_compose_for_redis_qdrant()
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(f"{url}/collections")
        if r.status_code != 200:
            return {
                "ready": False,
                "message": f"Qdrant 服务 {url} 返回 HTTP {r.status_code}，无法备份。",
                "url": url,
                "collections_count": 0,
            }
        data = r.json()
        collections = (data.get("result") or {}).get("collections") or []
        count = len(collections)
        if count == 0:
            return {
                "ready": True,
                "message": f"Qdrant 服务可访问（{url}），当前无集合，备份将为空。",
                "url": url,
                "collections_count": 0,
            }
        return {
            "ready": True,
            "message": f"Qdrant 服务可访问（{url}），共 {count} 个集合，可备份。",
            "url": url,
            "collections_count": count,
            "qdrant_container": qdrant_name,
        }
    except Exception as exc:
        return {
            "ready": False,
            "message": f"无法连接 Qdrant（{url}）：{str(exc)[:150]}。请确认 VECTOR_STORE_URL 与容器（如 {qdrant_name or 'qdrant'}）已启动。",
            "url": url,
            "collections_count": 0,
            "qdrant_container": qdrant_name,
        }


async def check_clickhouse_backup_readiness() -> dict:
    """
    检测 ClickHouse 是否具备备份条件：可连接且 market_kline 表存在。
    返回 {"ready": bool, "message": str, "url": str, "market_kline_exists": bool | None}。
    """
    import httpx
    ch_url = os.getenv("CLICKHOUSE_URL", "http://localhost:8123").rstrip("/")
    ch_db = os.getenv("CLICKHOUSE_DB", "default")
    ch_user = os.getenv("CLICKHOUSE_USER", "default")
    ch_password = os.getenv("CLICKHOUSE_PASSWORD", "")
    auth = (ch_user, ch_password) if ch_user else None
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.post(
                f"{ch_url}/",
                params={"database": ch_db, "query": "SELECT 1"},
                auth=auth,
            )
            if r.status_code != 200:
                return {
                    "ready": False,
                    "message": f"ClickHouse 服务 {ch_url} 返回 HTTP {r.status_code}，无法备份。",
                    "url": ch_url,
                    "market_kline_exists": None,
                }
            r2 = await client.post(
                f"{ch_url}/",
                params={
                    "database": ch_db,
                    "query": "SELECT count() FROM system.tables WHERE database=currentDatabase() AND name='market_kline'",
                },
                auth=auth,
            )
            if r2.status_code != 200:
                return {
                    "ready": False,
                    "message": f"ClickHouse 查询 system.tables 失败（HTTP {r2.status_code}）。",
                    "url": ch_url,
                    "market_kline_exists": None,
                }
            count = int(r2.text.strip()) if r2.text else 0
            market_kline_exists = count > 0
            if market_kline_exists:
                return {
                    "ready": True,
                    "message": f"ClickHouse 可访问（{ch_url}），market_kline 表存在，可备份。",
                    "url": ch_url,
                    "market_kline_exists": True,
                }
            return {
                "ready": False,
                "message": f"ClickHouse 可访问（{ch_url}），但 market_kline 表不存在，无法备份。",
                "url": ch_url,
                "market_kline_exists": False,
            }
    except Exception as exc:
        return {
            "ready": False,
            "message": f"无法连接 ClickHouse（{ch_url}）：{str(exc)[:150]}。请确认 CLICKHOUSE_URL 已配置且服务已启动。",
            "url": ch_url,
            "market_kline_exists": None,
        }


async def check_backup_readiness() -> dict:
    """
    汇总各组件备份条件：MySQL Binlog、Redis、Qdrant、ClickHouse。
    供「检测备份条件」使用，返回 { "binlog": {...}, "redis": {...}, "qdrant": {...}, "clickhouse": {...} }。
    """
    binlog_result = await check_binlog_for_incremental()
    redis_result = check_redis_backup_readiness()
    qdrant_result = await check_qdrant_backup_readiness()
    clickhouse_result = await check_clickhouse_backup_readiness()
    return {
        "binlog": binlog_result,
        "redis": redis_result,
        "qdrant": qdrant_result,
        "clickhouse": clickhouse_result,
    }


async def _get_mysql_binlog_status() -> dict | None:
    """全量备份后获取当前 binlog 位置，用于后续增量。未开启 binlog 时返回 None。"""
    engine = get_engine()
    if engine.dialect.name != "mysql":
        return None
    try:
        async with engine.connect() as conn:
            await conn.execute(text("FLUSH BINARY LOGS"))
            await conn.commit()
            r = await conn.execute(text("SHOW MASTER STATUS"))
            row = r.fetchone()
            if not row:
                return None
            # Row: File, Position, Binlog_Do_DB, Binlog_Ignore_DB, ...
            row_map = row._mapping if hasattr(row, "_mapping") else dict(zip(r.keys(), row))
            file_name = row_map.get("File") or row_map.get("file")
            position = row_map.get("Position") or row_map.get("position")
            if file_name and position is not None:
                return {"file": str(file_name), "position": int(position)}
    except Exception as exc:
        logger.debug("binlog status not available: %s", exc)
    return None


def _read_parent_manifest_binlog(parent_id: str) -> dict | None:
    """从父备份 zip 中读取 manifest.json 的 binlog 信息，用于增量导出起点。"""
    zip_path = os.path.join(BACKUP_DIR, f"{parent_id}.zip")
    if not os.path.isfile(zip_path):
        return None
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            with zf.open("manifest.json") as f:
                manifest = json.load(f)
        return manifest.get("binlog")
    except Exception as exc:
        logger.warning("read parent manifest binlog failed: %s", exc)
    return None


def _export_mysql_incremental_binlog_to_temp(
    temp_dir: str, binlog_start: dict, dsn: str
) -> bool:
    """
    使用 mysqlbinlog 从 binlog_start 位置导出到当前，写入 mysql/incremental.sql.gz。
    dsn 为同步 URL（如 mysql://user:pass@host:port/db）。成功返回 True，失败返回 False。
    """
    mysql_dir = os.path.join(temp_dir, "mysql")
    os.makedirs(mysql_dir, exist_ok=True)
    out_path = os.path.join(mysql_dir, "incremental.sql")
    gz_path = os.path.join(mysql_dir, "incremental.sql.gz")
    file_name = binlog_start.get("file")
    position = binlog_start.get("position")
    if not file_name or position is None:
        return False
    # mysqlbinlog --read-from-remote-server -h host -u user -p pass --start-position= N file
    # 解析 DSN: mysql://user:pass@host:port/db -> host, port, user, password
    try:
        from urllib.parse import urlparse
        parsed = urlparse(dsn.replace("mysql+asyncmy://", "mysql://").replace("mysql+pymysql://", "mysql://"))
        host = parsed.hostname or "localhost"
        port = parsed.port or 3306
        user = parsed.username or "root"
        password = parsed.password or ""
    except Exception:
        return False
    try:
        cmd = [
            "mysqlbinlog",
            "--read-from-remote-server",
            f"--host={host}",
            f"--port={port}",
            f"--user={user}",
            f"--start-position={position}",
            file_name,
        ]
        env = os.environ.copy()
        if password:
            env["MYSQL_PWD"] = password
        with open(out_path, "wb") as f:
            proc = subprocess.run(
                cmd,
                env=env,
                stdout=f,
                stderr=subprocess.PIPE,
                timeout=3600,
            )
        if proc.returncode != 0:
            logger.warning("mysqlbinlog failed: %s", proc.stderr.decode(errors="replace")[:500])
            return False
        if os.path.getsize(out_path) == 0:
            os.remove(out_path)
            # 无新数据时仍写空 gz，便于 manifest 一致
            with gzip.open(gz_path, "wb") as dst:
                pass
        else:
            with open(out_path, "rb") as src:
                with gzip.open(gz_path, "wb") as dst:
                    dst.write(src.read())
            os.remove(out_path)
        return True
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as exc:
        logger.warning("mysqlbinlog export failed: %s", exc)
        return False


async def _export_mysql_or_sqlite(engine: Any, content_list: List[str]) -> dict:
    """导出 MySQL 或 SQLite 表数据（content 含 mysql 时）"""
    out = {}
    if "mysql" not in content_list:
        return out
    tables = await _get_table_names(engine)
    async with engine.connect() as conn:
        for table_name in tables:
            try:
                q = text(f"SELECT * FROM {_quote_identifier(engine, table_name)}")
                r = await conn.execute(q)
                rows = r.fetchall()
                keys = list(r.keys())
                out[table_name] = [dict(zip(keys, row)) for row in rows]
            except Exception as exc:
                logger.warning("backup table %s failed: %s", table_name, exc)
                out[table_name] = {"error": str(exc)}
    return out


def _config_files_base_path() -> str:
    """配置备份文件基准路径：backend 目录"""
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _project_root() -> str:
    """项目根目录（backend 的上一级）。规格 2.2 配置路径相对项目根。"""
    return os.path.dirname(_config_files_base_path())


# 规格 2.2：首轮至少这些路径（相对项目根）
CONFIG_BACKUP_PATHS = [
    "backend/.env.example",
    "backend/config/dev.yaml",
    "backend/config/prod.yaml",
    "deploy/.env.example",
]


def _copy_config_files_to_temp(temp_dir: str, content_list: List[str]) -> List[str]:
    """按 2.2 路径列表拷贝配置文件到 temp_dir/config/，保持相对路径。规格 4.5.4。"""
    if "system_config" not in content_list and "ai_config" not in content_list:
        return []
    base = _project_root()
    config_dir = os.path.join(temp_dir, "config")
    copied = []
    for rel in CONFIG_BACKUP_PATHS:
        src = os.path.join(base, rel)
        if not os.path.isfile(src):
            continue
        dest = os.path.join(config_dir, rel)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        try:
            shutil.copy2(src, dest)
            copied.append(rel)
        except Exception as exc:
            logger.warning("backup config file %s failed: %s", rel, exc)
    return copied


async def _export_redis_to_temp(temp_dir: str) -> str | None:
    """
    Redis RDB 快照：BGSAVE 后从 REDIS_RDB_PATH 拷贝到 temp_dir/redis/dump.rdb。
    规格 4.4.2。未配置 REDIS_RDB_PATH 时尝试从 deploy 配置识别 Redis 容器并用 docker inspect 解析路径。
    """
    rdb_path = os.getenv("REDIS_RDB_PATH", "").strip()
    if not rdb_path:
        readiness = check_redis_backup_readiness()
        rdb_path = (readiness.get("suggested_rdb_path") or "").strip()
    if not rdb_path:
        logger.warning("Redis backup skipped: REDIS_RDB_PATH not set and no path from config/docker")
        return None
    try:
        from src.core.streams import get_redis_client
        client = await get_redis_client()
        await client.bgsave()
        # 轮询 LASTSAVE 等待 BGSAVE 完成（最多约 60 秒）
        last_save = await client.lastsave()
        for _ in range(60):
            await asyncio.sleep(1)
            current = await client.lastsave()
            if current and current != last_save:
                break
        redis_dir = os.path.join(temp_dir, "redis")
        os.makedirs(redis_dir, exist_ok=True)
        dest = os.path.join(redis_dir, "dump.rdb")
        if not os.path.isfile(rdb_path):
            logger.warning("Redis backup skipped: RDB file not found at %s", rdb_path)
            return None
        shutil.copy2(rdb_path, dest)
        return "redis/dump.rdb"
    except Exception as exc:
        logger.warning("Redis backup failed: %s", exc)
        return None


async def _export_qdrant_to_temp(temp_dir: str) -> Tuple[str | None, List[dict]]:
    """
    Qdrant：列出 collections，每 collection 创建 snapshot 并下载到 temp_dir/qdrant/snapshots/。
    规格 4.4.2。返回 (paths_info 值 "qdrant/snapshots", qdrant_snapshots 列表供 manifest)。
    失败或无集合时返回 (None, [])。
    """
    import httpx
    qdrant_url = os.getenv("VECTOR_STORE_URL", "http://127.0.0.1:6333").rstrip("/")
    qdrant_dir = os.path.join(temp_dir, "qdrant", "snapshots")
    os.makedirs(qdrant_dir, exist_ok=True)
    qdrant_snapshots: List[dict] = []
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.get(f"{qdrant_url}/collections")
            if r.status_code != 200:
                logger.warning("Qdrant list collections failed: %s", r.status_code)
                return None, []
            data = r.json()
            collections = (data.get("result") or {}).get("collections") or []
            for col in collections:
                name = col.get("name") if isinstance(col, dict) else str(col)
                if not name:
                    continue
                try:
                    create_r = await client.post(
                        f"{qdrant_url}/collections/{name}/snapshots",
                        params={"wait": "true"},
                    )
                    if create_r.status_code != 200:
                        logger.warning("Qdrant create snapshot %s failed: %s", name, create_r.status_code)
                        continue
                    snap_data = create_r.json()
                    snap_result = snap_data.get("result") or {}
                    snapshot_name = snap_result.get("name") or ""
                    if not snapshot_name:
                        continue
                    dl_r = await client.get(
                        f"{qdrant_url}/collections/{name}/snapshots/{snapshot_name}",
                    )
                    if dl_r.status_code != 200:
                        logger.warning("Qdrant download snapshot %s failed: %s", name, dl_r.status_code)
                        continue
                    local_name = f"{name}_{snapshot_name}.snapshot"
                    local_path = os.path.join(qdrant_dir, local_name)
                    with open(local_path, "wb") as f:
                        f.write(dl_r.content)
                    rel_path = f"qdrant/snapshots/{local_name}"
                    qdrant_snapshots.append({"collection": name, "path": rel_path})
                except Exception as exc:
                    logger.warning("Qdrant snapshot %s failed: %s", name, exc)
        return ("qdrant/snapshots" if qdrant_snapshots else None, qdrant_snapshots)
    except Exception as exc:
        logger.warning("Qdrant backup failed: %s", exc)
        return None, []


async def _export_clickhouse_to_temp(temp_dir: str) -> str | None:
    """
    ClickHouse：导出 market_kline 为 TSV，gzip 到 temp_dir/clickhouse/backup.tsv.gz。规格 4.4.2。
    """
    import httpx
    ch_url = os.getenv("CLICKHOUSE_URL", "http://localhost:8123").rstrip("/")
    ch_db = os.getenv("CLICKHOUSE_DB", "default")
    ch_user = os.getenv("CLICKHOUSE_USER", "default")
    ch_password = os.getenv("CLICKHOUSE_PASSWORD", "")
    clickhouse_dir = os.path.join(temp_dir, "clickhouse")
    os.makedirs(clickhouse_dir, exist_ok=True)
    tsv_path = os.path.join(clickhouse_dir, "backup.tsv")
    gz_path = os.path.join(clickhouse_dir, "backup.tsv.gz")
    try:
        query = "SELECT * FROM market_kline FORMAT TabSeparatedWithNames"
        auth = (ch_user, ch_password) if ch_user else None
        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.post(
                f"{ch_url}/",
                params={"database": ch_db, "query": query},
                auth=auth,
            )
            if r.status_code != 200:
                logger.warning("ClickHouse export failed: %s %s", r.status_code, r.text[:200])
                return None
            with open(tsv_path, "wb") as f:
                f.write(r.content)
        with open(tsv_path, "rb") as src:
            with gzip.open(gz_path, "wb") as dst:
                dst.write(src.read())
        try:
            os.remove(tsv_path)
        except OSError:
            pass
        return "clickhouse/backup.tsv.gz"
    except Exception as exc:
        logger.warning("ClickHouse backup failed: %s", exc)
        if os.path.isfile(tsv_path):
            try:
                os.remove(tsv_path)
            except OSError:
                pass
        return None


def _write_manifest(
    temp_dir: str,
    backup_id: str,
    tenant_id: str,
    content_list: List[str],
    description: str | None,
    paths_info: dict,
    backup_type: str = "full",
    parent_id: str | None = None,
    binlog_info: dict | None = None,
    qdrant_snapshots: List[dict] | None = None,
) -> None:
    """写入 temp_dir/manifest.json。规格 4.4.1、4.5.5；含 type、parent_id、binlog、qdrant_snapshots。"""
    obj = {
        "backup_id": backup_id,
        "tenant_id": tenant_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "type": backup_type,
        "content": content_list,
        "description": description,
        "paths": paths_info,
    }
    if parent_id:
        obj["parent_id"] = parent_id
    if binlog_info:
        obj["binlog"] = binlog_info
    if qdrant_snapshots:
        obj["qdrant_snapshots"] = qdrant_snapshots
    path = os.path.join(temp_dir, "manifest.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _zip_temp_dir_to_backup_dir(temp_dir: str, backup_id: str) -> Tuple[str, int]:
    """将 temp_dir 打包为 BACKUP_DIR/{backup_id}.zip，返回 (zip 绝对路径, size_bytes)。规格 4.5.6"""
    zip_path = os.path.join(BACKUP_DIR, f"{backup_id}.zip")
    zip_path = os.path.abspath(zip_path)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _dirs, files in os.walk(temp_dir):
            for name in files:
                full = os.path.join(root, name)
                arcname = os.path.relpath(full, temp_dir)
                zf.write(full, arcname)
    size = os.path.getsize(zip_path)
    return zip_path, size


def _export_config_files(content_list: List[str]) -> dict:
    """导出约定配置文件（system_config 时）；路径相对 backend"""
    out = {}
    if "system_config" not in content_list and "ai_config" not in content_list:
        return out
    base = _config_files_base_path()
    # 规格 2.2：首轮至少 backend/.env.example, backend/config/dev.yaml, backend/config/prod.yaml
    candidates = [
        ".env.example",
        "config/dev.yaml",
        "config/prod.yaml",
    ]
    for rel in candidates:
        path = os.path.join(base, rel)
        if os.path.isfile(path):
            try:
                with open(path, "r", encoding="utf-8", errors="replace") as f:
                    out[rel] = f.read()
            except Exception as exc:
                out[rel] = f"[read error: {exc}]"
    return out


async def _export_config_tables(engine: Any, content_list: List[str]) -> dict:
    """导出 config_entries、ai_configs（若在 content 中）"""
    out = {}
    if "system_config" in content_list:
        # config_entries
        try:
            q = text("SELECT * FROM config_entries")
            async with engine.connect() as conn:
                r = await conn.execute(q)
                rows = r.fetchall()
                out["config_entries"] = [dict(zip(r.keys(), row)) for row in rows]
        except Exception as exc:
            logger.warning("backup config_entries failed: %s", exc)
            out["config_entries"] = {"error": str(exc)}
    if "ai_config" in content_list:
        try:
            q = text("SELECT * FROM ai_configs")
            async with engine.connect() as conn:
                r = await conn.execute(q)
                rows = r.fetchall()
                out["ai_configs"] = [dict(zip(r.keys(), row)) for row in rows]
        except Exception as exc:
            logger.warning("backup ai_configs failed: %s", exc)
            out["ai_configs"] = {"error": str(exc)}
    return out


# 步骤进度：0 开始 → 17,33,50,67,83 各步 → 100 完成
_BACKUP_PROGRESS = [0, 17, 33, 50, 67, 83, 100]


def _backup_status_to_level(status: str) -> str:
    """与预热/代理日志 level 一致：INFO/WARN/ERROR"""
    return "ERROR" if status == "failed" else ("WARN" if status == "warn" else "INFO")


async def _append_backup_log(
    backup_id: str,
    step: int,
    message: str,
    status: str,
    progress_percent: int,
) -> None:
    """更新备份的 progress_percent 并追加一条步骤日志。格式与预热/代理 log_entries 一致：含 ts、level、msg。"""
    from src.core.db import get_session

    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    level = _backup_status_to_level(status)
    entry = {"step": step, "message": message, "status": status, "level": level, "ts": ts}
    async for session in get_session():
        b = await session.get(Backup, backup_id)
        if not b:
            break
        existing = []
        if b.log_entries:
            try:
                existing = json.loads(b.log_entries)
                if not isinstance(existing, list):
                    existing = []
            except (json.JSONDecodeError, TypeError):
                existing = []
        existing.append(entry)
        b.log_entries = json.dumps(existing, ensure_ascii=False)
        b.progress_percent = progress_percent
        await session.commit()
        break


async def process_backup(backup_id: str, tenant_id: str):
    """按 content 导出 → 临时目录 → manifest → zip 落盘。规格 4.5.7"""
    from src.core.db import get_session

    async for session in get_session():
        backup = await session.get(Backup, backup_id)
        if not backup:
            return
        if backup.status != BackupStatus.PENDING:
            return
        backup.status = BackupStatus.IN_PROGRESS
        backup.progress_percent = _BACKUP_PROGRESS[0]
        await session.commit()
        break
    else:
        return

    await _append_backup_log(backup_id, 0, "开始备份", "success", _BACKUP_PROGRESS[0])

    temp_dir = ""
    try:
        content_list = json.loads(backup.content) if backup.content else ["mysql", "ai_config", "system_config"]
        destination = backup.destination or "local"
        os.makedirs(BACKUP_DIR, exist_ok=True)
        temp_dir = _ensure_temp_backup_dir(backup_id)
        await _append_backup_log(backup_id, 1, "创建临时目录完成", "success", _BACKUP_PROGRESS[1])

        binlog_info: dict | None = None
        mysql_path = "mysql/backup.sql.gz" if "mysql" in content_list else None
        if "mysql" in content_list:
            if (backup.type or "full") == "incremental" and backup.parent_id:
                parent_binlog = _read_parent_manifest_binlog(backup.parent_id)
                dsn = os.getenv("MYSQL_DSN", "").replace("mysql+asyncmy://", "mysql://").replace("mysql+pymysql://", "mysql://")
                if parent_binlog and dsn and _export_mysql_incremental_binlog_to_temp(temp_dir, parent_binlog, dsn):
                    mysql_path = "mysql/incremental.sql.gz"
                else:
                    await _export_mysql_to_temp(temp_dir, content_list)
            else:
                await _export_mysql_to_temp(temp_dir, content_list)
                if (backup.type or "full") == "full":
                    binlog_info = await _get_mysql_binlog_status()
        await _append_backup_log(backup_id, 2, "MySQL 导出完成", "success", _BACKUP_PROGRESS[2])

        config_copied = _copy_config_files_to_temp(temp_dir, content_list)
        await _append_backup_log(backup_id, 3, "配置文件备份完成", "success", _BACKUP_PROGRESS[3])

        redis_path: str | None = None
        if "redis" in content_list:
            redis_path = await _export_redis_to_temp(temp_dir)
            if redis_path:
                await _append_backup_log(backup_id, 3, "Redis 快照完成", "success", _BACKUP_PROGRESS[3])
            else:
                await _append_backup_log(backup_id, 3, "Redis 快照跳过(未配置或失败)", "failed", _BACKUP_PROGRESS[3])

        qdrant_path: str | None = None
        qdrant_snapshots: List[dict] = []
        if "qdrant" in content_list:
            qdrant_path, qdrant_snapshots = await _export_qdrant_to_temp(temp_dir)
            if qdrant_path:
                await _append_backup_log(backup_id, 3, "Qdrant 快照完成", "success", _BACKUP_PROGRESS[3])
            else:
                await _append_backup_log(backup_id, 3, "Qdrant 快照跳过(无集合或失败)", "failed", _BACKUP_PROGRESS[3])

        clickhouse_path: str | None = None
        if "clickhouse" in content_list:
            clickhouse_path = await _export_clickhouse_to_temp(temp_dir)
            if clickhouse_path:
                await _append_backup_log(backup_id, 3, "ClickHouse 导出完成", "success", _BACKUP_PROGRESS[3])
            else:
                await _append_backup_log(backup_id, 3, "ClickHouse 导出跳过(不可用或失败)", "failed", _BACKUP_PROGRESS[3])

        paths_info = {"mysql": mysql_path, "config": config_copied}
        if redis_path:
            paths_info["redis"] = redis_path
        if qdrant_path:
            paths_info["qdrant"] = qdrant_path
        if clickhouse_path:
            paths_info["clickhouse"] = clickhouse_path
        _write_manifest(
            temp_dir, backup_id, tenant_id, content_list, None, paths_info,
            backup_type=backup.type or "full",
            parent_id=backup.parent_id,
            binlog_info=binlog_info,
            qdrant_snapshots=qdrant_snapshots if qdrant_snapshots else None,
        )
        await _append_backup_log(backup_id, 4, "manifest 生成完成", "success", _BACKUP_PROGRESS[4])

        zip_path, size_bytes = _zip_temp_dir_to_backup_dir(temp_dir, backup_id)
        await _append_backup_log(backup_id, 5, "打包 zip 落盘完成", "success", _BACKUP_PROGRESS[5])

        async for s in get_session():
            b = await s.get(Backup, backup_id)
            if b:
                if destination in ("aliyundrive", "baidupan"):
                    b.error_detail = f"云盘上传({destination})功能(二期)，当前仅落盘本地"
                b.status = BackupStatus.SUCCESS
                b.location = zip_path
                b.size_bytes = size_bytes
                b.completed_at = datetime.utcnow()
                b.progress_percent = _BACKUP_PROGRESS[6]
                entries = []
                if b.log_entries:
                    try:
                        entries = json.loads(b.log_entries)
                        if not isinstance(entries, list):
                            entries = []
                    except (json.JSONDecodeError, TypeError):
                        entries = []
                ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                entries.append({"step": 6, "message": "完成", "status": "success", "level": "INFO", "ts": ts})
                b.log_entries = json.dumps(entries, ensure_ascii=False)
                await s.commit()
            break
        logger.info("backup %s completed, size=%d bytes", backup_id, size_bytes)
    except Exception as exc:
        logger.exception("process_backup failed: %s", exc)
        ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        try:
            async for s in get_session():
                b = await s.get(Backup, backup_id)
                if b:
                    b.status = BackupStatus.FAILED
                    b.error_detail = str(exc)[:1000]
                    b.progress_percent = _BACKUP_PROGRESS[6]
                    entries = []
                    if b.log_entries:
                        try:
                            entries = json.loads(b.log_entries)
                            if not isinstance(entries, list):
                                entries = []
                        except (json.JSONDecodeError, TypeError):
                            entries = []
                    entries.append({"step": 6, "message": "失败", "status": "failed", "level": "ERROR", "ts": ts})
                    b.log_entries = json.dumps(entries, ensure_ascii=False)
                    await s.commit()
                break
        except Exception:
            pass
    finally:
        if temp_dir:
            _remove_temp_backup_dir(backup_id)


PAGE_SIZE_MAX = 1000  # page_size=0 表示「全部」时的上限


async def get_backups(
    session: AsyncSession,
    tenant_id: str,
    skip: int = 0,
    limit: int = 15,
) -> Tuple[List[Backup], int]:
    """返回 (当前页列表, 总条数)。limit=0 时按 PAGE_SIZE_MAX 取全部。"""
    base = select(Backup).where(Backup.tenant_id == tenant_id).order_by(Backup.created_at.desc())
    # 总数（同条件）
    total_result = await session.execute(
        select(func.count()).select_from(Backup).where(Backup.tenant_id == tenant_id)
    )
    total = total_result.scalar() or 0
    # 分页列表
    effective_limit = limit if limit > 0 else PAGE_SIZE_MAX
    query = base.offset(skip).limit(effective_limit)
    result = await session.execute(query)
    items = list(result.scalars().all())
    return items, total


async def delete_backup_by_id(
    session: AsyncSession,
    tenant_id: str,
    backup_id: str,
) -> bool:
    """删除单条备份记录及本地文件（若存在）。校验租户，返回是否找到并删除。"""
    backup = await session.get(Backup, backup_id)
    if not backup or backup.tenant_id != tenant_id:
        return False
    if backup.location and os.path.isfile(backup.location):
        try:
            os.remove(backup.location)
        except OSError as exc:
            logger.warning("remove backup file %s failed: %s", backup.location, exc)
    await session.delete(backup)
    await session.commit()
    return True


async def cleanup_expired_backups(
    session: AsyncSession,
    tenant_id: str,
    retention_days: int,
) -> List[str]:
    """按保留天数删除过期备份记录及本地文件，返回被删除的 backup_id 列表"""
    from datetime import timedelta

    if retention_days < 1:
        return []
    cutoff = datetime.utcnow() - timedelta(days=retention_days)
    query = (
        select(Backup)
        .where(Backup.tenant_id == tenant_id)
        .where(Backup.completed_at.isnot(None))
        .where(Backup.completed_at < cutoff)
    )
    result = await session.execute(query)
    to_delete = result.scalars().all()
    deleted_ids = []
    for backup in to_delete:
        if backup.location and os.path.isfile(backup.location):
            try:
                os.remove(backup.location)
            except OSError as exc:
                logger.warning("remove backup file %s failed: %s", backup.location, exc)
        deleted_ids.append(backup.id)
        await session.delete(backup)
    if deleted_ids:
        await session.commit()
        logger.info("cleanup_expired_backups tenant=%s retention_days=%s deleted=%s", tenant_id, retention_days, deleted_ids)
    return deleted_ids


async def _restore_tables_from_payload(engine: Any, conn: Any, tables_payload: dict) -> tuple[List[str], List[str]]:
    """在给定连接上恢复表；返回 (restored, skipped)。调用方需在 engine.begin() 内调用。"""
    restored = []
    skipped = []
    for table_name, table_data in tables_payload.items():
        if isinstance(table_data, dict) and "error" in table_data:
            skipped.append(table_name)
            continue
        if not isinstance(table_data, list) or len(table_data) == 0:
            skipped.append(table_name)
            continue
        try:
            q_del = text(f"DELETE FROM {_quote_identifier(engine, table_name)}")
            await conn.execute(q_del)
            for row in table_data:
                cols = ", ".join(_quote_identifier(engine, k) for k in row.keys())
                placeholders = ", ".join(f":{k}" for k in row.keys())
                sql = f"INSERT INTO {_quote_identifier(engine, table_name)} ({cols}) VALUES ({placeholders})"
                await conn.execute(text(sql), row)
            restored.append(table_name)
        except Exception as exc:
            logger.warning("restore table %s failed: %s", table_name, exc)
            skipped.append(table_name)
    return restored, skipped


async def _get_restore_chain(session: AsyncSession, backup_id: str, tenant_id: str) -> List[Backup]:
    """从目标备份递归到全量，返回 [全量, 增量1, ..., 目标]，用于按序恢复。"""
    chain: List[Backup] = []
    current_id: str | None = backup_id
    while current_id:
        b = await session.get(Backup, current_id)
        if not b or b.tenant_id != tenant_id:
            raise ValueError(f"备份不存在或无权访问: {current_id}")
        if b.status != BackupStatus.SUCCESS:
            raise ValueError(f"备份状态不可恢复: {b.status.value}")
        chain.append(b)
        current_id = b.parent_id
    chain.reverse()
    return chain


async def _apply_one_zip_mysql_and_config(
    extract_dir: str,
    manifest: dict,
    restore_mode: str,
    restored_tables: List[str],
    skipped_tables: List[str],
    restored_files: List[str],
    restored_redis: List[str] | None = None,
    restored_qdrant: List[str] | None = None,
    restored_clickhouse: List[str] | None = None,
) -> None:
    """对已解压的 zip 目录按 manifest 应用 MySQL、配置文件、Redis、Qdrant、ClickHouse。"""
    content_list = manifest.get("content") or []
    paths = manifest.get("paths") or {}
    engine = get_engine()
    if restore_mode in ("both", "db_only") and "mysql" in content_list and paths.get("mysql"):
        sql_gz = os.path.join(extract_dir, paths["mysql"])
        if os.path.isfile(sql_gz):
            with gzip.open(sql_gz, "rt", encoding="utf-8", errors="replace") as gf:
                sql_text = gf.read()
            async with engine.begin() as conn:
                for stmt in sql_text.split(";\n"):
                    stmt = stmt.strip()
                    if stmt:
                        try:
                            await conn.execute(text(stmt))
                            if "INSERT INTO" in stmt.upper():
                                tbl = stmt.upper().split("INSERT INTO")[1].split("(")[0].strip().strip("[]`")
                                if tbl not in restored_tables:
                                    restored_tables.append(tbl)
                        except Exception as exc:
                            logger.warning("restore sql failed: %s", exc)
                            skipped_tables.append("(sql)")
    if restore_mode in ("both", "file_only") and content_list and paths.get("config"):
        base = _project_root()
        for rel in paths["config"]:
            src = os.path.join(extract_dir, "config", rel)
            if not os.path.isfile(src):
                continue
            dest = os.path.join(base, rel)
            try:
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                shutil.copy2(src, dest)
                if rel not in restored_files:
                    restored_files.append(rel)
            except Exception as exc:
                logger.warning("restore file %s failed: %s", rel, exc)

    # Redis：拷贝 RDB 到 REDIS_RDB_PATH
    if paths.get("redis") and (restored_redis is not None):
        rdb_rel = paths["redis"]
        src = os.path.join(extract_dir, rdb_rel)
        rdb_dest = os.getenv("REDIS_RDB_PATH", "").strip()
        if os.path.isfile(src) and rdb_dest:
            try:
                os.makedirs(os.path.dirname(rdb_dest), exist_ok=True)
                shutil.copy2(src, rdb_dest)
                restored_redis.append("redis")
            except Exception as exc:
                logger.warning("restore redis rdb failed: %s", exc)

    # Qdrant：按 qdrant_snapshots 或 paths["qdrant"] 恢复
    qdrant_snapshots = manifest.get("qdrant_snapshots") or []
    if not qdrant_snapshots and paths.get("qdrant"):
        snap_dir = os.path.join(extract_dir, paths["qdrant"])
        if os.path.isdir(snap_dir):
            for f in os.listdir(snap_dir):
                if f.endswith(".snapshot"):
                    base_name = f[:-len(".snapshot")]
                    if "_" in base_name:
                        coll = base_name.rsplit("_", 1)[0]
                        qdrant_snapshots.append({"collection": coll, "path": f"{paths['qdrant']}/{f}"})
    if qdrant_snapshots and restored_qdrant is not None:
        import httpx
        qdrant_url = os.getenv("VECTOR_STORE_URL", "http://127.0.0.1:6333").rstrip("/")
        for item in qdrant_snapshots:
            coll = item.get("collection")
            rel_path = item.get("path")
            if not coll or not rel_path:
                continue
            abs_path = os.path.join(extract_dir, rel_path)
            if not os.path.isfile(abs_path):
                logger.warning("Qdrant snapshot file not found: %s", rel_path)
                continue
            try:
                location = "file:///" + os.path.normpath(abs_path).replace("\\", "/")
                async with httpx.AsyncClient(timeout=120.0) as client:
                    r = await client.put(
                        f"{qdrant_url}/collections/{coll}/snapshots/recover",
                        json={"location": location, "wait": True},
                    )
                if r.status_code in (200, 201):
                    restored_qdrant.append(coll)
                else:
                    logger.warning("Qdrant recover %s failed: %s %s", coll, r.status_code, r.text[:200])
            except Exception as exc:
                logger.warning("Qdrant recover %s failed: %s", coll, exc)

    # ClickHouse：解压 TSV 并 INSERT
    if paths.get("clickhouse") and (restored_clickhouse is not None):
        ch_gz = os.path.join(extract_dir, paths["clickhouse"])
        if os.path.isfile(ch_gz):
            try:
                from src.services.data_service.kline_storage import ensure_kline_table
                await ensure_kline_table()
                import httpx
                ch_url = os.getenv("CLICKHOUSE_URL", "http://localhost:8123").rstrip("/")
                ch_db = os.getenv("CLICKHOUSE_DB", "default")
                ch_user = os.getenv("CLICKHOUSE_USER", "default")
                ch_password = os.getenv("CLICKHOUSE_PASSWORD", "")
                auth = (ch_user, ch_password) if ch_user else None
                with gzip.open(ch_gz, "rt", encoding="utf-8", errors="replace") as gf:
                    tsv_content = gf.read()
                if tsv_content.strip():
                    insert_query = "INSERT INTO market_kline FORMAT TabSeparatedWithNames"
                    async with httpx.AsyncClient(timeout=60.0) as client:
                        r = await client.post(
                            f"{ch_url}/",
                            params={"database": ch_db, "query": insert_query},
                            content=tsv_content,
                            auth=auth,
                        )
                    if r.status_code == 200:
                        restored_clickhouse.append("market_kline")
                    else:
                        logger.warning("ClickHouse restore failed: %s %s", r.status_code, r.text[:200])
                else:
                    restored_clickhouse.append("market_kline")
            except Exception as exc:
                logger.warning("ClickHouse restore failed: %s", exc)


async def restore_backup(
    session: AsyncSession,
    tenant_id: str,
    backup_id: str,
    restore_config: str | None = None,
):
    """
    恢复备份。restore_config: db_only | file_only | both（配置备份时生效）。
    若为增量备份，先按 parent 链恢复全量再按序应用增量。
    一期同步执行。
    """
    chain = await _get_restore_chain(session, backup_id, tenant_id)
    if not chain:
        raise ValueError("恢复链为空")

    restore_mode = restore_config or "both"
    restored_tables = []
    skipped_tables = []
    restored_files = []
    restored_redis: List[str] = []
    restored_qdrant: List[str] = []
    restored_clickhouse: List[str] = []

    for backup in chain:
        backup_path = backup.location
        if not backup_path or not os.path.exists(backup_path):
            raise FileNotFoundError(f"备份文件不存在: {backup_path}")

        if backup_path.endswith(".zip"):
            extract_dir = tempfile.mkdtemp(prefix="restore_")
            try:
                with zipfile.ZipFile(backup_path, "r") as zf:
                    zf.extractall(extract_dir)
                manifest_path = os.path.join(extract_dir, "manifest.json")
                if not os.path.isfile(manifest_path):
                    raise ValueError("zip 内缺少 manifest.json")
                with open(manifest_path, encoding="utf-8") as f:
                    manifest = json.load(f)
                await _apply_one_zip_mysql_and_config(
                    extract_dir, manifest, restore_mode,
                    restored_tables, skipped_tables, restored_files,
                    restored_redis=restored_redis,
                    restored_qdrant=restored_qdrant,
                    restored_clickhouse=restored_clickhouse,
                )
            finally:
                try:
                    shutil.rmtree(extract_dir)
                except OSError:
                    pass
        else:
            break
    else:
        logger.info(
            "restore_backup %s: restored_tables=%s, skipped_tables=%s, restored_files=%s, "
            "restored_redis=%s, restored_qdrant=%s, restored_clickhouse=%s",
            backup_id, restored_tables, skipped_tables, restored_files,
            restored_redis, restored_qdrant, restored_clickhouse,
        )
        return {
            "backup_id": backup_id,
            "restored_tables": restored_tables,
            "skipped_tables": skipped_tables,
            "restored_files": restored_files,
            "restored_redis": restored_redis,
            "restored_qdrant": restored_qdrant,
            "restored_clickhouse": restored_clickhouse,
            "restore_config": restore_mode,
            "timestamp": datetime.utcnow().isoformat(),
        }

    # 非 zip 旧格式（backup 为 break 时当前项）
    backup_path = backup.location
    with open(backup_path, "r", encoding="utf-8") as f:
        dump_data = json.load(f)
    engine = get_engine()
    if restore_mode in ("both", "db_only"):
        async with engine.begin() as conn:
            if "tables" in dump_data and dump_data["tables"]:
                r, s = await _restore_tables_from_payload(engine, conn, dump_data["tables"])
                restored_tables.extend(r)
                skipped_tables.extend(s)
            if "config_tables" in dump_data and dump_data["config_tables"]:
                r, s = await _restore_tables_from_payload(engine, conn, dump_data["config_tables"])
                restored_tables.extend(r)
                skipped_tables.extend(s)
    if restore_mode in ("both", "file_only") and "config_files" in dump_data and dump_data["config_files"]:
        base = _config_files_base_path()
        for rel, content in dump_data["config_files"].items():
            if isinstance(content, dict):
                continue
            path = os.path.join(base, rel)
            try:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w", encoding="utf-8") as f:
                    f.write(content)
                restored_files.append(rel)
            except Exception as exc:
                logger.warning("restore file %s failed: %s", rel, exc)

    logger.info(
        "restore_backup %s: restored_tables=%s, skipped_tables=%s, restored_files=%s",
        backup_id, restored_tables, skipped_tables, restored_files,
    )
    return {
        "backup_id": backup_id,
        "restored_tables": restored_tables,
        "skipped_tables": skipped_tables,
        "restored_files": restored_files,
        "restore_config": restore_mode,
        "timestamp": datetime.utcnow().isoformat(),
    }
