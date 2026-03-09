#!/usr/bin/env python3
"""
云端模型接入测试脚本：从 MySQL ai_configs 读取 llm_keys，逐条测试 chat/completions 连通性，
并与现有 test_connection 逻辑、LLMRouter.chat 行为对比，输出差距说明。

用法（在云端 /opt/trading/backend）：
  PYTHONPATH=. .venv/bin/python scripts/test_llm_connection_cloud.py
  PYTHONPATH=. .venv/bin/python scripts/test_llm_connection_cloud.py --tenant default
  PYTHONPATH=. .venv/bin/python scripts/test_llm_connection_cloud.py --key-index 0 --verbose
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path

# 加载 backend .env
_backend_dir = Path(__file__).resolve().parent.parent
_env_file = Path(os.getenv("ENV_FILE", _backend_dir / ".env"))
if _env_file.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_file, override=True)
    except Exception:
        pass


async def get_llm_keys_from_db(tenant_id: str | None = None) -> tuple[str, dict] | None:
    """从 MySQL ai_configs 读取 llm_keys。返回 (tenant_id, llm_keys_value) 或 None。"""
    from sqlalchemy import select
    from src.core.db import get_session
    from src.models.ai_config import AIConfig

    async for session in get_session():
        if tenant_id:
            stmt = (
                select(AIConfig)
                .where(
                    AIConfig.tenant_id == tenant_id,
                    AIConfig.key == "llm_keys",
                    AIConfig.is_active.is_(True),
                )
                .order_by(AIConfig.version.desc())
                .limit(1)
            )
        else:
            stmt = (
                select(AIConfig)
                .where(AIConfig.key == "llm_keys", AIConfig.is_active.is_(True))
                .order_by(AIConfig.tenant_id, AIConfig.version.desc())
            )
        result = await session.execute(stmt)
        row = result.scalars().first()
        if row:
            return (row.tenant_id, row.value or {})
        if tenant_id:
            return None
        # 无 tenant 时再查任意 tenant 的 llm_keys
        stmt2 = select(AIConfig).where(AIConfig.key == "llm_keys", AIConfig.is_active.is_(True)).limit(1)
        result2 = await session.execute(stmt2)
        row2 = result2.scalars().first()
        if row2:
            return (row2.tenant_id, row2.value or {})
        return None


async def test_one_key(
    endpoint: str,
    api_key: str,
    model: str,
    label: str = "",
    verbose: bool = False,
) -> dict:
    """单条 Key 测试：POST endpoint/chat/completions，与现有 test_connection 逻辑一致。"""
    import httpx

    url = endpoint.rstrip("/") + "/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    body = {"model": model or "gpt-4o-mini", "messages": [{"role": "user", "content": "hi"}], "max_tokens": 5}

    out = {"label": label, "url": url, "model": model or "gpt-4o-mini", "success": False, "status": None, "latency_ms": 0, "error": "", "body_preview": ""}
    t0 = time.time()
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(url, json=body, headers=headers)
        out["latency_ms"] = int((time.time() - t0) * 1000)
        out["status"] = resp.status_code
        out["body_preview"] = (resp.text or "")[:400]
        if resp.status_code == 200:
            out["success"] = True
            try:
                data = resp.json()
                out["returned_model"] = data.get("model", "")
            except Exception:
                pass
        else:
            out["error"] = f"HTTP {resp.status_code}: {resp.text[:200]}"
    except Exception as e:
        out["latency_ms"] = int((time.time() - t0) * 1000)
        out["error"] = str(e)[:300]
    return out


async def test_llm_router_chat(keys_value: dict, verbose: bool = False) -> dict:
    """用 LLMRouter.chat 发一条请求，对比与单 Key 测试的差距。"""
    from src.services.llm_service.llm_router import LLMRouter

    router = LLMRouter(keys_value, {})
    t0 = time.time()
    result = await router.chat([{"role": "user", "content": "回复OK"}], max_tokens=5)
    elapsed = int((time.time() - t0) * 1000)
    out = {"success": "error" not in result or result.get("error") is None, "latency_ms": elapsed, "result_keys": list(result.keys()), "error": result.get("error") or result.get("message", ""), "content_preview": (result.get("content") or "")[:200]}
    return out


def main_async(tenant_id: str | None, key_index: int | None, verbose: bool) -> None:
    async def _run():
        print("========== 云端模型接入测试 ==========")
        print(f"ENV_FILE / .env: {_env_file} (exists={_env_file.exists()})")
        print(f"MYSQL_DSN: {'(set)' if os.getenv('MYSQL_DSN') else '(not set)'}")
        print()

        cfg = await get_llm_keys_from_db(tenant_id)
        if not cfg:
            print("[FAIL] 未找到 llm_keys 配置（请指定 --tenant 或确认 ai_configs 中已有 llm_keys）")
            return
        tid, keys_value = cfg
        print(f"[INFO] 使用租户: {tid}")
        key_list = keys_value.get("keys") or []
        default_model = keys_value.get("default_model") or "gpt-4o-mini"
        print(f"[INFO] Key 数量: {len(key_list)}, 默认模型: {default_model}")
        if not key_list:
            print("[FAIL] llm_keys.keys 为空")
            return

        indexes = [key_index] if key_index is not None else range(len(key_list))
        for i in indexes:
            if i < 0 or i >= len(key_list):
                continue
            k = key_list[i]
            endpoint = k.get("endpoint", "")
            api_key = k.get("api_key", "")
            model = k.get("model") or default_model
            label = k.get("label") or f"Key-{i+1}"
            if not endpoint or not api_key:
                print(f"[SKIP] {label}: endpoint 或 api_key 为空")
                continue
            print(f"\n--- 测试 {label} (key_index={i}) ---")
            res = await test_one_key(endpoint, api_key, model, label=label, verbose=verbose)
            if res["success"]:
                print(f"  结果: 成功 | 延迟: {res['latency_ms']}ms | 模型: {res.get('returned_model', res['model'])}")
            else:
                print(f"  结果: 失败 | 状态: {res['status']} | 延迟: {res['latency_ms']}ms")
                print(f"  错误: {res['error']}")
            if verbose and res["body_preview"]:
                print(f"  响应摘要: {res['body_preview'][:200]}...")

        print("\n--- LLMRouter.chat 行为测试（与单 Key 对比）---")
        router_out = await test_llm_router_chat(keys_value, verbose=verbose)
        if router_out["success"]:
            print(f"  结果: 成功 | 延迟: {router_out['latency_ms']}ms | 内容: {router_out['content_preview']}")
        else:
            print(f"  结果: 失败 | 错误: {router_out['error']} | 延迟: {router_out['latency_ms']}ms")
            print(f"  result 键: {router_out['result_keys']}")

        print("\n========== 测试结束 ==========")

    asyncio.run(_run())


def main():
    parser = argparse.ArgumentParser(description="云端模型接入测试：读 DB llm_keys，测 chat/completions 与 LLMRouter")
    parser.add_argument("--tenant", type=str, default=None, help="租户 ID，不传则自动取有 llm_keys 的任一租户")
    parser.add_argument("--key-index", type=int, default=None, help="只测第几个 Key，不传则测全部")
    parser.add_argument("--verbose", action="store_true", help="打印响应摘要")
    args = parser.parse_args()
    main_async(tenant_id=args.tenant, key_index=args.key_index, verbose=args.verbose)


if __name__ == "__main__":
    main()
    sys.exit(0)
