"""T251 - JSON Schema 生成与校验"""

from typing import Any, Optional, Type

from pydantic import BaseModel


def generate_json_schema(model: Type[BaseModel]) -> dict[str, Any]:
    """从 Pydantic 模型生成 JSON Schema"""
    return model.model_json_schema()


def validate_against_schema(
    data: dict[str, Any], schema: dict[str, Any]
) -> dict[str, Any]:
    """校验数据是否符合 JSON Schema"""
    errors = []

    required = schema.get("required", [])
    properties = schema.get("properties", {})

    for field in required:
        if field not in data:
            errors.append(f"缺少必填字段: {field}")

    for field, value in data.items():
        if field in properties:
            prop = properties[field]
            expected_type = prop.get("type")
            if expected_type == "string" and not isinstance(value, str):
                errors.append(f"字段 {field} 应为 string 类型")
            elif expected_type == "number" and not isinstance(value, (int, float)):
                errors.append(f"字段 {field} 应为 number 类型")
            elif expected_type == "integer" and not isinstance(value, int):
                errors.append(f"字段 {field} 应为 integer 类型")
            elif expected_type == "boolean" and not isinstance(value, bool):
                errors.append(f"字段 {field} 应为 boolean 类型")

            # 字符串长度校验
            if isinstance(value, str):
                max_len = prop.get("maxLength")
                min_len = prop.get("minLength")
                if max_len and len(value) > max_len:
                    errors.append(f"字段 {field} 超过最大长度 {max_len}")
                if min_len and len(value) < min_len:
                    errors.append(f"字段 {field} 未达最小长度 {min_len}")

    return {"valid": len(errors) == 0, "errors": errors}


def generate_all_schemas() -> dict[str, dict[str, Any]]:
    """生成所有核心 Schema"""
    from src.schemas.auth import LoginRequest, RegisterRequest
    from src.schemas.market import MarketQuote, MarketDepth
    from src.schemas.community import CommunityPostCreate, CommunityPostOut

    schemas = {}
    for model in [LoginRequest, RegisterRequest, MarketQuote, MarketDepth, CommunityPostCreate, CommunityPostOut]:
        schemas[model.__name__] = generate_json_schema(model)
    return schemas
