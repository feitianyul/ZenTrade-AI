from typing import List

from src.schemas.validators import (
    validate_password,
    validate_phone,
    validate_price,
    validate_required,
    validate_symbol,
    validate_volume,
)


def validate_login_payload(phone: str, password: str) -> List[str]:
    errors: List[str] = []
    try:
        validate_phone(phone)
    except ValueError as exc:
        errors.append(str(exc))
    try:
        validate_password(password)
    except ValueError as exc:
        errors.append(str(exc))
    return errors


def validate_order_payload(symbol: str, price: float, volume: int) -> List[str]:
    errors: List[str] = []
    try:
        validate_symbol(symbol)
    except ValueError as exc:
        errors.append(str(exc))
    try:
        validate_price(price)
    except ValueError as exc:
        errors.append(str(exc))
    try:
        validate_volume(volume)
    except ValueError as exc:
        errors.append(str(exc))
    return errors


def validate_text_payload(text: str, label: str) -> List[str]:
    errors: List[str] = []
    try:
        validate_required(text, label)
    except ValueError as exc:
        errors.append(str(exc))
    return errors
