import re
from typing import Any

_PHONE_PATTERN = re.compile(r"^[0-9]{6,32}$")
_SYMBOL_PATTERN = re.compile(r"^[0-9A-Z]{4,12}(\.[A-Z]{2,4})?$")


def validate_phone(phone: str) -> None:
    if not _PHONE_PATTERN.match(phone):
        raise ValueError("invalid phone")


def validate_password(password: str) -> None:
    if len(password) < 6 or len(password) > 64:
        raise ValueError("invalid password length")
    if not re.search(r"[A-Za-z]", password) or not re.search(r"[0-9]", password):
        raise ValueError("password too weak")


def validate_symbol(symbol: str) -> None:
    if not _SYMBOL_PATTERN.match(symbol):
        raise ValueError("invalid symbol")


def validate_volume(volume: int) -> None:
    if volume <= 0:
        raise ValueError("invalid volume")


def validate_price(price: float) -> None:
    if price <= 0:
        raise ValueError("invalid price")


def validate_required(value: Any, label: str) -> None:
    if value is None or (isinstance(value, str) and not value.strip()):
        raise ValueError(f"{label} required")
