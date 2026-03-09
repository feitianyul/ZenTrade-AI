import base64
import os
from typing import Optional, cast

from gmssl.sm4 import SM4_DECRYPT, SM4_ENCRYPT, CryptSM4

_DEFAULT_KEY = os.getenv("SM4_KEY", "0123456789abcdef")

def _pad(data: bytes) -> bytes:
    pad_len = 16 - (len(data) % 16)
    return data + bytes([pad_len] * pad_len)

def _unpad(data: bytes) -> bytes:
    pad_len = data[-1]
    if pad_len < 1 or pad_len > 16:
        return data
    return data[:-pad_len]

def _key_bytes(key: Optional[str] = None) -> bytes:
    raw = (key or _DEFAULT_KEY).encode("utf-8")
    return raw[:16].ljust(16, b"0")

def encrypt_bytes(data: bytes, key: Optional[str] = None) -> bytes:
    crypt = CryptSM4()
    crypt.set_key(_key_bytes(key), SM4_ENCRYPT)
    return cast(bytes, crypt.crypt_ecb(_pad(data)))

def decrypt_bytes(data: bytes, key: Optional[str] = None) -> bytes:
    crypt = CryptSM4()
    crypt.set_key(_key_bytes(key), SM4_DECRYPT)
    return _unpad(cast(bytes, crypt.crypt_ecb(data)))

def encrypt_text(text: str, key: Optional[str] = None) -> str:
    encrypted = encrypt_bytes(text.encode("utf-8"), key)
    return base64.b64encode(encrypted).decode("utf-8")

def decrypt_text(text: str, key: Optional[str] = None) -> str:
    raw = base64.b64decode(text.encode("utf-8"))
    return decrypt_bytes(raw, key).decode("utf-8")
