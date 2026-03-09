import os
from typing import Iterable

from src.core.errors import ValidationError

IMPORT_MAX_BYTES = 10 * 1024 * 1024
EXPORT_MAX_BYTES = 50 * 1024 * 1024


def get_extension(filename: str) -> str:
    return os.path.splitext(filename)[1].lower()


def ensure_import_limits(
    filename: str,
    size_bytes: int,
    allowed_extensions: Iterable[str],
) -> None:
    if size_bytes > IMPORT_MAX_BYTES:
        raise ValidationError(
            "import file too large",
            detail={"max_bytes": IMPORT_MAX_BYTES, "size_bytes": size_bytes},
        )
    ext = get_extension(filename)
    normalized = {ext_name.lower() for ext_name in allowed_extensions}
    if ext not in normalized:
        raise ValidationError(
            "unsupported file type",
            detail={"extension": ext, "allowed": sorted(normalized)},
        )


def ensure_export_limits(size_bytes: int) -> None:
    if size_bytes > EXPORT_MAX_BYTES:
        raise ValidationError(
            "export file too large",
            detail={"max_bytes": EXPORT_MAX_BYTES, "size_bytes": size_bytes},
        )
