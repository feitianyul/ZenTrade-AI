from typing import Any, Optional

from fastapi import HTTPException


class AppError(Exception):
    def __init__(
        self,
        code: int,
        message: str,
        status_code: int = 400,
        detail: Optional[Any] = None,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.detail = detail

    def as_http_exception(self) -> HTTPException:
        return HTTPException(
            status_code=self.status_code,
            detail={"code": self.code, "message": self.message, "detail": self.detail},
        )

class NotFoundError(AppError):
    def __init__(self, message: str = "not found", detail: Optional[Any] = None):
        super().__init__(code=4040, message=message, status_code=404, detail=detail)

class ForbiddenError(AppError):
    def __init__(self, message: str = "forbidden", detail: Optional[Any] = None):
        super().__init__(code=4030, message=message, status_code=403, detail=detail)

class ValidationError(AppError):
    def __init__(self, message: str = "invalid request", detail: Optional[Any] = None):
        super().__init__(code=4000, message=message, status_code=400, detail=detail)
