from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    phone: str = Field(..., min_length=6, max_length=64)
    password: str = Field(..., min_length=6, max_length=64)
    login_method: str = Field(default="phone", min_length=2, max_length=16)
    identifier: str | None = Field(default=None, min_length=2, max_length=128)
    device_id: str | None = Field(default=None, min_length=2, max_length=64)
    location: str | None = Field(default=None, min_length=2, max_length=64)
    mfa_code: str | None = Field(default=None, min_length=4, max_length=12)


class RegisterRequest(BaseModel):
    phone: str = Field(..., min_length=6, max_length=32)
    password: str = Field(..., min_length=6, max_length=64)


class TokenData(BaseModel):
    token: str


class ChangePasswordRequest(BaseModel):
    current_password: str = Field(..., min_length=6, max_length=64)
    new_password: str = Field(..., min_length=6, max_length=64)
