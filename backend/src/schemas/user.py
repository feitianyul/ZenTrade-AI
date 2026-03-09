from pydantic import BaseModel


class UserOut(BaseModel):
    user_id: str
    tenant_id: str
    phone: str
    level: str
    risk_level: str

    # get_current_user returns UserOut; routers use .user_id / .id interchangeably
    @property
    def id(self) -> str:
        return self.user_id


# backward-compatible alias used by some routers
User = UserOut
