from typing import Literal


class EnvService:
    _current_env: Literal["sim", "real"] = "sim"
    
    @classmethod
    def get_current_env(cls) -> str:
        return cls._current_env
        
    @classmethod
    def switch_env(cls, env: Literal["sim", "real"]) -> str:
        # In a real app, this might check permissions or restart connections
        cls._current_env = env
        return cls._current_env

# Helper functions for functional usage
def get_current_env() -> str:
    return EnvService.get_current_env()

def switch_env(env: str) -> str:
    if env not in ["sim", "real"]:
        raise ValueError("Invalid environment")
    return EnvService.switch_env(env) # type: ignore
