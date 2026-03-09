import random
from typing import Any, Dict


class LLMRuntime:
    def __init__(self, model_id: str, endpoint: str, weight: int = 100):
        self.model_id = model_id
        self.endpoint = endpoint
        self.weight = weight

class RuntimeManager:
    def __init__(self):
        self.runtimes: Dict[str, LLMRuntime] = {}
        
    def register_runtime(self, runtime: LLMRuntime):
        self.runtimes[runtime.model_id] = runtime
        
    def get_runtime(self, model_id: str) -> LLMRuntime:
        return self.runtimes.get(model_id)
        
    def select_runtime_weighted(self) -> LLMRuntime:
        if not self.runtimes:
            return None
        # Simple weighted choice
        total_weight = sum(r.weight for r in self.runtimes.values())
        r = random.uniform(0, total_weight)
        uptime = 0
        for rt in self.runtimes.values():
            if uptime + rt.weight >= r:
                return rt
            uptime += rt.weight
        return list(self.runtimes.values())[0]

async def select_model(version: str) -> dict[str, Any]:
    # Backward compatibility
    return {"version": version, "endpoint": "http://panda_llm:8002"}

manager = RuntimeManager()
# Default setup
manager.register_runtime(LLMRuntime("panda-v1", "http://panda_llm:8002", 80))
manager.register_runtime(LLMRuntime("deepseek-v2", "http://deepseek:8000", 20))
