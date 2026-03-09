import uuid
from contextvars import ContextVar
from typing import Optional

# Context variable to store trace_id
trace_id_ctx: ContextVar[Optional[str]] = ContextVar("trace_id", default=None)

def get_trace_id() -> str:
    tid = trace_id_ctx.get()
    if not tid:
        tid = str(uuid.uuid4())
        trace_id_ctx.set(tid)
    return tid

def set_trace_id(tid: str) -> None:
    trace_id_ctx.set(tid)

class TraceContext:
    def __init__(self, trace_id: Optional[str] = None):
        self.trace_id = trace_id or str(uuid.uuid4())
        self.token = None

    def __enter__(self):
        self.token = trace_id_ctx.set(self.trace_id)
        return self.trace_id

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.token:
            trace_id_ctx.reset(self.token)
