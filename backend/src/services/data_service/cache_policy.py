from dataclasses import dataclass


@dataclass
class CachePolicy:
    ttl_seconds: int
    max_items: int


DEFAULT_POLICY = CachePolicy(ttl_seconds=60, max_items=1000)
