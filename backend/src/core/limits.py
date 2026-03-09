import os
import time
from dataclasses import dataclass
from typing import Dict


@dataclass
class RateLimitState:
    count: int
    reset_at: float


class RateLimiter:
    def __init__(self, max_calls: int, window_seconds: int) -> None:
        self.max_calls = max_calls
        self.window_seconds = window_seconds
        self._states: Dict[str, RateLimitState] = {}

    def allow(self, key: str) -> bool:
        now = time.time()
        state = self._states.get(key)
        if not state or now >= state.reset_at:
            self._states[key] = RateLimitState(count=1, reset_at=now + self.window_seconds)
            return True
        if state.count >= self.max_calls:
            return False
        state.count += 1
        return True


class CircuitBreaker:
    def __init__(self, failure_threshold: int, recovery_seconds: int) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_seconds = recovery_seconds
        self._failures: Dict[str, int] = {}
        self._opened_at: Dict[str, float] = {}

    def allow(self, key: str) -> bool:
        opened_at = self._opened_at.get(key)
        if opened_at is None:
            return True
        if time.time() - opened_at >= self.recovery_seconds:
            self._opened_at.pop(key, None)
            self._failures[key] = 0
            return True
        return False

    def record_success(self, key: str) -> None:
        self._failures[key] = 0
        self._opened_at.pop(key, None)

    def record_failure(self, key: str) -> None:
        failures = self._failures.get(key, 0) + 1
        self._failures[key] = failures
        if failures >= self.failure_threshold:
            self._opened_at[key] = time.time()


def _get_int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


_rate_limiter = RateLimiter(
    max_calls=_get_int_env("RATE_LIMIT_MAX_CALLS", 100),
    window_seconds=_get_int_env("RATE_LIMIT_WINDOW_SECONDS", 60),
)
_circuit_breaker = CircuitBreaker(
    failure_threshold=_get_int_env("CIRCUIT_FAILURE_THRESHOLD", 5),
    recovery_seconds=_get_int_env("CIRCUIT_RECOVERY_SECONDS", 30),
)


def get_rate_limiter() -> RateLimiter:
    return _rate_limiter


def get_circuit_breaker() -> CircuitBreaker:
    return _circuit_breaker
