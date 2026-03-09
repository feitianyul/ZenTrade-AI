"""网关治理服务 — 熔断器与限流

TODO: 接入 Redis 实现真实限流。
      当前 check_rate_limit 始终返回 True。
      接入步骤:
        1. 配置 Redis 连接
        2. 在 check_rate_limit 中使用 Redis 滑动窗口计数
        3. 配置每个 client_id 的限流阈值
"""

import time
from typing import Dict


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 30):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failures = 0
        self.last_failure_time = 0
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

    def record_failure(self):
        self.failures += 1
        self.last_failure_time = time.time()
        if self.failures >= self.failure_threshold:
            self.state = "OPEN"

    def record_success(self):
        self.failures = 0
        self.state = "CLOSED"

    def allow_request(self) -> bool:
        if self.state == "CLOSED":
            return True
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "HALF_OPEN"
                return True
            return False
        if self.state == "HALF_OPEN":
            return True # Allow one trial request
        return True

class GovernanceService:
    def __init__(self):
        self.breakers: Dict[str, CircuitBreaker] = {}
        self.rate_limits: Dict[str, int] = {} # Requests per minute

    def get_breaker(self, service_name: str) -> CircuitBreaker:
        if service_name not in self.breakers:
            self.breakers[service_name] = CircuitBreaker()
        return self.breakers[service_name]

    def check_rate_limit(self, client_id: str, limit: int = 60) -> bool:
        # TODO: 接入 Redis 实现真实滑动窗口限流
        return True

governance_service = GovernanceService()
