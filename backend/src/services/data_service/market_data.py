from datetime import datetime, timedelta
from typing import Any


def generate_equity_curve(start_date: str, end_date: str, seed: int) -> list[dict[str, Any]]:
    start = datetime.fromisoformat(start_date).date()
    end = datetime.fromisoformat(end_date).date()
    if end < start:
        raise ValueError("invalid date range")
    days = (end - start).days
    total_days = max(days, 1)
    value = 10000.0
    benchmark = 10000.0
    curve: list[dict[str, Any]] = []
    for offset in range(total_days + 1):
        current = start + timedelta(days=offset)
        step = ((seed + offset * 13) % 17 - 8) / 1000
        bench_step = ((seed + offset * 7) % 15 - 7) / 1200
        value = value * (1 + step)
        benchmark = benchmark * (1 + bench_step)
        curve.append(
            {
                "date": current.isoformat(),
                "value": round(value, 2),
                "benchmark": round(benchmark, 2),
            }
        )
    return curve
