from typing import Iterable


def assess_risk(scores: Iterable[int]) -> str:
    total = sum(scores)
    if total >= 80:
        return "aggressive"
    if total >= 50:
        return "balanced"
    return "conservative"
