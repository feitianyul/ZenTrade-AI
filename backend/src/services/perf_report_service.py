from typing import Any, Dict

from src.services.ops_service import get_system_status


async def generate_performance_report() -> Dict[str, Any]:
    """
    Generate a performance report based on current system status and metrics.
    """
    status = await get_system_status()
    resources = status.get("resources", {})
    
    report = {
        "status": "healthy",
        "metrics": resources,
        "recommendations": []
    }
    
    # Simple logic
    if resources.get("cpu_percent", 0) > 80:
        report["status"] = "degraded"
        report["recommendations"].append("High CPU usage detected. Consider scaling up.")
        
    if resources.get("memory_percent", 0) > 80:
        report["status"] = "degraded"
        report["recommendations"].append("High Memory usage detected.")
        
    return report
