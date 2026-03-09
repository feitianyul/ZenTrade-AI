import asyncio
from typing import Any, Dict, List

# Mock lock manager
_backup_locks: Dict[str, float] = {}

async def acquire_backup_lock(resource_id: str, timeout: int = 10) -> bool:
    if resource_id in _backup_locks:
        return False
    _backup_locks[resource_id] = asyncio.get_event_loop().time() + timeout
    return True

async def release_backup_lock(resource_id: str):
    if resource_id in _backup_locks:
        del _backup_locks[resource_id]

async def check_restore_conflict(
    backup_meta: Dict[str, Any],
    current_system_version: str,
) -> List[str]:
    conflicts = []
    # Check version compatibility
    if backup_meta.get("version") != current_system_version:
        conflicts.append(
            "Version mismatch: "
            f"Backup {backup_meta.get('version')} != System {current_system_version}"
        )
    
    # Check if a backup is currently running
    if "global_backup" in _backup_locks:
        conflicts.append("Backup in progress")
        
    return conflicts

async def resolve_backup_conflict(
    backup_id: str,
    force: bool = False,
) -> Dict[str, Any]:
    # Logic to handle conflict resolution, e.g., wait or kill other process
    if force:
        # Force release lock
        await release_backup_lock("global_backup")
        return {"status": "forced_resolution", "backup_id": backup_id}
    return {"status": "conflict_pending", "backup_id": backup_id}
