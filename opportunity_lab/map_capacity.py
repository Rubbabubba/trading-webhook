"""Small read-only capacity sample for this service's memory cgroup and data mount."""
from datetime import datetime, timezone
from pathlib import Path
from shutil import disk_usage


def sample():
    result = {'observed_at': datetime.now(timezone.utc).isoformat()}
    group = Path('/sys/fs/cgroup')
    try:
        used = int((group / 'memory.current').read_text())
        total = int((group / 'memory.max').read_text())
        if total > 0 and used >= 0:
            result['ram'] = {'used': min(used, total), 'total': total}
    except (OSError, ValueError):
        pass
    try:
        mount = Path('/var/data')
        if mount.is_mount():
            disk = disk_usage(mount)
            result['disk'] = {'used': disk.total - disk.free, 'total': disk.total}
    except OSError:
        pass
    return result
