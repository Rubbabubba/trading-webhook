"""Storage admission for optional research on the shared Demo disk."""
import shutil

from .kalshi_demo_stream_archive import MIN_FREE


class ShadowDiskReserveReached(RuntimeError):
    pass


def require_research_space(root):
    if shutil.disk_usage(root).free <= MIN_FREE:
        raise ShadowDiskReserveReached('Optional research paused at the Demo disk reserve')
