"""Offline OS-owned single-writer guard. No transport, orders or study rollover.

The stable lock inode is never unlinked or replaced. Kernel ownership ends on
process death; a timestamp or PID file is never permission to steal a live lock.
"""
import os
from pathlib import Path
import stat

from resource_guard import GuardedArchive

SCHEMA = 'raw_receipt_process_lock_component_v1_20261101'


class ProcessLease:
    def __init__(self, path):
        self.path = Path(path)
        self.fd = None
        self.owned = False
        if self.path.is_symlink():
            raise ValueError('lock_symlink_forbidden')
        flags = os.O_CREAT | os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0)
        self.fd = os.open(self.path, flags, 0o600)
        try:
            os.set_inheritable(self.fd, False)
            if not stat.S_ISREG(os.fstat(self.fd).st_mode):
                raise ValueError('regular_lock_file_required')
            os.lseek(self.fd, 0, os.SEEK_SET)
            try:
                if os.name == 'nt':
                    import msvcrt
                    msvcrt.locking(self.fd, msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                raise RuntimeError('archive_writer_already_owned_or_lock_unavailable') from None
            self.owned = True
            size = os.fstat(self.fd).st_size
            if size not in (0, 1):
                raise ValueError('unexpected_lock_file_size')
            if size == 0:
                os.write(self.fd, b'1')
                os.fsync(self.fd)
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.fd is None:
            return
        try:
            if self.owned:
                os.lseek(self.fd, 0, os.SEEK_SET)
                if os.name == 'nt':
                    import msvcrt
                    msvcrt.locking(self.fd, msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self.fd, fcntl.LOCK_UN)
        finally:
            os.close(self.fd)
            self.fd = None
            self.owned = False


class ExclusiveArchive:
    def __init__(self, path, *, started_at, **kwargs):
        self.path = Path(path)
        self.inner = None
        # Acquire BEFORE registration, disk checks, stop receipts or SQLite open.
        self.lease = ProcessLease(str(self.path)+'.writer-lock')
        try:
            self.inner = GuardedArchive(self.path, started_at=started_at, **kwargs)
        except BaseException:
            self.lease.close()
            raise

    def _owned(self):
        if self.inner is None or not self.lease.owned:
            raise RuntimeError('archive_writer_closed')

    def observe(self, receipt, *, at):
        self._owned()
        return self.inner.observe(receipt, at=at)

    def status(self):
        self._owned()
        return dict(self.inner.status(), writer_guard=SCHEMA, process_exclusive=True)

    def close(self):
        try:
            if self.inner is not None:
                self.inner.close()
        finally:
            self.inner = None
            self.lease.close()
