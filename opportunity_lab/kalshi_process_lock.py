"""One-worker file lock on Windows and Linux."""
def acquire(path):
    handle=path.open('a+b');handle.seek(0)
    try:
        if not handle.read(1):handle.write(b'0');handle.flush()
    except OSError:
        handle.close();raise RuntimeError('worker_already_running') from None
    handle.seek(0)
    try:
        import msvcrt
    except ImportError:
        import fcntl
        try:fcntl.flock(handle.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError:handle.close();raise RuntimeError('worker_already_running') from None
    else:
        try:msvcrt.locking(handle.fileno(),msvcrt.LK_NBLCK,1)
        except OSError:handle.close();raise RuntimeError('worker_already_running') from None
    return handle
