"""Single-writer lock for operational mutations of one Northstar operational database.

Paper decisions must be processed one session cutoff at a time, in order. Two
writers against one database can interleave cutoffs -- one freezing and judging
D+1 before the other has created D's order -- and the deterministic identities
that make each write idempotent cannot repair that interleaving afterwards. So
every command that mutates operational state holds this lock for its whole run.

The lock is keyed to the writable database, the deployment's safety boundary:

    <resolved database path>.operations.lock

Two different databases never block each other; every command naming the same
database shares one lock, whatever spelling of the path it was given.

Ownership is an OS lock, not a file
-----------------------------------
The lock file is a rendezvous point only. It is never deleted and holds no
content -- no process id, no credential -- so a file left behind by a crashed
process means nothing: the OS releases a process's lock when it exits or dies,
and the next writer acquires it. Linux uses ``flock`` (per open file
description), Windows ``msvcrt.locking`` (per handle). Both refuse a second
acquisition even from the same process, so the lock is deliberately not
re-entrant: a caller that needs several steps under one lock holds it once
around all of them.

Acquisition never waits. If another writer holds the lock, the caller gets
OperationsAlreadyActiveError and must not mutate anything.
"""

from __future__ import annotations

import errno
import os
from pathlib import Path
from types import TracebackType

from northstar_api.runtime import DatabaseConfigurationError

LOCK_SUFFIX = ".operations.lock"


class OperationsAlreadyActiveError(RuntimeError):
    """Raised when another writer already holds a database's operations lock."""

    def __init__(self, database: Path) -> None:
        super().__init__(
            f"Another Northstar operations writer is active for database {database}; "
            "nothing was changed."
        )
        self.database = database


def operations_lock_path(database: Path) -> Path:
    """Return the lock file for ``database``: its resolved path plus the lock suffix."""
    resolved = Path(database).resolve()
    return resolved.with_name(resolved.name + LOCK_SUFFIX)


class _PosixBackend:
    """``flock`` on the open file description: released when the process exits or dies."""

    def __init__(self, fcntl_module) -> None:
        self._fcntl = fcntl_module

    def try_lock(self, descriptor: int) -> bool:
        try:
            self._fcntl.flock(descriptor, self._fcntl.LOCK_EX | self._fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        return True

    def unlock(self, descriptor: int) -> None:
        self._fcntl.flock(descriptor, self._fcntl.LOCK_UN)


class _WindowsBackend:
    """A one-byte ``msvcrt`` region lock: released when its handle closes or the process ends."""

    _HELD = {errno.EACCES, errno.EDEADLK}

    def __init__(self, msvcrt_module) -> None:
        self._msvcrt = msvcrt_module

    def try_lock(self, descriptor: int) -> bool:
        os.lseek(descriptor, 0, os.SEEK_SET)
        try:
            self._msvcrt.locking(descriptor, self._msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            if exc.errno in self._HELD:
                return False
            raise
        return True

    def unlock(self, descriptor: int) -> None:
        os.lseek(descriptor, 0, os.SEEK_SET)
        self._msvcrt.locking(descriptor, self._msvcrt.LK_UNLCK, 1)


def _platform_backend():
    if os.name == "nt":
        import msvcrt

        return _WindowsBackend(msvcrt)
    import fcntl

    return _PosixBackend(fcntl)


class DatabaseOperationsLock:
    """Hold one database's operations lock for the duration of a ``with`` block.

    Entering acquires without waiting and raises OperationsAlreadyActiveError
    when another writer holds it; leaving releases it, also when the block
    raises. One instance can be held once at a time.
    """

    def __init__(self, database: Path, *, backend=None) -> None:
        self._database = Path(database)
        self._path = operations_lock_path(self._database)
        self._backend = backend if backend is not None else _platform_backend()
        self._descriptor: int | None = None

    @property
    def path(self) -> Path:
        """Return the lock file this instance acquires."""
        return self._path

    def __repr__(self) -> str:
        return f"DatabaseOperationsLock(path={str(self._path)!r})"

    def __enter__(self) -> DatabaseOperationsLock:
        if self._descriptor is not None:
            raise RuntimeError("DatabaseOperationsLock is not re-entrant; it is already held.")
        if not self._path.parent.is_dir():
            # The same message the runtime gives, before anything is created.
            raise DatabaseConfigurationError(
                f"Database directory does not exist: {self._database.parent}"
            )
        descriptor = os.open(self._path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            acquired = self._backend.try_lock(descriptor)
        except BaseException:
            os.close(descriptor)
            raise
        if not acquired:
            os.close(descriptor)
            raise OperationsAlreadyActiveError(self._database)
        self._descriptor = descriptor
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        descriptor, self._descriptor = self._descriptor, None
        if descriptor is None:
            return
        try:
            self._backend.unlock(descriptor)
        finally:
            os.close(descriptor)
