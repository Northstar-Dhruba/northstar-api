"""A durable marker that a database's futures for one exchange are reviewed-candle protected.

Finality is configured per process, in its environment. Without something in
the database itself, a second process that simply lacked that environment
would run as ``disabled`` and could store unreviewed candles into, or paper
trade on, the same file. This marker closes that gap: it lives in the SQLite
file, so every process that opens the file through Northstar sees it,
whatever its own configuration says.

When a database becomes protected
---------------------------------
The first write-capable command for an exchange's futures that runs in an
operated mode -- ``operator-approved`` or ``operator-reviewed`` -- marks the
file before it writes anything else. Nothing marks it in ``disabled`` mode, so
disposable development databases operated by hand stay unprotected unless an
operated mode is used on them.

What protection means
---------------------
A protected database refuses every write-capable command for that exchange's
futures unless the process runs in an operated mode: a missing, blank or
``disabled`` mode is a configuration error, never a quiet downgrade. Which
operated mode may do what is decided by the caller; this module only records
and answers whether the file is protected.

The marker is never removed by Northstar. Restoring a backup taken before the
marker existed restores an unprotected file, and a build that predates this
module does not read it; both are operator procedures, not code paths.

Storage
-------
One table, created only when a database first becomes protected, so an
unprotected database gains no table and older builds simply ignore it::

    northstar_reviewed_candle_protection(exchange_code TEXT PRIMARY KEY,
                                         protected_by_mode TEXT NOT NULL)

Rows are only ever inserted. No clock is read.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

from northstar_core.foundation.value_objects import ExchangeCode

from northstar_api.runtime import DatabaseConfigurationError

_TABLE = "northstar_reviewed_candle_protection"
_CREATE = (
    f"CREATE TABLE IF NOT EXISTS {_TABLE} ("
    "exchange_code TEXT PRIMARY KEY NOT NULL, "
    "protected_by_mode TEXT NOT NULL)"
)


def is_protected(database: Path, exchange: ExchangeCode) -> bool:
    """Return whether the file is protected for this exchange's futures.

    A file that does not exist yet is not protected. Any failure to read an
    existing file is a configuration error, never "unprotected".
    """
    if not isinstance(exchange, ExchangeCode):
        raise TypeError("Reviewed-candle protection exchange must be an ExchangeCode.")
    if not database.exists():
        return False
    try:
        # The file exists, so connecting cannot create one; only SELECTs follow.
        with closing(sqlite3.connect(database)) as db:
            table = db.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (_TABLE,)
            ).fetchone()
            if table is None:
                return False
            row = db.execute(
                f"SELECT 1 FROM {_TABLE} WHERE exchange_code = ?",  # noqa: S608 - fixed name
                (exchange.value,),
            ).fetchone()
    except sqlite3.Error as exc:
        raise DatabaseConfigurationError(
            f"Reviewed-candle protection cannot be read from {database}."
        ) from exc
    return row is not None


def protect(database: Path, exchange: ExchangeCode, mode: str) -> None:
    """Mark the file protected for this exchange's futures; a no-op when already marked."""
    if not isinstance(exchange, ExchangeCode):
        raise TypeError("Reviewed-candle protection exchange must be an ExchangeCode.")
    if not isinstance(mode, str) or not mode:
        raise ValueError("Reviewed-candle protection requires the operated mode's name.")
    try:
        with closing(sqlite3.connect(database)) as db, db:
            db.execute(_CREATE)
            db.execute(
                f"INSERT OR IGNORE INTO {_TABLE} (exchange_code, protected_by_mode) "  # noqa: S608
                "VALUES (?, ?)",
                (exchange.value, mode),
            )
    except sqlite3.Error as exc:
        raise DatabaseConfigurationError(
            f"Reviewed-candle protection cannot be recorded in {database}."
        ) from exc
