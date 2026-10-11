"""A durable marker that a database's futures for one exchange are reviewed-candle protected.

Finality is configured per process, in its environment. Without something in
the database itself, a second process that simply lacked that environment
would run as ``disabled`` and could store unreviewed candles into, or paper
trade on, the same file. This marker closes that gap for every process that
opens the file through this build: it lives in the SQLite file, whatever a
process's own configuration says.

When a database becomes protected
---------------------------------
Either explicitly, by ``northstar protection init`` -- the controlled-release
step, run while nothing else can write -- or implicitly, by the first
write-capable command for that exchange's futures that runs in an operated mode
(``operator-approved`` or ``operator-reviewed``), before it writes anything
else. Nothing marks a file in ``disabled`` mode, so disposable development
databases operated by hand stay unprotected unless an operated mode or the
explicit command is used on them.

What protection means
---------------------
A protected database refuses every write-capable command for that exchange's
futures unless the process runs in an operated mode: a missing, blank or
``disabled`` mode is a configuration error, never a quiet downgrade. Which
operated mode may do what is decided by the caller; this module records,
validates and answers whether the file is protected.

What it cannot do
-----------------
A file without the marker cannot show whether it was never protected or lost
its marker, for example by restoring an older backup. A build that predates
this module never reads the marker. A process that writes the file without
Northstar is not stopped. Those are release and rollback procedures, not code
paths. The marker is never removed by Northstar.

Storage
-------
One table, created only when a database first becomes protected, so an
unprotected database gains no table and older builds simply ignore it::

    northstar_reviewed_candle_protection(exchange_code TEXT PRIMARY KEY NOT NULL,
                                         protected_by_mode TEXT NOT NULL)

Rows are only ever inserted, inside one ``BEGIN IMMEDIATE`` transaction that
also creates the table, so a failure leaves neither. No clock is read. A table
of any other shape, or a row naming an unknown mode, is invalid: reading it is
a configuration error, never "unprotected".
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from northstar_core.foundation.value_objects import ExchangeCode

from northstar_api.runtime import DatabaseConfigurationError

# The protection contract this build enforces; ``protection status`` prints it.
PROTECTION_VERSION = 1
INITIALIZED_MODE = "operator-initialized"
KNOWN_MODES = frozenset({"operator-approved", "operator-reviewed", INITIALIZED_MODE})

_TABLE = "northstar_reviewed_candle_protection"
_CREATE = (
    f"CREATE TABLE {_TABLE} ("
    "exchange_code TEXT PRIMARY KEY NOT NULL, "
    "protected_by_mode TEXT NOT NULL)"
)
# PRAGMA table_info: (name, type, notnull, pk) for the one valid shape.
_COLUMNS = (("exchange_code", "TEXT", 1, 1), ("protected_by_mode", "TEXT", 1, 0))


@dataclass(frozen=True, slots=True)
class ProtectionState:
    """What one file says about one exchange's reviewed-candle protection."""

    database_exists: bool
    futures_schema_present: bool
    marker_table_present: bool
    problems: tuple[str, ...]
    protected_by_mode: str | None

    @property
    def valid(self) -> bool:
        return not self.problems

    @property
    def protected(self) -> bool:
        return self.valid and self.protected_by_mode is not None


def _require_exchange(exchange: object) -> ExchangeCode:
    if not isinstance(exchange, ExchangeCode):
        raise TypeError("Reviewed-candle protection exchange must be an ExchangeCode.")
    return exchange


def _schema_problems(db: sqlite3.Connection) -> list[str]:
    columns = tuple(
        (row[1], str(row[2]).upper(), row[3], row[5])
        for row in db.execute(f"PRAGMA table_info({_TABLE})")
    )
    if columns != _COLUMNS:
        return [f"{_TABLE} does not have the expected columns"]
    return []


def _row_problems(rows: list[tuple[object, object]]) -> list[str]:
    problems: list[str] = []
    for code, mode in rows:
        try:
            ExchangeCode(code)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            problems.append(f"{_TABLE} holds an invalid exchange code")
        if mode not in KNOWN_MODES:
            problems.append(f"{_TABLE} holds an unknown protection mode")
    return problems


def _table_present(db: sqlite3.Connection) -> bool:
    return (
        db.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (_TABLE,)
        ).fetchone()
        is not None
    )


def inspect_protection(database: Path, exchange: ExchangeCode) -> ProtectionState:
    """Return the file's protection state, opening it read-only; never writes.

    A missing file is reported as such. A file SQLite cannot read raises
    DatabaseConfigurationError.
    """
    _require_exchange(exchange)
    if not database.is_file():
        return ProtectionState(False, False, False, (), None)
    uri = database.resolve().as_uri() + "?mode=ro"
    try:
        with closing(sqlite3.connect(uri, uri=True)) as db:
            futures = (
                db.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'futures_ohlcv'"
                ).fetchone()
                is not None
            )
            if not _table_present(db):
                return ProtectionState(True, futures, False, (), None)
            problems = _schema_problems(db)
            mode: str | None = None
            if not problems:
                rows = db.execute(
                    f"SELECT exchange_code, protected_by_mode FROM {_TABLE}"
                ).fetchall()
                problems = _row_problems(rows)
                mode = next((m for c, m in rows if c == exchange.value), None)
    except sqlite3.Error as exc:
        raise DatabaseConfigurationError(
            f"Reviewed-candle protection cannot be read from {database}."
        ) from exc
    return ProtectionState(True, futures, True, tuple(problems), mode)


def is_protected(database: Path, exchange: ExchangeCode) -> bool:
    """Return whether the file is protected; an invalid marker is refused, never ignored."""
    state = inspect_protection(database, exchange)
    if not state.valid:
        raise DatabaseConfigurationError(
            f"Reviewed-candle protection in {database} is invalid: "
            + "; ".join(state.problems)
            + ". Nothing was written."
        )
    return state.protected


def _insert(db: sqlite3.Connection, exchange: ExchangeCode, mode: str) -> None:
    db.execute(
        f"INSERT OR IGNORE INTO {_TABLE} (exchange_code, protected_by_mode) VALUES (?, ?)",  # noqa: S608
        (exchange.value, mode),
    )


def protect(database: Path, exchange: ExchangeCode, mode: str) -> ProtectionState:
    """Mark the file protected for this exchange; idempotent; verified afterwards.

    One ``BEGIN IMMEDIATE`` transaction creates the table if needed and inserts
    the row; any failure rolls both back. An existing marker is kept as it is,
    never overwritten. An invalid existing table or row is refused.
    """
    _require_exchange(exchange)
    if mode not in KNOWN_MODES:
        raise ValueError(f"Reviewed-candle protection mode must be one of {sorted(KNOWN_MODES)}.")
    try:
        with closing(sqlite3.connect(database, isolation_level=None)) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                if _table_present(db):
                    problems = _schema_problems(db) or _row_problems(
                        db.execute(
                            f"SELECT exchange_code, protected_by_mode FROM {_TABLE}"
                        ).fetchall()
                    )
                    if problems:
                        raise DatabaseConfigurationError(
                            f"Reviewed-candle protection in {database} is invalid: "
                            + "; ".join(problems)
                            + ". Nothing was written."
                        )
                else:
                    db.execute(_CREATE)
                _insert(db, exchange, mode)
                db.execute("COMMIT")
            except BaseException:
                db.execute("ROLLBACK")
                raise
    except sqlite3.Error as exc:
        raise DatabaseConfigurationError(
            f"Reviewed-candle protection cannot be recorded in {database}."
        ) from exc
    state = inspect_protection(database, exchange)
    if not state.protected:
        raise DatabaseConfigurationError(
            f"Reviewed-candle protection could not be verified in {database} after writing it."
        )
    return state
