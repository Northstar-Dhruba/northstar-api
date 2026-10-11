"""Acceptance: FG-9.2E operator commands -- candle identity and database protection.

Everything is synthetic: evidence is written by the production record writer
from made-up candles, databases are disposable files built by the INDIA-7
harness, and the network is refused for the whole module. The known-answer
fingerprint was computed outside Python (``sha256sum`` and PowerShell
``Get-FileHash``) over the documented fdc1 bytes of the same synthetic candle.
"""

from __future__ import annotations

import hashlib
import io
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
from contextlib import closing
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import parse_futures_candle_approval_manifest
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import ExchangeCode, Symbol
from northstar_core.futures import FuturesContract, FuturesProductReference
from northstar_infrastructure.market_data import (
    UpstoxCandleEvidenceLog,
    UpstoxDailyCandleEvidence,
    UpstoxDailyCandleEvidenceObservation,
    upstox_http,
)
from test_india7_nifty_incremental_operations_acceptance import Operator, Outcome
from test_india8b_nse_chronological_operations import _daily, _operator

from northstar_api import reviewed_candle_protection as protection_module
from northstar_api.cli import ExitCode, main
from northstar_api.operations_lock import DatabaseOperationsLock
from northstar_api.settings import APPROVED_CANDLES_VARIABLE, load_session_operation_settings

_CONTRACT = FuturesContract(
    FuturesProductReference(Symbol("NIFTY"), ExchangeCode("NSE")), ExpirationDate("2026-10-27")
)
_SESSION = date(2026, 10, 1)
# FG-9.2A known answer 1: the fdc1 identity of this exact synthetic candle.
_KAT1 = "fdc1-sha256:a165a7760c27af793ac47c39af0db1c34fcef7ef0bcf6413669855eecd94e919"
_HISTORICAL = datetime(2026, 10, 5, 4, 0, tzinfo=UTC)  # 09:30 IST four days later
_SAME_DAY = datetime(2026, 10, 1, 10, 30, tzinfo=UTC)  # 16:00 IST on the session
_TABLES = (
    "futures_ohlcv",
    "futures_forward_research_records",
    "futures_paper_orders",
    "futures_paper_fills",
)
_MARKER = "northstar_reviewed_candle_protection"


@pytest.fixture(scope="module", autouse=True)
def no_network():
    import databento

    def refuse(*args, **kwargs):
        raise AssertionError("FG-9.2E acceptance must not touch the network")

    patcher = pytest.MonkeyPatch()
    patcher.setattr(socket.socket, "connect", refuse)
    patcher.setattr(socket, "create_connection", refuse)
    patcher.setattr(upstox_http, "urlopen", refuse)
    patcher.setattr(databento, "Historical", refuse)
    yield
    patcher.undo()


class TrackingEnv(dict):
    def __init__(self, *args) -> None:
        super().__init__(*args)
        self.read: list[str] = []

    def get(self, key, default=None):
        self.read.append(key)
        return super().get(key, default)

    def __getitem__(self, key):
        self.read.append(key)
        return super().__getitem__(key)


def _cli(argv: list[str], env: dict | None = None) -> Outcome:
    out, err = io.StringIO(), io.StringIO()
    code = main(argv, env={} if env is None else env, stdout=out, stderr=err)
    return Outcome(code, out.getvalue(), err.getvalue())


# ---------------------------------------------------------------------------
# Synthetic evidence, written by the production record writer
# ---------------------------------------------------------------------------


def _observation(
    *,
    requested_at: datetime = _HISTORICAL,
    trading_date: date = _SESSION,
    candle: bool = True,
    raw_volume: str = "2730",
    close: str = "100.00",
) -> UpstoxDailyCandleEvidenceObservation:
    lot = 65
    whole = int(Decimal(raw_volume)) // lot if int(Decimal(raw_volume)) % lot == 0 else None
    return UpstoxDailyCandleEvidenceObservation(
        requested_at=requested_at,
        received_at=requested_at,
        contract=_CONTRACT,
        trading_date=trading_date,
        instrument_key="NSE_FO|SYNTHETIC",
        lot_size=lot,
        candle=(
            UpstoxDailyCandleEvidence(
                provider_timestamp=f"{trading_date.isoformat()}T00:00:00+05:30",
                open=Decimal("101.250"),
                high=Decimal("102.50"),
                low=Decimal("99.75"),
                close=Decimal(close),
                volume=Decimal(raw_volume),
                open_interest=Decimal("777"),
            )
            if candle
            else None
        ),
        volume_contracts=whole if candle else None,
        volume_contracts_note=None if (not candle or whole is not None) else "not whole",
    )


def _evidence(tmp_path: Path, *observations: UpstoxDailyCandleEvidenceObservation) -> Path:
    path = tmp_path / "evidence.jsonl"
    log = UpstoxCandleEvidenceLog(path)
    for observation in observations:
        log.append(observation)
    return path


def _identity(evidence: Path, *extra: str, trading_date: str = "2026-10-01", env=None):
    return _cli(
        ["finality-evidence", "identity", "--evidence", str(evidence), "--product", "NIFTY",
         "--exchange", "NSE", "--expiration", "2026-10-27", "--trading-date", trading_date,
         *extra],
        env,
    )  # fmt: skip


# ---------------------------------------------------------------------------
# Candle identity command
# ---------------------------------------------------------------------------


def test_identity_prints_the_independently_known_fingerprint(tmp_path: Path) -> None:
    evidence = _evidence(tmp_path, _observation())

    run = _identity(evidence)

    assert run.code == ExitCode.SUCCESS, run.err
    assert run.out.splitlines()[:7] == [
        "CANDLE IDENTITY -- NOT AN APPROVAL",
        "Contract: NIFTY@NSE 2026-10-27",
        "Session: 2026-10-01",
        f"Evidence: {evidence} line 1 of 1",
        "Requested at: 2026-10-05T04:00:00Z",
        "Endpoint: historical",
        f"Manifest entry: 2026-10-27/2026-10-01={_KAT1}",
    ]
    assert "approves nothing" in run.out and "FG-8" in run.out


def test_the_printed_entry_is_a_valid_manifest_for_exactly_that_session(tmp_path: Path) -> None:
    run = _identity(_evidence(tmp_path, _observation()))
    entry = next(line for line in run.out.splitlines() if line.startswith("Manifest entry: "))

    approvals = parse_futures_candle_approval_manifest(
        entry.removeprefix("Manifest entry: "), _CONTRACT.product
    )

    assert approvals.approved_fingerprint(_CONTRACT, _SESSION) == _KAT1


def test_identity_never_prints_candle_values_or_the_raw_record(tmp_path: Path) -> None:
    evidence = _evidence(tmp_path, _observation())

    run = _identity(evidence)
    text = run.out + run.err

    for value in ("101.25", "102.5", "99.75", "100.00", "2730", "777", "SYNTHETIC"):
        assert value not in text, value
    for name in ("open", "high", "low", "close", "volume", "open_interest", "instrument_key"):
        assert f'"{name}"' not in text and f"{name}=" not in text


def test_several_matching_records_are_ambiguous_until_one_line_is_chosen(tmp_path: Path) -> None:
    evidence = _evidence(
        tmp_path,
        _observation(requested_at=_SAME_DAY, close="100.5"),
        _observation(),
        _observation(),
    )

    ambiguous = _identity(evidence)
    chosen = _identity(evidence, "--line", "2")
    same_day = _identity(evidence, "--line", "1")

    assert ambiguous.code == ExitCode.INPUT
    assert "3 evidence records of NIFTY@NSE 2026-10-27 session 2026-10-01" in ambiguous.err
    assert "line 1 (same-day" in ambiguous.err and "line 2 (historical" in ambiguous.err
    assert "Manifest entry" not in ambiguous.out
    assert chosen.code == ExitCode.SUCCESS and _KAT1 in chosen.out
    assert same_day.code == ExitCode.SUCCESS
    assert (
        "Endpoint: same-day" in same_day.out and "WARNING: a same-day observation" in same_day.out
    )
    assert _KAT1 not in same_day.out  # a different close is a different version


@pytest.mark.parametrize(
    ("argv", "trading_date", "code", "message"),
    [
        (["--line", "1"], "2026-10-05", ExitCode.INPUT, "is not a record of"),
        ([], "2026-10-05", ExitCode.INPUT, "No evidence record of"),
        (["--line", "9"], "2026-10-01", ExitCode.INPUT, "Evidence line 9 is not a record"),
    ],
    ids=["line-for-another-session", "no-record", "no-such-line"],
)
def test_a_wrong_selection_produces_nothing(
    tmp_path: Path, argv: list[str], trading_date: str, code: ExitCode, message: str
) -> None:
    run = _identity(_evidence(tmp_path, _observation()), *argv, trading_date=trading_date)

    assert run.code == code
    assert message in run.err
    assert "fdc1-sha256:" not in run.out + run.err


@pytest.mark.parametrize(
    ("observation", "message"),
    [
        (_observation(candle=False), "recorded no candle"),
        (_observation(raw_volume="2731"), "no whole contract volume"),
    ],
    ids=["no-candle", "fractional-volume"],
)
def test_an_unusable_record_produces_nothing(tmp_path: Path, observation, message: str) -> None:
    run = _identity(_evidence(tmp_path, observation))

    assert run.code == ExitCode.STATE
    assert message in run.err


def test_a_non_session_date_produces_nothing(tmp_path: Path) -> None:
    run = _identity(
        _evidence(tmp_path, _observation(trading_date=date(2026, 10, 3))), trading_date="2026-10-03"
    )

    assert run.code == ExitCode.INPUT
    assert "2026-10-03 is not an NSE trading session" in run.err


@pytest.mark.parametrize(
    ("content", "code"),
    [
        (b"{not json}\n", ExitCode.STATE),
        (b'{"schema": "northstar.upstox-daily-candle-observation/1"}', ExitCode.STATE),
        (b'{"schema": "something-else/1"}\n', ExitCode.STATE),
    ],
    ids=["invalid-json", "truncated", "other-schema"],
)
def test_malformed_evidence_is_refused_whole(tmp_path: Path, content: bytes, code) -> None:
    good = _evidence(tmp_path, _observation())
    good.write_bytes(good.read_bytes() + content)

    run = _identity(good, "--line", "1")

    assert run.code == code
    assert "fdc1-sha256:" not in run.out


def test_a_missing_evidence_file_is_configuration(tmp_path: Path) -> None:
    run = _identity(tmp_path / "absent.jsonl")

    assert run.code == ExitCode.CONFIGURATION
    assert not (tmp_path / "absent.jsonl").exists()


def test_identity_reads_no_configuration_and_writes_nothing(tmp_path: Path) -> None:
    evidence = _evidence(tmp_path, _observation())
    before = hashlib.sha256(evidence.read_bytes()).hexdigest()
    env = TrackingEnv({APPROVED_CANDLES_VARIABLE: "", "NORTHSTAR_DATABASE": "x"})

    run = _identity(evidence, env=env)

    assert run.code == ExitCode.SUCCESS
    assert env.read == []  # no manifest, no finality, no database setting
    assert hashlib.sha256(evidence.read_bytes()).hexdigest() == before
    assert sorted(p.name for p in tmp_path.iterdir()) == ["evidence.jsonl"]


# ---------------------------------------------------------------------------
# Protection marker: init and status
# ---------------------------------------------------------------------------


def _snapshot(op: Operator) -> dict[str, list[tuple]]:
    return {table: op.rows(f"SELECT * FROM {table} ORDER BY rowid") for table in _TABLES}


def _init(database: Path) -> Outcome:
    return _cli(["protection", "init", "--database", str(database), "--exchange", "NSE"])


def _status(database: Path, *extra: str, env=None) -> Outcome:
    return _cli(
        ["protection", "status", "--database", str(database), "--exchange", "NSE", *extra], env
    )


def _marker_rows(database: Path) -> list[tuple]:
    with closing(sqlite3.connect(database)) as db:
        if not db.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (_MARKER,)
        ).fetchone():
            return []
        return db.execute(f"SELECT * FROM {_MARKER}").fetchall()


def test_init_marks_an_unprotected_database_without_touching_trading_data(tmp_path: Path) -> None:
    op = _operator(tmp_path, "init")
    before = _snapshot(op)

    run = _init(op.database)

    assert run.code == ExitCode.SUCCESS, run.err
    assert "PROTECTION: INITIALIZED -- verified" in run.out
    assert "Protected: yes (marked by operator-initialized)" in run.out
    assert _marker_rows(op.database) == [("NSE", "operator-initialized")]
    assert _snapshot(op) == before


def test_init_is_idempotent_and_never_overwrites_an_existing_marker(tmp_path: Path) -> None:
    op = _operator(tmp_path, "idempotent")
    assert _daily(op, final_through=21, go_live=21).code == ExitCode.SUCCESS  # marked by a run
    before = _snapshot(op)

    first, second = _init(op.database), _init(op.database)

    assert first.code == second.code == ExitCode.SUCCESS
    assert "PROTECTION: ALREADY PRESENT -- unchanged" in second.out
    assert _marker_rows(op.database) == [("NSE", "operator-reviewed")]
    assert _snapshot(op) == before


def test_init_never_creates_or_adopts_a_file(tmp_path: Path) -> None:
    absent = tmp_path / "absent.sqlite3"
    foreign = tmp_path / "foreign.sqlite3"
    with closing(sqlite3.connect(foreign)) as db:
        db.execute("CREATE TABLE unrelated (x)")

    missing, unrelated = _init(absent), _init(foreign)

    assert missing.code == unrelated.code == ExitCode.CONFIGURATION
    assert not absent.exists()
    assert "not a Northstar futures database" in unrelated.err
    assert _marker_rows(foreign) == []


@pytest.mark.parametrize(
    "corrupt",
    [
        f"CREATE TABLE {_MARKER} (exchange_code TEXT, note TEXT)",
        f"CREATE TABLE {_MARKER} (exchange_code TEXT PRIMARY KEY NOT NULL, "
        "protected_by_mode TEXT NOT NULL); "
        f"INSERT INTO {_MARKER} VALUES ('NSE', 'disabled')",
    ],
    ids=["wrong-columns", "unknown-mode"],
)
def test_an_invalid_marker_is_detected_and_never_written_over(tmp_path: Path, corrupt: str) -> None:
    op = _operator(tmp_path, "invalid")
    with closing(sqlite3.connect(op.database)) as db:
        db.executescript(corrupt)
    marker_before = _marker_rows(op.database)

    init, status = _init(op.database), _status(op.database)
    paper = op.paper(21)  # any write path is refused too, never treated as unprotected

    assert init.code == status.code == paper.code == ExitCode.CONFIGURATION
    assert "invalid" in init.err.lower()
    assert "Marker: present, INVALID" in status.out and "STATUS: UNSAFE" in status.out
    assert _marker_rows(op.database) == marker_before


def test_a_failed_init_leaves_no_partial_marker(tmp_path: Path, monkeypatch) -> None:
    op = _operator(tmp_path, "failed")

    def failing_insert(db, exchange, mode):
        raise sqlite3.OperationalError("disk I/O error (synthetic)")

    monkeypatch.setattr(protection_module, "_insert", failing_insert)
    run = _init(op.database)

    assert run.code == ExitCode.CONFIGURATION
    with closing(sqlite3.connect(op.database)) as db:
        assert not db.execute(
            "SELECT 1 FROM sqlite_master WHERE name = ?", (_MARKER,)
        ).fetchone()  # the CREATE TABLE was rolled back with the INSERT


def test_init_refuses_while_another_writer_holds_the_lock(tmp_path: Path) -> None:
    op = _operator(tmp_path, "locked")

    with DatabaseOperationsLock(op.database):
        run = _init(op.database)

    assert run.code == ExitCode.STATE
    assert _marker_rows(op.database) == []


def test_status_is_read_only(tmp_path: Path) -> None:
    op = _operator(tmp_path, "read-only")
    files_before = sorted(p.name for p in tmp_path.iterdir())
    digest = hashlib.sha256(op.database.read_bytes()).hexdigest()
    mtime = op.database.stat().st_mtime_ns

    run = _status(op.database)

    assert run.code == ExitCode.SUCCESS
    assert "Marker: absent" in run.out and "Protected: no" in run.out
    assert "This build enforces reviewed-candle protection: version 1" in run.out
    assert hashlib.sha256(op.database.read_bytes()).hexdigest() == digest
    assert op.database.stat().st_mtime_ns == mtime
    assert sorted(p.name for p in tmp_path.iterdir()) == files_before


def test_status_requires_protection_and_a_compatible_configuration(tmp_path: Path) -> None:
    op = _operator(tmp_path, "preflight")
    unprotected = _status(op.database, "--require-protected")
    assert _init(op.database).code == ExitCode.SUCCESS
    weaker = _status(op.database, "--require-protected", env={})
    approved = _status(
        op.database,
        "--require-protected",
        env={
            "NORTHSTAR_FUTURES_DAILY_BAR_FINALITY": "operator-approved",
            "NORTHSTAR_FUTURES_FINAL_THROUGH": "2026-09-30",
        },
    )

    assert unprotected.code == ExitCode.CONFIGURATION
    assert "STATUS: UNSAFE -- the database is not reviewed-candle protected" in unprotected.out
    assert weaker.code == ExitCode.CONFIGURATION
    assert "disabled: REFUSED by a protected database" in weaker.out
    assert approved.code == ExitCode.SUCCESS
    assert "acquires and decides no new NSE session" in approved.out


def test_status_validates_a_reviewed_manifest_without_printing_it(tmp_path: Path) -> None:
    op = _operator(tmp_path, "manifest")
    assert _init(op.database).code == ExitCode.SUCCESS
    entry = f"2026-10-27/2026-10-01={_KAT1}"
    env = {
        "NORTHSTAR_FUTURES_DAILY_BAR_FINALITY": "operator-reviewed",
        "NORTHSTAR_FUTURES_FINAL_THROUGH": "2026-10-01",
        "NORTHSTAR_FUTURES_PRODUCT": "NIFTY",
        APPROVED_CANDLES_VARIABLE: entry,
    }

    valid = _status(op.database, "--require-protected", env=env)
    malformed = _status(
        op.database, "--require-protected", env={**env, APPROVED_CANDLES_VARIABLE: entry.upper()}
    )

    assert valid.code == ExitCode.SUCCESS
    assert "manifest valid, 1 approved candle version(s)" in valid.out
    assert malformed.code == ExitCode.CONFIGURATION
    assert "INVALID manifest" in malformed.out
    for outcome in (valid, malformed):
        assert _KAT1[12:] not in outcome.out + outcome.err
        assert _KAT1[12:].upper() not in outcome.out + outcome.err


# ---------------------------------------------------------------------------
# Release and rollback safety
# ---------------------------------------------------------------------------


def test_an_initialized_marker_protects_before_any_scheduled_run(tmp_path: Path) -> None:
    op = _operator(tmp_path, "before-scheduler")
    assert _init(op.database).code == ExitCode.SUCCESS
    before = _snapshot(op)

    sync, paper = op.sync(21), op.paper(21)  # no finality configured, as a stray process
    daily_disabled = _daily(op, finality="disabled", final_through=21, go_live=21)
    daily_approved = _daily(op, finality="operator-approved", final_through=21, go_live=21)

    for outcome in (sync, paper, daily_disabled, daily_approved):
        assert outcome.code == ExitCode.CONFIGURATION, outcome.err
        assert "STATUS: WAITING" not in outcome.out
        assert "STATUS: COMPLETED" not in outcome.out
    assert _snapshot(op) == before


def test_a_separate_process_cannot_bypass_an_initialized_marker(tmp_path: Path) -> None:
    op = _operator(tmp_path, "separate")
    assert _init(op.database).code == ExitCode.SUCCESS
    before = _snapshot(op)
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("NORTHSTAR_", "UPSTOX_", "DATABENTO_"))
    }
    program = "import sys; from northstar_api.cli import main; sys.exit(main(sys.argv[1:]))"
    argv = ["paper", "run", "--database", str(op.database), "--product", "NIFTY",
            "--exchange", "NSE", "--expiration", "2026-10-27", "--strategy", "s",
            "--portfolio", "p", "--target", "1", "--as-of", "2026-08-27T10:10:00Z"]  # fmt: skip

    result = subprocess.run(
        [sys.executable, "-c", program, *argv], env=env, capture_output=True, text=True, timeout=120
    )

    assert result.returncode == ExitCode.CONFIGURATION, result.stderr
    assert "requires reviewed candles" in result.stderr
    assert _snapshot(op) == before


def test_restoring_an_unmarked_backup_is_caught_by_preflight(tmp_path: Path) -> None:
    op = _operator(tmp_path, "restore")
    backup = tmp_path / "backup.sqlite3"
    shutil.copyfile(op.database, backup)  # taken before protection existed
    assert _init(op.database).code == ExitCode.SUCCESS
    assert _status(op.database, "--require-protected", env={
        "NORTHSTAR_FUTURES_DAILY_BAR_FINALITY": "operator-approved",
        "NORTHSTAR_FUTURES_FINAL_THROUGH": "2026-09-30",
    }).code == ExitCode.SUCCESS  # fmt: skip

    shutil.copyfile(backup, op.database)  # the restore
    preflight = _status(op.database, "--require-protected")

    assert preflight.code == ExitCode.CONFIGURATION
    assert "STATUS: UNSAFE -- the database is not reviewed-candle protected" in preflight.out


def test_a_pre_fg9_build_cannot_run_the_preflight(tmp_path: Path) -> None:
    """The rollback preflight is a command an older build does not have (RB-5)."""
    api = Path(__file__).resolve().parents[1]
    for baseline in ("76cc3e9", "cfa7774"):
        old = subprocess.run(
            ["git", "-C", str(api), "show", f"{baseline}:src/northstar_api/cli.py"],
            capture_output=True, text=True, check=True,
        ).stdout  # fmt: skip
        assert '"protection"' not in old and "northstar_reviewed_candle_protection" not in old
        assert "CANDLE_APPROVAL" not in old

    unknown = _cli(["no-such-group", "status"])
    assert unknown.code == ExitCode.INPUT  # what that older build returns for the preflight


def test_an_older_configuration_restored_with_the_rollback_cannot_write(tmp_path: Path) -> None:
    op = _operator(tmp_path, "older-env")
    assert _init(op.database).code == ExitCode.SUCCESS
    before = _snapshot(op)

    for finality in ("disabled", "operator-approved", ""):
        run = _daily(op, finality=finality, final_through=22, go_live=21)
        assert run.code == ExitCode.CONFIGURATION, (finality, run.err)

    assert _snapshot(op) == before


def test_the_manifest_reaches_the_settings_exactly_as_given() -> None:
    raw = " 2026-10-27/2026-10-01=fdc1-sha256:" + "a" * 64 + " "

    settings = load_session_operation_settings(
        {
            "NORTHSTAR_FUTURES_DAILY_BAR_FINALITY": "operator-reviewed",
            "NORTHSTAR_FUTURES_FINAL_THROUGH": "2026-10-01",
            APPROVED_CANDLES_VARIABLE: raw,
        }
    )

    assert settings.approved_candles == raw  # never trimmed; the strict parser refuses it
