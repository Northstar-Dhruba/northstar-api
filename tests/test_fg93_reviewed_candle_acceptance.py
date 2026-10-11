"""Acceptance: FG-9.3 end-to-end synthetic acceptance of reviewed-candle protection.

Every scenario runs through the production CLI (``northstar_api.cli.main`` or a
separate ``python`` process), the production runtime builders, the real
Application use cases, the real NSE calendar and real SQLite files. The only
substitute is the provider: the INDIA-7 Upstox HTTP double, injected as the
transport of the real Upstox adapter and the real evidence collector. All
candles are synthetic; the network is refused for the whole module.

Scenario H also runs two genuine pre-FG-9 builds (``76cc3e9`` on ``main`` and
``cfa7774`` on ``develop``), extracted from Git history into a temporary
directory, in separate processes. Those tests need the full Git history
(CI checks out with ``fetch-depth: 0``) and they demonstrate limitations; they
do not claim those limitations away.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import socket
import sqlite3
import subprocess
import sys
import tarfile
from contextlib import closing
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from northstar_application.application_services import (
    acquire_futures_native_daily_history as acquisition,
)
from northstar_core.foundation.value_objects import PointInTime
from northstar_infrastructure.market_data import (
    SQLiteFuturesHistoricalMarketDataStore,
    UpstoxDailyCandleEvidenceCollector,
    upstox_http,
)
from test_india7_nifty_incremental_operations_acceptance import (
    _CONTRACT,
    _LOT,
    _TOKEN,
    Operator,
    Outcome,
    _close,
    _day,
)
from test_india8b_nse_chronological_operations import (
    _AFTER_THE_CONTRACT,
    _Clock,
    _daily,
    _env,
    _manual,
    _operator,
    _paper_args,
    _sync_args,
)
from test_india8d_operational_status import _settings
from test_india_windows_operations_scheduler import Harness, _windows

from northstar_api import reviewed_candle_protection as protection_module
from northstar_api.cli import ExitCode, main
from northstar_api.operational_status import operational_status
from northstar_api.operations_lock import DatabaseOperationsLock
from northstar_api.runtime import build_database_runtime
from northstar_api.settings import APPROVED_CANDLES_VARIABLE

_API = Path(__file__).resolve().parents[1]
_TABLES = (
    "futures_ohlcv",
    "futures_forward_research_records",
    "futures_paper_orders",
    "futures_paper_fills",
)
_MARKER = "northstar_reviewed_candle_protection"
# Evidence is observed after the sessions, so Upstox's historical endpoint answers.
_OBSERVED_AT = datetime(2026, 10, 27, 3, 0, tzinfo=UTC)
_FDC1 = re.compile(r"fdc1-sha256:[0-9a-f]{64}")


@pytest.fixture(scope="module", autouse=True)
def no_network():
    import databento

    def refuse(*args, **kwargs):
        raise AssertionError("FG-9.3 acceptance must not touch the network")

    patcher = pytest.MonkeyPatch()
    patcher.setattr(socket.socket, "connect", refuse)
    patcher.setattr(socket, "create_connection", refuse)
    patcher.setattr(upstox_http, "urlopen", refuse)
    patcher.setattr(databento, "Historical", refuse)
    yield
    patcher.undo()


# ---------------------------------------------------------------------------
# Observation helpers: SQLite state, provider traffic, real CLI commands
# ---------------------------------------------------------------------------


def _snapshot(op: Operator) -> dict[str, list[tuple]]:
    return {table: op.rows(f"SELECT * FROM {table} ORDER BY rowid") for table in _TABLES}


def _marker(database: Path) -> list[tuple]:
    with closing(sqlite3.connect(database)) as db:
        if not db.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (_MARKER,)
        ).fetchone():
            return []
        return db.execute(f"SELECT * FROM {_MARKER}").fetchall()


def _requests(op: Operator) -> int:
    return len(op.upstox.candle_requests) + op.upstox.current_day_requests


def _cli(argv: list[str], env: dict[str, str] | None = None, **kwargs) -> Outcome:
    out, err = io.StringIO(), io.StringIO()
    code = main(argv, env={} if env is None else env, stdout=out, stderr=err, **kwargs)
    return Outcome(code, out.getvalue(), err.getvalue())


def _observe(op: Operator, evidence: Path, bar: int) -> Outcome:
    """The real ``finality-evidence observe`` command, its collector fed by the double."""
    return _cli(
        ["finality-evidence", "observe", "--evidence", str(evidence), "--product", "NIFTY",
         "--exchange", "NSE", "--expiration", "2026-10-27", "--trading-date", _day(bar)],
        {"UPSTOX_ANALYTICS_TOKEN": _TOKEN},
        clock=_Clock(_OBSERVED_AT),
        upstox_candle_evidence=lambda token, clock: UpstoxDailyCandleEvidenceCollector(
            token, clock=clock, fetch=op.upstox
        ),
    )  # fmt: skip


def _identity(evidence: Path, bar: int, *extra: str) -> Outcome:
    return _cli(
        ["finality-evidence", "identity", "--evidence", str(evidence), "--product", "NIFTY",
         "--exchange", "NSE", "--expiration", "2026-10-27", "--trading-date", _day(bar), *extra]
    )  # fmt: skip


def _entry(outcome: Outcome) -> str:
    assert outcome.code == ExitCode.SUCCESS, outcome.err
    line = next(x for x in outcome.out.splitlines() if x.startswith("Manifest entry: "))
    return line.removeprefix("Manifest entry: ")


def _independent_entry(op: Operator, bar: int) -> str:
    """The fdc1 entry computed from the double's served row by hand, not by Northstar."""
    _label, open_, high, low, close, volume, _oi = op.upstox._row(bar)
    text = (
        "northstar.futures-daily-candle-identity/1\n"
        "product=NIFTY\nexchange=NSE\nexpiration=2026-10-27\n"
        f"trading_date={_day(bar)}\ntimeframe=1d\n"
        f"open={open_}\nhigh={high}\nlow={low}\nclose={close}\n"
        f"volume_contracts={volume // _LOT}\n"
    )
    digest = hashlib.sha256(text.encode("ascii")).hexdigest()
    return f"2026-10-27/{_day(bar)}=fdc1-sha256:{digest}"


def _reviewed_manifest(op: Operator, tmp_path: Path, *bars: int) -> str:
    """Observe, then produce each identity through the real commands; one entry per bar."""
    evidence = tmp_path / f"evidence-{op.database.stem}.jsonl"
    entries = []
    for bar in bars:
        assert _observe(op, evidence, bar).code == ExitCode.SUCCESS
        entries.append(_entry(_identity(evidence, bar, "--line", str(len(entries) + 1))))
    return ",".join(entries)


def _init(database: Path) -> Outcome:
    return _cli(["protection", "init", "--database", str(database), "--exchange", "NSE"])


def _status(database: Path, env: dict[str, str] | None = None) -> Outcome:
    return _cli(
        ["protection", "status", "--database", str(database), "--exchange", "NSE",
         "--require-protected"],
        env,
    )  # fmt: skip


def _refusal(outcome: Outcome) -> None:
    assert "STATUS: WAITING" not in outcome.out
    assert "STATUS: COMPLETED" not in outcome.out


def _decisions(op: Operator) -> list[str]:
    return [r.decision_instant.value for r in op.facts()["records"]]


def _stored_fingerprint(op: Operator, bar: int) -> str:
    rt = build_database_runtime(op.database)
    from northstar_application.application_services import futures_daily_candle_fingerprint
    from northstar_application.ports import FuturesHistoricalMarketDataQuery
    from northstar_core.foundation.value_objects import Timeframe

    close = PointInTime(_close(bar))
    (stored,) = rt.market_repository.get_bars(
        FuturesHistoricalMarketDataQuery(_CONTRACT, Timeframe("1d"), close, close)
    )
    return futures_daily_candle_fingerprint(_CONTRACT, date.fromisoformat(_day(bar)), stored)


def _protected_operator(tmp_path: Path, name: str) -> Operator:
    op = _operator(tmp_path, name)  # bootstrap 1..20, unprotected
    assert _init(op.database).code == ExitCode.SUCCESS
    return op


# ---------------------------------------------------------------------------
# Scenario A: the valid reviewed-candle workflow, end to end
# ---------------------------------------------------------------------------


def test_a_the_reviewed_candle_workflow_end_to_end(tmp_path: Path, monkeypatch) -> None:
    op = _operator(tmp_path, "workflow")

    # 1-4: evidence through the real collector, identities through the real command,
    # each independently recomputed from the double's own row.
    manifest = _reviewed_manifest(op, tmp_path, 21, 22)
    assert manifest.split(",") == [_independent_entry(op, 21), _independent_entry(op, 22)]

    # 5-7: protect the disposable database explicitly, then the real preflight.
    before_init = _snapshot(op)
    init = _init(op.database)
    assert init.code == ExitCode.SUCCESS and "PROTECTION: INITIALIZED -- verified" in init.out
    assert _snapshot(op) == before_init
    reviewed_env = _env(op, final_through=22, go_live=21, approvals=manifest)
    preflight = _status(op.database, reviewed_env)
    assert preflight.code == ExitCode.SUCCESS, preflight.out
    assert "manifest valid, 2 approved candle version(s)" in preflight.out
    assert "STATUS: SAFE" in preflight.out

    # 8-11: the scheduled operation, with the store and the fingerprinting observed.
    verified, stored_batches = [], []
    real_fingerprint = acquisition.futures_daily_candle_fingerprint
    real_store = SQLiteFuturesHistoricalMarketDataStore.store

    def spy_fingerprint(contract, trading_date, bar):
        verified.append(bar)
        return real_fingerprint(contract, trading_date, bar)

    def spy_store(self, bars):
        stored_batches.append(bars)
        return real_store(self, bars)

    monkeypatch.setattr(acquisition, "futures_daily_candle_fingerprint", spy_fingerprint)
    monkeypatch.setattr(SQLiteFuturesHistoricalMarketDataStore, "store", spy_store)
    requests_before = len(op.upstox.candle_requests)

    run = _daily(op, final_through=22, go_live=21, approvals=manifest)

    assert run.code == ExitCode.SUCCESS, run.err
    assert "STATUS: COMPLETED -- 2 session(s) processed" in run.out
    assert op.upstox.candle_requests[requests_before:] == [(_day(21), _day(22))]  # one range
    assert len(stored_batches) == 1 and len(stored_batches[0]) == 2  # one atomic batch
    assert all(s is v for s, v in zip(stored_batches[0], verified, strict=True))
    for bar, entry in zip((21, 22), manifest.split(","), strict=True):
        assert _stored_fingerprint(op, bar) == entry.split("=", 1)[1]

    # 12-13: the paper path consumed exactly the approved sessions.
    facts = op.facts()
    assert _decisions(op) == [_close(21), _close(22)]
    assert len(facts["orders"]) == 1 and not facts["fills"]  # HOLD at 21, BUY pending at 22

    # 14-15: the marker is still valid; every supported rerun is idempotent.
    after = _snapshot(op)
    assert _marker(op.database) == [("NSE", "operator-initialized")]
    assert _status(op.database, reviewed_env).code == ExitCode.SUCCESS
    rerun = _daily(op, final_through=22, go_live=21, approvals=manifest)
    assert rerun.code == ExitCode.SUCCESS and "STATUS: WAITING" in rerun.out
    assert "ALREADY PRESENT" in _init(op.database).out
    replay = _manual(op, _paper_args(op, 22), final_through=22, approvals=manifest)
    assert replay.code == ExitCode.SUCCESS
    assert _snapshot(op) == after


# ---------------------------------------------------------------------------
# Scenario B: missing or invalid approval
# ---------------------------------------------------------------------------


def _b_case(tmp_path: Path, name: str, manifest_of, code: ExitCode, requests: int, **env_extra):
    op = _protected_operator(tmp_path, name)
    good = _reviewed_manifest(op, tmp_path, 21, 22)
    manifest = manifest_of(op, good)
    env = _env(op, final_through=22, go_live=21, approvals=manifest if manifest is not None else "")
    if manifest is None:
        del env[APPROVED_CANDLES_VARIABLE]
    env.update(env_extra)
    before, asked = _snapshot(op), _requests(op)

    outcome = _cli(["operations", "daily"], env, clock=_Clock(_OBSERVED_AT),
                   upstox_market_sync_runtime=_upstox_factory(op))  # fmt: skip

    assert outcome.code == code, (outcome.code, outcome.err)
    assert _snapshot(op) == before
    assert _requests(op) - asked == requests
    _refusal(outcome)
    assert not _FDC1.search(outcome.out + outcome.err)  # never prints a fingerprint
    return outcome


def _upstox_factory(op: Operator, fetch=None):
    from northstar_api.runtime import build_upstox_market_sync_runtime

    return lambda path, token, current_instant=None, **kw: build_upstox_market_sync_runtime(
        path, token, fetch=fetch or op.upstox, current_instant=current_instant, **kw
    )


def _flip(entry: str) -> str:
    key, fingerprint = entry.split("=", 1)
    last = "0" if fingerprint[-1] != "0" else "1"
    return f"{key}={fingerprint[:-1]}{last}"


_B_CASES = {
    # configuration: exit 3, before the lock, the file or any request
    "missing-manifest": (lambda op, good: None, ExitCode.CONFIGURATION, 0),
    "malformed": (lambda op, good: good.upper(), ExitCode.CONFIGURATION, 0),
    "duplicate-entry": (
        lambda op, good: ",".join([good.split(",")[0], good]),
        ExitCode.CONFIGURATION,
        0,
    ),
    # valid configuration, approval missing: exit 8 before the provider
    "empty-with-work": (lambda op, good: "", ExitCode.CANDLE_APPROVAL, 0),
    "session-absent": (lambda op, good: good.split(",")[0], ExitCode.CANDLE_APPROVAL, 0),
    "other-contract": (
        lambda op, good: (
            good.split(",")[0] + "," + good.split(",")[1].replace("2026-10-27/", "2026-11-24/")
        ),
        ExitCode.CANDLE_APPROVAL,
        0,
    ),
    # valid configuration, approval mismatched: exit 8 after one fetch, before storage
    "wrong-fingerprint": (
        lambda op, good: ",".join([good.split(",")[0], _flip(good.split(",")[1])]),
        ExitCode.CANDLE_APPROVAL,
        1,
    ),
    "fingerprint-of-another-session": (
        lambda op, good: ",".join(
            [
                good.split(",")[0].split("=")[0] + "=" + good.split(",")[1].split("=")[1],
                good.split(",")[1].split("=")[0] + "=" + good.split(",")[0].split("=")[1],
            ]
        ),
        ExitCode.CANDLE_APPROVAL,
        1,
    ),
}


@pytest.mark.parametrize("case", list(_B_CASES))
def test_b_missing_or_invalid_approval_fails_closed(tmp_path: Path, case: str) -> None:
    manifest_of, code, requests = _B_CASES[case]

    outcome = _b_case(tmp_path, case, manifest_of, code, requests)

    expected = {
        ExitCode.CONFIGURATION: "CONFIGURATION ERROR:",
        ExitCode.CANDLE_APPROVAL: "CANDLE_APPROVAL ERROR: FuturesCandleApproval",
    }[code]
    assert expected in outcome.err


def test_b_candle_values_that_differ_from_the_reviewed_evidence(tmp_path: Path) -> None:
    op = _protected_operator(tmp_path, "revised")
    manifest = _reviewed_manifest(op, tmp_path, 21, 22)
    op.upstox.revised[22] = op.upstox.market.close(22) + 7  # the provider now serves another
    before, asked = _snapshot(op), _requests(op)

    run = _daily(op, final_through=22, go_live=21, approvals=manifest)

    assert run.code == ExitCode.CANDLE_APPROVAL
    assert "FuturesCandleApprovalMismatchError" in run.err and _day(22) in run.err
    assert _requests(op) - asked == 1
    assert _snapshot(op) == before  # the matching session 21 is not stored either
    _refusal(run)


def test_b_ambiguous_evidence_produces_no_identity(tmp_path: Path) -> None:
    op = _operator(tmp_path, "ambiguous")
    evidence = tmp_path / "ambiguous.jsonl"
    assert _observe(op, evidence, 21).code == ExitCode.SUCCESS
    assert _observe(op, evidence, 21).code == ExitCode.SUCCESS

    outcome = _identity(evidence, 21)

    assert outcome.code == ExitCode.INPUT
    assert "2 evidence records" in outcome.err and "--line" in outcome.err
    assert not _FDC1.search(outcome.out + outcome.err)


# ---------------------------------------------------------------------------
# Scenario C: guards before and after acquisition
# ---------------------------------------------------------------------------


def test_c_a_partially_matching_batch_persists_nothing(tmp_path: Path) -> None:
    op = _protected_operator(tmp_path, "partial")
    manifest = _reviewed_manifest(op, tmp_path, 21, 22, 23)
    op.upstox.revised[22] = op.upstox.market.close(22) - 3  # the middle session differs
    before = _snapshot(op)

    run = _daily(op, final_through=23, go_live=21, approvals=manifest)

    assert run.code == ExitCode.CANDLE_APPROVAL
    assert _snapshot(op) == before
    assert len(op.facts()["bars"]) == 20  # neither 21 nor 23 was stored


def test_c_an_unexpected_provider_session_cannot_pass_the_boundary(tmp_path: Path) -> None:
    op = _protected_operator(tmp_path, "unexpected")
    gap = next(
        bar
        for bar in range(22, 27)
        if (date.fromisoformat(_day(bar)) - date.fromisoformat(_day(bar - 1))).days > 1
    )
    manifest = _reviewed_manifest(op, tmp_path, *range(21, gap + 1))
    extra_day = date.fromisoformat(_day(gap - 1)).toordinal() + 1  # inside the range, no session

    def with_extra(url, headers, timeout):
        body = op.upstox(url, headers, timeout)
        if "/historical-candle/" not in url or "/intraday/" in url:
            return body
        payload = json.loads(body)
        label = date.fromordinal(extra_day).isoformat()
        payload["data"]["candles"].append([f"{label}T00:00:00+05:30", 1, 2, 1, 1, _LOT, 0])
        return json.dumps(payload).encode("utf-8")

    before = _snapshot(op)
    run = _cli(
        ["operations", "daily"],
        _env(op, final_through=gap, go_live=21, approvals=manifest),
        clock=_Clock(_OBSERVED_AT),
        upstox_market_sync_runtime=_upstox_factory(op, with_extra),
    )

    assert run.code == ExitCode.DATA, run.err  # the coverage check refuses it
    assert "candles for non-session dates" in run.err
    assert _snapshot(op) == before
    _refusal(run)


def test_c_a_concurrent_invocation_cannot_write(tmp_path: Path) -> None:
    op = _protected_operator(tmp_path, "concurrent")
    manifest = _reviewed_manifest(op, tmp_path, 21)
    before, asked = _snapshot(op), _requests(op)

    with DatabaseOperationsLock(op.database):
        daily = _daily(op, final_through=21, go_live=21, approvals=manifest)
        sync = _manual(op, _sync_args(op, 21), final_through=21, approvals=manifest)

    assert daily.code == ExitCode.SUCCESS and "DAILY OPERATION: SKIPPED" in daily.out
    assert sync.code == ExitCode.STATE
    assert _snapshot(op) == before and _requests(op) == asked


# ---------------------------------------------------------------------------
# Scenario D: paper-trading safety over stored bars
# ---------------------------------------------------------------------------


def _paper(op: Operator, bar: int, **settings) -> Outcome:
    return _manual(op, _paper_args(op, bar), **settings)


def test_d_approved_stored_candles_are_decided_on(tmp_path: Path) -> None:
    op = _protected_operator(tmp_path, "d-approved")
    manifest = _reviewed_manifest(op, tmp_path, 21)
    assert _manual(op, _sync_args(op, 21), final_through=21, approvals=manifest).code == 0

    run = _paper(op, 21, final_through=21, approvals=manifest)

    assert run.code == ExitCode.SUCCESS, run.err
    assert _decisions(op) == [_close(21)]


@pytest.mark.parametrize("case", ["unapproved", "mismatched", "tampered"])
def test_d_an_unverified_stored_candle_never_reaches_a_decision(tmp_path: Path, case: str) -> None:
    op = _operator(tmp_path, f"d-{case}")
    manifest = _reviewed_manifest(op, tmp_path, 21)
    if case == "tampered":
        assert _init(op.database).code == ExitCode.SUCCESS
        assert _manual(op, _sync_args(op, 21), final_through=21, approvals=manifest).code == 0
        with closing(sqlite3.connect(op.database)) as db, db:  # an external writer
            db.execute(
                "UPDATE futures_ohlcv SET close_value = '25001' WHERE point_in_time = ?",
                (_close(21),),
            )
    else:
        assert op.sync(21).code == ExitCode.SUCCESS  # legacy, unreviewed, unprotected
        assert _init(op.database).code == ExitCode.SUCCESS
        if case == "unapproved":
            manifest = ""
        else:  # the operator reviewed and approved a later, different version
            op.upstox.revised[21] = op.upstox.market.close(21) + 2
            revised = tmp_path / "revised"
            revised.mkdir()
            manifest = _reviewed_manifest(op, revised, 21)
    before = _snapshot(op)

    run = _paper(op, 21, final_through=21, approvals=manifest)

    assert run.code == ExitCode.CANDLE_APPROVAL, run.err
    assert _snapshot(op) == before
    assert _decisions(op) == []
    _refusal(run)


def test_d_finality_mode_and_boundary_refusals(tmp_path: Path) -> None:
    op = _protected_operator(tmp_path, "d-modes")
    manifest = _reviewed_manifest(op, tmp_path, 21, 22)
    assert _daily(op, final_through=21, go_live=21, approvals=manifest).code == 0
    assert _manual(op, _sync_args(op, 22), final_through=22, approvals=manifest).code == 0
    before = _snapshot(op)

    beyond = _paper(op, 22, final_through=21, approvals=manifest)
    legacy = _paper(op, 22, finality="operator-approved", final_through=22)
    disabled = _paper(op, 22, finality="disabled")
    missing = op.paper(22)  # no finality settings at all
    replay = _paper(op, 21, final_through=21, approvals=manifest)

    assert beyond.code == ExitCode.STATE and "not approved as final" in beyond.err
    assert legacy.code == ExitCode.CONFIGURATION
    assert disabled.code == missing.code == ExitCode.CONFIGURATION
    assert replay.code == ExitCode.SUCCESS  # the latest decision, no new session
    for outcome in (beyond, legacy, disabled, missing):
        _refusal(outcome)
    assert _snapshot(op) == before

    newly = _paper(op, 22, final_through=22, approvals=manifest)  # verified, then decided
    assert newly.code == ExitCode.SUCCESS and _decisions(op) == [_close(21), _close(22)]


# ---------------------------------------------------------------------------
# Scenario E: expiry exception precedence
# ---------------------------------------------------------------------------


def test_e_an_expiry_exception_precedes_every_approval_failure(tmp_path: Path) -> None:
    op = _protected_operator(tmp_path, "expiry")
    manifest = _reviewed_manifest(op, tmp_path, 21, 22)
    assert _daily(op, final_through=22, go_live=21, approvals=manifest).code == 0  # exposed
    before = _snapshot(op)

    for approvals in ("", manifest, _flip(manifest.split(",")[1])):
        run = _daily(
            op, clock=_Clock(_AFTER_THE_CONTRACT), final_through=24, go_live=21, approvals=approvals
        )
        assert run.code == ExitCode.EXPIRY_EXCEPTION, run.err
        assert "STATUS: EXPIRY EXCEPTION" in run.out and "CANDLE_APPROVAL" not in run.err
    # A configuration that cannot run at all is refused before any planning, as before.
    weaker = _daily(op, clock=_Clock(_AFTER_THE_CONTRACT), finality="disabled", go_live=21)
    assert weaker.code == ExitCode.CONFIGURATION
    assert _snapshot(op) == before


# ---------------------------------------------------------------------------
# Scenario F: the marker and cross-process safety, with its limits
# ---------------------------------------------------------------------------


def test_f_marker_commands_never_touch_trading_state(tmp_path: Path, monkeypatch) -> None:
    op = _operator(tmp_path, "marker")
    before = _snapshot(op)

    def fail(db, exchange, mode):
        raise sqlite3.OperationalError("synthetic failure")

    with monkeypatch.context() as patch:
        patch.setattr(protection_module, "_insert", fail)
        failed = _init(op.database)
    assert failed.code == ExitCode.CONFIGURATION and _marker(op.database) == []

    first, again = _init(op.database), _init(op.database)
    digest = hashlib.sha256(op.database.read_bytes()).hexdigest()
    status = _status(op.database)

    assert first.code == again.code == ExitCode.SUCCESS
    assert _marker(op.database) == [("NSE", "operator-initialized")]
    assert hashlib.sha256(op.database.read_bytes()).hexdigest() == digest  # status read-only
    assert "Marker: present, valid" in status.out
    assert _snapshot(op) == before


def test_f_a_malformed_marker_is_refused_everywhere(tmp_path: Path) -> None:
    op = _operator(tmp_path, "malformed-marker")
    with closing(sqlite3.connect(op.database)) as db, db:
        db.execute(f"CREATE TABLE {_MARKER} (exchange_code TEXT, note TEXT)")
    before = _snapshot(op)

    outcomes = [_init(op.database), _status(op.database), op.paper(21), op.sync(21)]

    assert [o.code for o in outcomes] == [ExitCode.CONFIGURATION] * 4
    assert _snapshot(op) == before


def test_f_an_independent_process_cannot_bypass_the_marker(tmp_path: Path) -> None:
    op = _protected_operator(tmp_path, "independent")
    before = _snapshot(op)
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("NORTHSTAR_", "UPSTOX_", "DATABENTO_"))
    }
    operation = {
        **env,
        **{k: v for k, v in _env(op, finality=None).items() if not k.startswith("UPSTOX_")},
    }
    program = "import sys; from northstar_api.cli import main; sys.exit(main(sys.argv[1:]))"

    for argv, process_env in ((_paper_args(op, 21), env), (["operations", "daily"], operation)):
        result = subprocess.run(
            [sys.executable, "-c", program, *argv],
            env=process_env, capture_output=True, text=True, timeout=120,
        )  # fmt: skip
        assert result.returncode == ExitCode.CONFIGURATION, result.stderr
        assert "requires reviewed candles" in result.stderr
    assert _snapshot(op) == before


def test_f_limitation_a_restored_unmarked_backup_looks_never_protected(tmp_path: Path) -> None:
    """Northstar cannot prove a marker was lost: the preflight can only say 'absent'."""
    restored = _operator(tmp_path, "restored")
    never = _operator(tmp_path, "never")
    backup = tmp_path / "backup.sqlite3"
    backup.write_bytes(restored.database.read_bytes())
    assert _init(restored.database).code == ExitCode.SUCCESS
    restored.database.write_bytes(backup.read_bytes())  # the restore

    lines = [
        [x for x in _status(op.database).out.splitlines() if x.startswith(("Marker", "Protected"))]
        for op in (restored, never)
    ]

    assert lines[0] == lines[1] == ["Marker: absent", "Protected: no"]
    assert _status(restored.database).code == ExitCode.CONFIGURATION  # caught by preflight


def test_f_limitation_an_external_sqlite_writer_is_not_controlled(tmp_path: Path) -> None:
    op = _protected_operator(tmp_path, "external")

    with closing(sqlite3.connect(op.database)) as db, db:
        db.execute("DELETE FROM futures_ohlcv WHERE point_in_time = ?", (_close(20),))

    assert len(op.facts()["bars"]) == 19  # Northstar did not and cannot stop it
    assert _marker(op.database) == [("NSE", "operator-initialized")]


# ---------------------------------------------------------------------------
# Scenario G: the Windows wrapper, under real Windows PowerShell 5.1
# ---------------------------------------------------------------------------


@_windows
def test_g_the_wrapper_classifies_a_real_refusal_and_leaks_nothing(tmp_path: Path) -> None:
    op = _protected_operator(tmp_path, "wrapper")
    manifest = _reviewed_manifest(op, tmp_path, 21, 22)
    refusal = _daily(op, final_through=22, go_live=21, approvals="")  # a real exit 8
    assert refusal.code == ExitCode.CANDLE_APPROVAL

    harness = Harness(tmp_path / "wrapper")
    harness.env_file.write_bytes(
        harness.env_file.read_bytes()
        + f"{APPROVED_CANDLES_VARIABLE}={manifest}\r\n".encode("ascii")
    )
    harness.scenario(
        stdout="DAILY OPERATION: CHRONOLOGICAL\nSTATUS: WAITING -- printed earlier\n",
        stderr=refusal.err,
        exit_code=ExitCode.CANDLE_APPROVAL,
    )

    result = harness.run()
    status = harness.status()
    logged = "".join(p.read_text(encoding="utf-8") for p in harness.log_files())
    logged += json.dumps(status)

    assert result.returncode == ExitCode.CANDLE_APPROVAL
    assert (status["outcome"], status["exitClass"]) == ("FAILED", "CANDLE_APPROVAL")
    assert status["reason"].startswith("CANDLE_APPROVAL ERROR: FuturesCandleApprovalMissingError")
    assert harness.secret not in logged
    assert "fdc1-sha256:" not in logged and not _FDC1.search(logged)
    assert manifest not in logged


@_windows
@pytest.mark.parametrize(
    ("code", "stderr", "exit_class", "outcome"),
    [
        (7, "EXPIRY_EXCEPTION ERROR: Expiry exception: NIFTY@NSE 2026-10-27\n",
         "EXPIRY_EXCEPTION", "EXPIRY_EXCEPTION"),
        (3, "CONFIGURATION ERROR: This database requires reviewed candles\n",
         "CONFIGURATION", "FAILED"),
    ],
    ids=["expiry", "configuration"],
)  # fmt: skip
def test_g_the_wrapper_keeps_exit_7_and_3(
    tmp_path: Path, code: int, stderr: str, exit_class: str, outcome: str
) -> None:
    harness = Harness(tmp_path)
    harness.scenario(stdout="STATUS: WAITING -- earlier\n", stderr=stderr, exit_code=code)

    assert harness.run().returncode == code
    assert (harness.status()["outcome"], harness.status()["exitClass"]) == (outcome, exit_class)


# ---------------------------------------------------------------------------
# Scenario H: rollback and recovery containment, with genuine older builds
# ---------------------------------------------------------------------------

_OLD_BUILDS = ("76cc3e9", "cfa7774")  # main (the likely deployed build), the M1.5.2 release


@pytest.fixture(scope="module")
def old_builds(tmp_path_factory) -> dict[str, Path]:
    """Each pre-FG-9 API source tree, extracted from Git history; nothing is checked out."""
    root = tmp_path_factory.mktemp("old-builds")
    trees = {}
    for commit in _OLD_BUILDS:
        archive = subprocess.run(
            ["git", "-C", str(_API), "archive", "--format=tar", commit, "src"],
            capture_output=True, check=True,
        ).stdout  # fmt: skip
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
            tar.extractall(root / commit, filter="data")
        trees[commit] = root / commit / "src"
    return trees


def _old(tree: Path, argv: list[str], extra_env: dict[str, str]) -> subprocess.CompletedProcess:
    """Run an older build's CLI in its own process; any HTTP attempt fails locally."""
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("NORTHSTAR_", "UPSTOX_", "DATABENTO_"))
    }
    env.update({"HTTPS_PROXY": "http://127.0.0.1:9", "HTTP_PROXY": "http://127.0.0.1:9"})
    env.update(extra_env)
    program = (
        "import sys; sys.path.insert(0, sys.argv[1]); import northstar_api.cli as cli; "
        "assert cli.__file__.startswith(sys.argv[1]); sys.exit(cli.main(sys.argv[2:]))"
    )
    return subprocess.run(
        [sys.executable, "-I", "-c", program, str(tree), *argv],
        env=env, capture_output=True, text=True, timeout=180,
    )  # fmt: skip


@pytest.mark.parametrize("commit", _OLD_BUILDS)
def test_h_rb5_an_older_build_cannot_run_the_preflight(old_builds, tmp_path, commit) -> None:
    op = _protected_operator(tmp_path, f"rb5-{commit}")

    result = _old(old_builds[commit], ["protection", "status", "--database", str(op.database),
                                       "--exchange", "NSE", "--require-protected"], {})  # fmt: skip

    assert result.returncode == ExitCode.INPUT
    assert "invalid choice: 'protection'" in result.stderr


@pytest.mark.parametrize("commit", _OLD_BUILDS)
def test_h_an_older_build_refuses_the_reviewed_mode(old_builds, tmp_path, commit) -> None:
    op = _protected_operator(tmp_path, f"old-mode-{commit}")
    manifest = _reviewed_manifest(op, tmp_path, 21)
    before = _snapshot(op)
    env = {k: v for k, v in _env(op, final_through=21, go_live=21, approvals=manifest).items()
           if not k.startswith("UPSTOX_")}  # fmt: skip

    result = _old(old_builds[commit], ["operations", "daily"], env)

    assert result.returncode == ExitCode.CONFIGURATION, result.stderr
    assert "NORTHSTAR_FUTURES_DAILY_BAR_FINALITY is invalid" in result.stderr
    assert _snapshot(op) == before


@pytest.mark.parametrize("commit", _OLD_BUILDS)
def test_h_limitation_rb3_an_older_build_ignores_the_marker(old_builds, tmp_path, commit) -> None:
    """RB-3 is containment, not code: an older build writes to a protected database."""
    op = _protected_operator(tmp_path, f"rb3-{commit}")
    manifest = _reviewed_manifest(op, tmp_path, 21)
    assert _manual(op, _sync_args(op, 21), final_through=21, approvals=manifest).code == 0
    assert _decisions(op) == []

    result = _old(old_builds[commit], _paper_args(op, 21), {})  # no finality configured

    assert result.returncode == ExitCode.SUCCESS, result.stderr
    assert _decisions(op) == [_close(21)]  # written despite the marker
    assert _marker(op.database) == [("NSE", "operator-initialized")]  # ignored, not removed


def test_h_rb1_final_through_ahead_of_the_latest_decision_is_visible(tmp_path: Path) -> None:
    op = _protected_operator(tmp_path, "rb1")
    manifest = _reviewed_manifest(op, tmp_path, 21, 22)
    assert _daily(op, final_through=22, go_live=21, approvals=manifest).code == 0

    level = _operations_view(op, final_through=22)
    ahead = _operations_view(op, final_through=24)

    assert level.finality.next_session_outcome == "NOT_YET_FINAL"
    assert ahead.finality.next_session_outcome == "FINAL"  # the RB-1 signal on the dashboard


def _operations_view(op: Operator, final_through: int):
    """The dashboard's operational status, computed exactly as its route does, without HTTP."""
    from northstar_application.ports import FuturesHistoricalMarketDataQuery
    from northstar_core.foundation.value_objects import Timeframe

    settings = _settings(
        op.database, finality="operator-approved", final_through=final_through, go_live=21
    )
    rt = build_database_runtime(op.database)
    bars = rt.market_repository.get_bars(
        FuturesHistoricalMarketDataQuery(settings.contract, Timeframe("1d"))
    )
    cutoff = max((b.point_in_time for b in bars), key=lambda p: p.value)
    snapshot = rt.snapshot.execute(settings.contract, settings.strategy, settings.portfolio, cutoff)
    return operational_status(settings, snapshot, bars)


def test_h_rb2_and_older_environments_cannot_write_through_this_build(tmp_path: Path) -> None:
    op = _protected_operator(tmp_path, "rb2")
    manifest = _reviewed_manifest(op, tmp_path, 21)
    assert _daily(op, final_through=21, go_live=21, approvals=manifest).code == 0
    before = _snapshot(op)

    for finality in ("operator-approved", "disabled", ""):
        run = _daily(op, finality=finality, final_through=23, go_live=21)
        assert run.code == ExitCode.CONFIGURATION, (finality, run.err)
        _refusal(run)
    assert _status(op.database, {}).code == ExitCode.CONFIGURATION  # preflight: not SAFE
    assert _snapshot(op) == before
