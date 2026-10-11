"""Acceptance: FG-9.2D reviewed-candle runtime guards for NSE futures.

Everything runs through the production CLI and runtime on disposable SQLite
files, against the INDIA-7 synthetic market and its Upstox transport double.
Approved fingerprints are computed by the merged fdc1 implementation over the
candles the double serves, never from real data. The network is refused for
the whole module, and one test runs a genuinely separate process.

Every refusal is checked by complete before/after equality of the four futures
tables, by the double's request count, and by the absence of any WAITING or
COMPLETED status. Paper facts are not one transaction with acquisition: after a
successful acquisition each session commits on its own, as before FG-9.
"""

from __future__ import annotations

import io
import os
import socket
import subprocess
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import FuturesDailyAcquisitionResult
from northstar_infrastructure.market_data import upstox_http
from test_india7_nifty_incremental_operations_acceptance import (
    Operator,
    Outcome,
    _close,
    _day,
    _never_read_the_clock,
)
from test_india8b_nse_chronological_operations import (
    _AFTER_THE_CONTRACT,
    _Clock,
    _daily,
    _env,
    _manual,
    _operator,
    _paper_args,
    _served_approvals,
    _sync_args,
)

from northstar_api.cli import ExitCode, main
from northstar_api.runtime import build_upstox_market_sync_runtime
from northstar_api.settings import APPROVED_CANDLES_VARIABLE

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
        raise AssertionError("FG-9.2D acceptance must not touch the network")

    patcher = pytest.MonkeyPatch()
    patcher.setattr(socket.socket, "connect", refuse)
    patcher.setattr(socket, "create_connection", refuse)
    patcher.setattr(upstox_http, "urlopen", refuse)
    patcher.setattr(databento, "Historical", refuse)
    yield
    patcher.undo()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _snapshot(op: Operator) -> dict[str, list[tuple]]:
    return {table: op.rows(f"SELECT * FROM {table} ORDER BY rowid") for table in _TABLES}


def _requests(op: Operator) -> int:
    return len(op.upstox.candle_requests) + op.upstox.current_day_requests


def _protected(op: Operator) -> bool:
    exists = op.rows(f"SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '{_MARKER}'")
    return bool(exists) and bool(op.rows(f"SELECT 1 FROM {_MARKER} WHERE exchange_code = 'NSE'"))


def _without(manifest: str, *bars: int) -> str:
    dropped = {_day(bar) for bar in bars}
    return ",".join(e for e in manifest.split(",") if e.split("/")[1].split("=")[0] not in dropped)


def _refused(op: Operator, run, code: ExitCode, *, requests: int | None = None):
    """Run, then require the code, unchanged tables and no success or waiting status."""
    before, asked = _snapshot(op), _requests(op)
    outcome = run()
    assert outcome.code == code, (outcome.code, outcome.err)
    assert _snapshot(op) == before
    if requests is not None:
        assert _requests(op) - asked == requests
    assert "STATUS: WAITING" not in outcome.out
    assert "STATUS: COMPLETED" not in outcome.out
    return outcome


def _main(op: Operator, argv: list[str], env: dict[str, str], **kwargs) -> Outcome:
    out, err = io.StringIO(), io.StringIO()
    code = main(
        argv,
        env=env,
        stdout=out,
        stderr=err,
        clock=kwargs.pop("clock", _never_read_the_clock),
        upstox_market_sync_runtime=lambda path, token, current_instant=None, **kw: (
            build_upstox_market_sync_runtime(
                path, token, fetch=op.upstox, current_instant=current_instant, **kw
            )
        ),
        **kwargs,
    )
    return Outcome(code, out.getvalue(), err.getvalue())


def _decided(op: Operator) -> list[str]:
    return [r.decision_instant.value for r in op.facts()["records"]]


def _operated(tmp_path: Path, name: str, through: int = 22) -> Operator:
    """A database operated in reviewed mode from go-live 21 through ``through``."""
    op = _operator(tmp_path, name)
    run = _daily(op, final_through=through, go_live=21)
    assert run.code == ExitCode.SUCCESS, run.err
    assert _protected(op)
    return op


# ---------------------------------------------------------------------------
# 1-5, 18-20, 32. Reviewed daily acquisition
# ---------------------------------------------------------------------------


def test_reviewed_daily_with_matching_approvals_acquires_and_decides(tmp_path: Path) -> None:
    op = _operator(tmp_path, "matching")

    run = _daily(op, final_through=22, go_live=21)

    assert run.code == ExitCode.SUCCESS, run.err
    assert "STATUS: COMPLETED -- 2 session(s) processed" in run.out
    assert _decided(op) == [_close(21), _close(22)]
    assert _protected(op)


@pytest.mark.parametrize(("missing", "named"), [((22,), [22]), ((21, 22), [21, 22])])
def test_reviewed_daily_with_missing_approvals_is_refused_before_the_provider(
    tmp_path: Path, missing: tuple[int, ...], named: list[int]
) -> None:
    op = _operator(tmp_path, "missing")
    approvals = _without(_served_approvals(op), *missing)

    run = _refused(
        op,
        lambda: _daily(op, final_through=22, go_live=21, approvals=approvals),
        ExitCode.CANDLE_APPROVAL,
        requests=0,
    )

    assert "CANDLE_APPROVAL ERROR: FuturesCandleApprovalMissingError" in run.err
    assert ", ".join(_day(b) for b in named) in run.err


@pytest.mark.parametrize("change", ["close", "volume"])
def test_reviewed_daily_with_a_changed_candle_is_refused_before_storage(
    tmp_path: Path, change: str
) -> None:
    op = _operator(tmp_path, "changed")
    approvals = _served_approvals(op)
    if change == "close":
        op.upstox.revised[22] = Decimal(25050)
    else:
        op.upstox.market.volumes[21] += 7

    run = _refused(
        op,
        lambda: _daily(op, final_through=22, go_live=21, approvals=approvals),
        ExitCode.CANDLE_APPROVAL,
        requests=1,  # one fetch; verified and refused before the one write
    )

    assert "FuturesCandleApprovalMismatchError" in run.err
    assert _day(22) in run.err
    assert approvals.split(",")[0].split("=")[1] not in run.err + run.out


def test_reviewed_mode_with_an_empty_manifest_approves_nothing(tmp_path: Path) -> None:
    op = _operator(tmp_path, "empty")

    run = _refused(
        op,
        lambda: _daily(op, final_through=22, go_live=21, approvals=""),
        ExitCode.CANDLE_APPROVAL,
        requests=0,
    )

    assert f"{_day(21)}, {_day(22)}" in run.err


@pytest.mark.parametrize(
    "manifest",
    [
        "2026-10-27/2026-08-26=fdc1-sha256:" + "A" * 64,
        "2026-10-27/2026-08-26=fdc1-sha512:" + "a" * 128,
        " 2026-10-27/2026-08-26=fdc1-sha256:" + "a" * 64,
        "2026-10-27/2026-08-26=fdc1-sha256:" + "a" * 64 + ",",
    ],
    ids=["uppercase", "algorithm", "leading-space", "trailing-comma"],
)
def test_reviewed_mode_with_a_malformed_manifest_is_configuration(
    tmp_path: Path, manifest: str
) -> None:
    op = _operator(tmp_path, "malformed")

    run = _refused(
        op,
        lambda: _daily(op, final_through=22, go_live=21, approvals=manifest),
        ExitCode.CONFIGURATION,
        requests=0,
    )

    assert f"{APPROVED_CANDLES_VARIABLE} is invalid" in run.err
    assert not _protected(op)  # refused before the file was touched


def test_reviewed_mode_without_a_manifest_is_configuration(tmp_path: Path) -> None:
    op = _operator(tmp_path, "absent")
    env = _env(op, final_through=22, go_live=21)
    del env[APPROVED_CANDLES_VARIABLE]

    run = _refused(
        op, lambda: _main(op, ["operations", "daily"], env), ExitCode.CONFIGURATION, requests=0
    )

    assert f"{APPROVED_CANDLES_VARIABLE} is required" in run.err


def test_repeated_runs_with_unchanged_approvals_add_nothing(tmp_path: Path) -> None:
    op = _operated(tmp_path, "repeat")
    before = _snapshot(op)

    again = _daily(op, final_through=22, go_live=21)

    assert again.code == ExitCode.SUCCESS
    assert "STATUS: WAITING" in again.out  # genuinely nothing eligible
    assert _snapshot(op) == before


# ---------------------------------------------------------------------------
# 6-13. Manual Upstox sync and manual paper run in reviewed mode
# ---------------------------------------------------------------------------


def test_manual_sync_with_a_matching_approval_stores_the_bar(tmp_path: Path) -> None:
    op = _operator(tmp_path, "sync-ok")

    run = _manual(op, _sync_args(op, 21), final_through=21)

    assert run.code == ExitCode.SUCCESS, run.err
    assert len(op.facts()["bars"]) == 21
    assert _protected(op)


def test_manual_sync_with_a_missing_approval_makes_no_request(tmp_path: Path) -> None:
    op = _operator(tmp_path, "sync-missing")
    approvals = _without(_served_approvals(op), 21)

    run = _refused(
        op,
        lambda: _manual(op, _sync_args(op, 21), final_through=21, approvals=approvals),
        ExitCode.CANDLE_APPROVAL,
        requests=0,
    )

    assert "FuturesCandleApprovalMissingError" in run.err


def test_manual_sync_with_a_mismatching_approval_stores_nothing(tmp_path: Path) -> None:
    op = _operator(tmp_path, "sync-mismatch")
    approvals = _served_approvals(op)
    op.upstox.revised[21] = Decimal(25010)

    run = _refused(
        op,
        lambda: _manual(op, _sync_args(op, 21), final_through=21, approvals=approvals),
        ExitCode.CANDLE_APPROVAL,
        requests=1,
    )

    assert "FuturesCandleApprovalMismatchError" in run.err


def test_manual_paper_run_within_finality_decides(tmp_path: Path) -> None:
    op = _operator(tmp_path, "paper-ok")
    assert _manual(op, _sync_args(op, 21), final_through=21).code == ExitCode.SUCCESS

    run = _manual(op, _paper_args(op, 21), final_through=21)

    assert run.code == ExitCode.SUCCESS, run.err
    assert _decided(op) == [_close(21)]


def test_manual_paper_run_beyond_finality_is_refused(tmp_path: Path) -> None:
    op = _operated(tmp_path, "paper-beyond", through=21)
    assert _manual(op, _sync_args(op, 22), final_through=22).code == ExitCode.SUCCESS

    run = _refused(
        op, lambda: _manual(op, _paper_args(op, 22), final_through=21), ExitCode.STATE, requests=0
    )

    assert f"reaches session {_day(22)}, which is not approved as final" in run.err


def test_a_previously_stored_unapproved_bar_is_never_decided_on(tmp_path: Path) -> None:
    op = _operator(tmp_path, "stored-unapproved")
    assert op.sync(21).code == ExitCode.SUCCESS  # legacy, unprotected: stored unreviewed
    approvals = _without(_served_approvals(op), 21)

    run = _refused(
        op,
        lambda: _manual(op, _paper_args(op, 21), final_through=21, approvals=approvals),
        ExitCode.CANDLE_APPROVAL,
        requests=0,
    )

    assert "FuturesCandleApprovalMissingError" in run.err


def test_a_previously_stored_matching_bar_is_decided_on(tmp_path: Path) -> None:
    op = _operator(tmp_path, "stored-matching")
    assert op.sync(21).code == ExitCode.SUCCESS

    run = _manual(op, _paper_args(op, 21), final_through=21)

    assert run.code == ExitCode.SUCCESS, run.err
    assert _decided(op) == [_close(21)]


def test_a_previously_stored_mismatching_bar_is_refused_by_paper_and_daily(
    tmp_path: Path,
) -> None:
    op = _operator(tmp_path, "stored-mismatch")
    assert op.sync(21).code == ExitCode.SUCCESS  # the original version is stored
    op.upstox.revised[21] = Decimal(25010)
    approvals = _served_approvals(op)  # the operator approved the revised version

    paper = _refused(
        op,
        lambda: _manual(op, _paper_args(op, 21), final_through=21, approvals=approvals),
        ExitCode.CANDLE_APPROVAL,
        requests=0,
    )
    daily = _refused(
        op,
        lambda: _daily(op, final_through=21, go_live=21, approvals=approvals),
        ExitCode.STATE,
        requests=1,
    )

    assert "FuturesCandleApprovalMismatchError" in paper.err
    assert "FuturesHistoricalMarketDataConflictError" in daily.err


# ---------------------------------------------------------------------------
# 14-16. Pending orders, open positions and replay
# ---------------------------------------------------------------------------


def test_a_pending_order_is_not_filled_from_an_unapproved_session(tmp_path: Path) -> None:
    op = _operated(tmp_path, "pending")  # HOLD at 21, BUY pending at 22
    assert [o for o in op.facts()["orders"]] and not op.facts()["fills"]
    approvals = _without(_served_approvals(op), 23)

    _refused(
        op,
        lambda: _daily(op, final_through=23, go_live=21, approvals=approvals),
        ExitCode.CANDLE_APPROVAL,
        requests=0,
    )

    assert not op.facts()["fills"]


def test_an_open_position_is_not_marked_on_a_mismatching_session(tmp_path: Path) -> None:
    op = _operated(tmp_path, "position", through=23)  # the BUY fills at 23's open
    assert op.facts()["fills"]
    approvals = _served_approvals(op)
    op.upstox.revised[24] = Decimal(24100)

    _refused(
        op,
        lambda: _daily(op, final_through=24, go_live=21, approvals=approvals),
        ExitCode.CANDLE_APPROVAL,
        requests=1,
    )


@pytest.mark.parametrize("finality", ["operator-reviewed", "operator-approved"])
def test_the_latest_decision_replays_without_new_facts(tmp_path: Path, finality: str) -> None:
    op = _operated(tmp_path, f"replay-{finality}")
    before = _snapshot(op)

    run = _manual(op, _paper_args(op, 22), final_through=22, finality=finality)

    assert run.code == ExitCode.SUCCESS, run.err
    assert _snapshot(op) == before


# ---------------------------------------------------------------------------
# 17. Legacy operator-approved
# ---------------------------------------------------------------------------


def test_legacy_operator_approved_never_acquires_an_nse_session(tmp_path: Path) -> None:
    op = _operator(tmp_path, "legacy")

    run = _refused(
        op,
        lambda: _daily(op, finality="operator-approved", final_through=22, go_live=21),
        ExitCode.CONFIGURATION,
        requests=0,
    )

    assert "operator-approved does not authorize acquiring or deciding sessions" in run.err
    assert _protected(op)  # the operated mode protected the file before refusing


def test_legacy_operator_approved_still_waits_when_nothing_is_eligible(tmp_path: Path) -> None:
    op = _operated(tmp_path, "legacy-waiting")

    run = _daily(op, finality="operator-approved", final_through=22, go_live=21)

    assert run.code == ExitCode.SUCCESS
    assert "STATUS: WAITING" in run.out


@pytest.mark.parametrize("command", ["sync", "paper"])
def test_legacy_operator_approved_manual_commands_are_refused(tmp_path: Path, command: str) -> None:
    op = _operated(tmp_path, f"legacy-{command}")
    argv = _sync_args(op, 23) if command == "sync" else _paper_args(op, 23)

    _refused(
        op,
        lambda: _manual(op, argv, finality="operator-approved", final_through=23),
        ExitCode.CONFIGURATION,
        requests=0,
    )


# ---------------------------------------------------------------------------
# 21-24. Databento is never used for NSE
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("provider", ["databento", None], ids=["selected", "default"])
def test_nse_daily_never_uses_databento(tmp_path: Path, provider: str | None) -> None:
    op = _operator(tmp_path, f"databento-daily-{provider}")
    env = _env(op, final_through=22, go_live=21, provider=provider or "")
    if provider is None:
        del env["NORTHSTAR_FUTURES_MARKET_DATA_PROVIDER"]
    env["DATABENTO_API_KEY"] = "placeholder-not-a-key"

    run = _refused(
        op, lambda: _main(op, ["operations", "daily"], env), ExitCode.CONFIGURATION, requests=0
    )

    assert "operations daily refuses Databento for NIFTY@NSE 2026-10-27" in run.err


@pytest.mark.parametrize("explicit", [True, False], ids=["selected", "default"])
def test_nse_manual_sync_never_uses_databento(tmp_path: Path, explicit: bool) -> None:
    op = _operator(tmp_path, f"databento-sync-{explicit}")
    argv = _sync_args(op, 21)[:-2] + (["--provider", "databento"] if explicit else [])
    env = {"DATABENTO_API_KEY": "placeholder-not-a-key"}
    built = []

    run = _refused(
        op,
        lambda: _main(op, argv, env, market_sync_runtime=lambda *a: built.append(a)),
        ExitCode.CONFIGURATION,
        requests=0,
    )

    assert "market-data sync refuses Databento" in run.err
    assert built == []  # no Databento acquisition was even composed


def test_a_cme_databento_sync_is_unchanged(tmp_path: Path) -> None:
    built = []

    class Acquisition:
        def execute(self, query):
            return FuturesDailyAcquisitionResult(query, 0, 0)

    def factory(path, key):
        built.append((path, key))
        return Acquisition()

    out, err = io.StringIO(), io.StringIO()
    code = main(
        ["market-data", "sync", "--database", str(tmp_path / "cme.sqlite3"),
         "--product", "ES", "--exchange", "CME", "--expiration", "2026-12-18",
         "--start", "2026-09-28", "--end", "2026-09-28"],
        env={"DATABENTO_API_KEY": "placeholder-not-a-key"},
        stdout=out,
        stderr=err,
        market_sync_runtime=factory,
    )  # fmt: skip

    assert code == ExitCode.SUCCESS, err.getvalue()
    assert len(built) == 1


# ---------------------------------------------------------------------------
# 25. Expiry exception precedence
# ---------------------------------------------------------------------------


def test_an_expiry_exception_still_precedes_any_approval_failure(tmp_path: Path) -> None:
    op = _operated(tmp_path, "expiry")  # BUY pending at 22: exposed

    run = _daily(op, clock=_Clock(_AFTER_THE_CONTRACT), final_through=23, go_live=21, approvals="")

    assert run.code == ExitCode.EXPIRY_EXCEPTION
    assert "STATUS: EXPIRY EXCEPTION" in run.out
    assert "CANDLE_APPROVAL" not in run.err


# ---------------------------------------------------------------------------
# 26-29, 33. OD-9: configuration cannot weaken a protected database
# ---------------------------------------------------------------------------


def test_missing_configuration_on_an_unprotected_development_file_is_unchanged(
    tmp_path: Path,
) -> None:
    op = _operator(tmp_path, "development")

    synced, ran = op.cycle(21)  # the INDIA-7 manual workflow, no finality configured

    assert synced.code == ran.code == ExitCode.SUCCESS
    assert _decided(op) == [_close(21)]
    assert not _protected(op)


@pytest.mark.parametrize("command", ["sync", "paper"])
def test_missing_configuration_on_a_protected_file_is_refused(tmp_path: Path, command: str) -> None:
    op = _operated(tmp_path, f"protected-{command}")
    run_manual = (lambda: op.sync(23)) if command == "sync" else (lambda: op.paper(23))

    run = _refused(op, run_manual, ExitCode.CONFIGURATION, requests=0)

    assert "This database requires reviewed candles for NSE futures" in run.err


@pytest.mark.parametrize(
    "finality", ["disabled", "", "DISABLED"], ids=["disabled", "blank", "case"]
)
def test_a_weaker_daily_configuration_on_a_protected_file_is_refused(
    tmp_path: Path, finality: str
) -> None:
    op = _operated(tmp_path, f"weaker-{finality or 'blank'}")

    run = _refused(
        op,
        lambda: _daily(op, finality=finality, final_through=23, go_live=21),
        ExitCode.CONFIGURATION,
        requests=0,
    )

    assert "requires reviewed candles" in run.err


def test_an_unknown_mode_is_configuration(tmp_path: Path) -> None:
    op = _operator(tmp_path, "unknown")

    run = _refused(
        op,
        lambda: _daily(op, finality="automatic", final_through=22, go_live=21),
        ExitCode.CONFIGURATION,
        requests=0,
    )

    assert "NORTHSTAR_FUTURES_DAILY_BAR_FINALITY is invalid" in run.err


def test_a_second_process_without_configuration_cannot_write(tmp_path: Path) -> None:
    op = _operated(tmp_path, "second-process")
    before = _snapshot(op)
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("NORTHSTAR_", "UPSTOX_", "DATABENTO_"))
    }
    program = "import sys; from northstar_api.cli import main; sys.exit(main(sys.argv[1:]))"

    for argv in (_paper_args(op, 23), ["operations", "daily"]):
        result = subprocess.run(
            [sys.executable, "-c", program, *argv],
            env={**env, "NORTHSTAR_DATABASE": str(op.database)} if argv[0] == "operations" else env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == ExitCode.CONFIGURATION, result.stderr
        assert "STATUS: COMPLETED" not in result.stdout
    assert _snapshot(op) == before


def test_a_restored_older_configuration_cannot_downgrade_the_file(tmp_path: Path) -> None:
    op = _operated(tmp_path, "restore")
    before = _snapshot(op)

    # An older .env restored by hand: legacy mode, or no finality settings at all.
    older_approved = _daily(op, finality="operator-approved", final_through=23, go_live=21)
    older_none = op.paper(23)
    older_waiting = _daily(op, finality="operator-approved", final_through=22, go_live=21)

    assert older_approved.code == ExitCode.CONFIGURATION
    assert older_none.code == ExitCode.CONFIGURATION
    assert older_waiting.code == ExitCode.SUCCESS and "STATUS: WAITING" in older_waiting.out
    assert _snapshot(op) == before
    assert _protected(op)


def test_the_protection_marker_is_the_only_new_table(tmp_path: Path) -> None:
    op = _operated(tmp_path, "schema")

    tables = {row[0] for row in op.rows("SELECT name FROM sqlite_master WHERE type = 'table'")}

    assert _MARKER in tables
    assert op.rows(f"SELECT exchange_code, protected_by_mode FROM {_MARKER}") == [
        ("NSE", "operator-reviewed")
    ]


def test_the_routing_clock_is_still_read_at_most_once(tmp_path: Path) -> None:
    op = _operator(tmp_path, "clock")
    clock = _Clock(datetime(2026, 10, 27, 3, 0, tzinfo=UTC))

    assert _daily(op, clock=clock, final_through=22, go_live=21).code == ExitCode.SUCCESS
    assert clock.reads == 1
