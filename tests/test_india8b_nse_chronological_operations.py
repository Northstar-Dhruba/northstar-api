"""Acceptance: INDIA-8B chronological unattended operation for NSE / Upstox.

``northstar operations daily`` with NORTHSTAR_FUTURES_MARKET_DATA_PROVIDER=upstox
runs the chronological operation: under one operations lock it derives the
backlog from persisted facts (the latest frozen decision, or an explicit go-live
session), assesses each resolved NSE session with the configured finality
policy, acquires the final sessions as one range and paper-runs each session at
its own close, oldest first, stopping at the first failure. It reads no clock.

Everything runs through the production CLI and runtime on temporary SQLite, on
real NSE sessions. The synthetic markets, the range-honouring Upstox transport
double and the manual operator are INDIA-7's, so "equivalent to day-by-day
operation" is checked against exactly the INDIA-7 manual workflow. The token is
a placeholder and the network is refused for the whole module.
"""

from __future__ import annotations

import io
import socket
import sqlite3
from contextlib import closing
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from urllib.error import HTTPError

import pytest
from northstar_application.application_services import FuturesPaperExecutionIdentityService
from northstar_application.ports import FuturesSessionResolutionError
from northstar_infrastructure.market_data import (
    NSEFuturesTradingSessionResolver,
    upstox_http,
)
from northstar_infrastructure.persistence import FuturesPaperTradingStorageError
from test_india7_nifty_incremental_operations_acceptance import (
    _E8_BAR,
    _PORTFOLIO,
    _SESSIONS,
    _STRATEGY,
    _TOKEN,
    Operator,
    Outcome,
    _close,
    _day,
    _expiry_market,
    _final_facts,
    _normal_market,
)

from northstar_api import cli as cli_module
from northstar_api.cli import ExitCode, main
from northstar_api.operations_lock import DatabaseOperationsLock
from northstar_api.runtime import build_upstox_market_sync_runtime

# ---------------------------------------------------------------------------
# No network for the whole module
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def no_network():
    import databento

    def refuse(*args, **kwargs):
        raise AssertionError("INDIA-8B acceptance must not touch the network")

    patcher = pytest.MonkeyPatch()
    patcher.setattr(socket.socket, "connect", refuse)
    patcher.setattr(socket, "create_connection", refuse)
    patcher.setattr(upstox_http, "urlopen", refuse)
    patcher.setattr(databento, "Historical", refuse)
    yield
    patcher.undo()


# ---------------------------------------------------------------------------
# The unattended runner, configured only through its environment
# ---------------------------------------------------------------------------


def _never_read_the_clock():
    raise AssertionError("the chronological operation must not read the clock")


def _date(value: int | str | date) -> str:
    if isinstance(value, int):
        return _day(value)
    return value if isinstance(value, str) else value.isoformat()


def _env(
    op: Operator,
    *,
    finality: str | None = "operator-approved",
    final_through: int | str | None = None,
    go_live: int | str | None = None,
    expiration: str = "2026-10-27",
    exchange: str = "NSE",
    token: str | None = _TOKEN,
    provider: str = "upstox",
) -> dict[str, str]:
    env = {
        "NORTHSTAR_DATABASE": str(op.database),
        "NORTHSTAR_FUTURES_PRODUCT": "NIFTY",
        "NORTHSTAR_FUTURES_EXCHANGE": exchange,
        "NORTHSTAR_FUTURES_EXPIRATION": expiration,
        "NORTHSTAR_STRATEGY": _STRATEGY.identity,
        "NORTHSTAR_PORTFOLIO": _PORTFOLIO.identity,
        "NORTHSTAR_TARGET": "1",
        "NORTHSTAR_FUTURES_MARKET_DATA_PROVIDER": provider,
    }
    if token is not None:
        env["UPSTOX_ANALYTICS_TOKEN"] = token
    if finality is not None:
        env["NORTHSTAR_FUTURES_DAILY_BAR_FINALITY"] = finality
    if final_through is not None:
        env["NORTHSTAR_FUTURES_FINAL_THROUGH"] = _date(final_through)
    if go_live is not None:
        env["NORTHSTAR_FUTURES_GO_LIVE"] = _date(go_live)
    return env


def _daily(op: Operator, fetch=None, **settings) -> Outcome:
    out, err = io.StringIO(), io.StringIO()
    code = main(
        ["operations", "daily"],
        env=_env(op, **settings),
        stdout=out,
        stderr=err,
        clock=_never_read_the_clock,
        upstox_market_sync_runtime=lambda path, token: build_upstox_market_sync_runtime(
            path, token, fetch=fetch or op.upstox
        ),
    )
    return Outcome(code, out.getvalue(), err.getvalue())


def _session_block(out: str, bar: int) -> str:
    """Return one session's rendered block: from its header line to the next one."""
    lines = out.splitlines()
    start = lines.index(f"SESSION {_day(bar)}")
    end = next(
        (i for i in range(start + 1, len(lines)) if lines[i].startswith("SESSION 20")),
        len(lines),
    )
    return "\n".join(lines[start:end])


def _decisions(op: Operator) -> list[str]:
    return [r.decision_instant.value for r in op.facts()["records"]]


def _requests_after(op: Operator, count: int) -> list[tuple[str, str]]:
    return op.upstox.candle_requests[count:]


def _operator(tmp_path: Path, name: str, market=None, bootstrap: int = 20) -> Operator:
    op = Operator(tmp_path / f"{name}.sqlite3", market or _normal_market())
    op.bootstrap(bootstrap)
    return op


def _daily_reference(tmp_path: Path, name: str, last: int, market=None, first: int = 21):
    """The INDIA-7 manual workflow: sync D, paper D, one session at a time."""
    op = _operator(tmp_path, name, market, first - 1)
    for bar in range(first, last + 1):
        op.cycle(bar)
    return op


# ---------------------------------------------------------------------------
# A. Disabled finality fails closed
# ---------------------------------------------------------------------------


def test_disabled_finality_acquires_and_decides_nothing(tmp_path: Path) -> None:
    op = _operator(tmp_path, "disabled")
    before, requests = op.facts(), len(op.upstox.candle_requests)

    run = _daily(op, finality=None, go_live=21)

    assert run.code == ExitCode.SUCCESS
    assert "Finality mode: disabled" in run.out
    assert f"  {_day(21)} UNKNOWN: Daily-bar finality is not established" in run.out
    assert "STATUS: WAITING -- Daily-bar finality is not established" in run.out
    assert _requests_after(op, requests) == []
    assert op.facts() == before


# ---------------------------------------------------------------------------
# B, C. Operator-approved: one session, and stopping at the first unapproved one
# ---------------------------------------------------------------------------


def test_one_approved_session_is_acquired_and_paper_run(tmp_path: Path) -> None:
    op = _operator(tmp_path, "one-session")
    requests = len(op.upstox.candle_requests)

    run = _daily(op, final_through=21, go_live=21)

    assert run.code == ExitCode.SUCCESS, run.err
    assert _requests_after(op, requests) == [(_day(21), _day(21))]
    assert _decisions(op) == [_close(21)]
    assert f"Finality mode: operator-approved (final through {_day(21)})" in run.out
    assert f"Go-live session: {_day(21)} (no decision frozen yet)" in run.out
    assert f"  {_day(21)} FINAL: Session {_day(21)} is on or before" in run.out
    assert f"Acquired: {_day(21)} .. {_day(21)} (1 sessions)" in run.out
    assert f"SESSION {_day(21)}" in run.out
    assert "STATUS: COMPLETED -- 1 session(s) processed" in run.out
    assert f"Waiting at {_day(22)}: Session {_day(22)} is after" in run.out
    assert f"session {_day(21)} finality FINAL" in run.err


def test_processing_stops_at_the_first_unapproved_session(tmp_path: Path) -> None:
    op = _operator(tmp_path, "not-yet")
    requests = len(op.upstox.candle_requests)

    run = _daily(op, final_through=22, go_live=21)

    assert run.code == ExitCode.SUCCESS
    assert _requests_after(op, requests) == [(_day(21), _day(22))]
    assert _decisions(op) == [_close(21), _close(22)]
    assert len(op.facts()["bars"]) == 22  # bar 23 was never requested
    assert f"  {_day(23)} NOT_YET_FINAL" in run.out
    assert _day(24) not in run.out  # nothing after the first unapproved session is assessed


def test_an_approval_before_go_live_waits_without_any_request(tmp_path: Path) -> None:
    op = _operator(tmp_path, "approval-behind")
    before, requests = op.facts(), len(op.upstox.candle_requests)

    run = _daily(op, final_through=20, go_live=21)

    assert run.code == ExitCode.SUCCESS
    assert f"STATUS: WAITING -- Session {_day(21)} is after" in run.out
    assert _requests_after(op, requests) == []
    assert op.facts() == before


# ---------------------------------------------------------------------------
# D, E, F. Backlog, go-live and resume
# ---------------------------------------------------------------------------


def test_a_backlog_is_one_range_then_every_cutoff_in_order(tmp_path: Path) -> None:
    op = _operator(tmp_path, "backlog")
    requests = len(op.upstox.candle_requests)

    run = _daily(op, final_through=26, go_live=21)

    assert run.code == ExitCode.SUCCESS, run.err
    assert _requests_after(op, requests) == [(_day(21), _day(26))]
    assert _decisions(op) == [_close(bar) for bar in range(21, 27)]
    positions = [run.out.index(f"SESSION {_day(bar)}") for bar in range(21, 27)]
    assert positions == sorted(positions)
    reference = _daily_reference(tmp_path, "backlog-reference", 26)
    assert _final_facts(op, 26) == _final_facts(reference, 26)


def test_go_live_is_the_first_decision_even_with_more_history(tmp_path: Path) -> None:
    op = _operator(tmp_path, "go-live", bootstrap=25)

    run = _daily(op, final_through=23, go_live=23)

    assert run.code == ExitCode.SUCCESS, run.err
    assert _decisions(op) == [_close(23)]  # nothing before go-live, though bars 1..25 exist


def test_existing_decisions_resume_and_go_live_no_longer_applies(tmp_path: Path) -> None:
    op = _operator(tmp_path, "resume")
    assert _daily(op, final_through=22, go_live=21).code == ExitCode.SUCCESS
    requests = len(op.upstox.candle_requests)

    resumed = _daily(op, final_through=24, go_live=25)  # a later go-live is ignored now

    assert resumed.code == ExitCode.SUCCESS
    assert _requests_after(op, requests) == [(_day(23), _day(24))]
    assert _decisions(op) == [_close(bar) for bar in range(21, 25)]
    assert f"Latest frozen decision: {_close(22)}" in resumed.out
    assert "Go-live session" not in resumed.out


# ---------------------------------------------------------------------------
# G, H. Restart recovery from persisted facts alone
# ---------------------------------------------------------------------------


def test_an_interrupted_frozen_decision_is_completed_without_duplication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    op = _operator(tmp_path, "interrupted")
    assert _daily(op, final_through=21, go_live=21).code == ExitCode.SUCCESS
    assert op.sync(22).code == ExitCode.SUCCESS

    def fail_closed(self, product, start_date, end_date):
        raise FuturesSessionResolutionError("calendar unavailable (fail closed)")

    with monkeypatch.context() as patch:  # bar 22's decision freezes, its execution fails
        patch.setattr(NSEFuturesTradingSessionResolver, "sessions_in_range", fail_closed)
        assert op.paper(22).code == ExitCode.PROVIDER
    assert _decisions(op) == [_close(21), _close(22)] and op.facts()["orders"] == ()

    run = _daily(op, final_through=23)

    assert run.code == ExitCode.SUCCESS, run.err
    facts = op.facts()
    assert _decisions(op) == [_close(21), _close(22), _close(23)]
    [order] = facts["orders"]
    record_22 = next(r for r in facts["records"] if r.decision_instant.value == _close(22))
    assert order.identity == FuturesPaperExecutionIdentityService().order_identity(
        record_22, _PORTFOLIO
    )
    assert "latest frozen cutoff" in run.err and "replayed idempotently" in run.err
    reference = _daily_reference(tmp_path, "interrupted-reference", 23)
    assert _final_facts(op, 23) == _final_facts(reference, 23)


def test_stored_market_data_without_paper_is_still_processed(tmp_path: Path) -> None:
    op = _operator(tmp_path, "data-only")
    assert op.sync(21, 22).code == ExitCode.SUCCESS  # acquired, then the process "died"

    run = _daily(op, final_through=22, go_live=21)

    assert run.code == ExitCode.SUCCESS, run.err
    assert _decisions(op) == [_close(21), _close(22)]


# ---------------------------------------------------------------------------
# I, J, K. Stop on the first failure, recover by rerunning
# ---------------------------------------------------------------------------


def test_a_paper_failure_stops_before_any_later_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    op = _operator(tmp_path, "stop")
    real = cli_module._run_paper_session

    def failing(context, runtime, contract, strategy, portfolio, target, as_of):
        if as_of.value == _close(22):
            raise FuturesSessionResolutionError("calendar unavailable at session 22")
        return real(context, runtime, contract, strategy, portfolio, target, as_of)

    with monkeypatch.context() as patch:
        patch.setattr(cli_module, "_run_paper_session", failing)
        failed = _daily(op, final_through=23, go_live=21)

    assert failed.code == ExitCode.PROVIDER
    assert f"stopped at session {_day(22)}; no later session was processed" in failed.err
    assert _decisions(op) == [_close(21)]  # neither 22 nor 23 decided

    recovered = _daily(op, final_through=23, go_live=21)
    assert recovered.code == ExitCode.SUCCESS, recovered.err
    assert _decisions(op) == [_close(21), _close(22), _close(23)]
    reference = _daily_reference(tmp_path, "stop-reference", 23)
    assert _final_facts(op, 23) == _final_facts(reference, 23)


def test_a_provider_failure_stops_and_a_rerun_recovers(tmp_path: Path) -> None:
    op = _operator(tmp_path, "provider")
    op.upstox.candle_error = HTTPError("u", 503, "Unavailable", {}, io.BytesIO(b""))

    failed = _daily(op, final_through=22, go_live=21)

    assert failed.code == ExitCode.PROVIDER
    assert "UpstoxProviderUnavailableError" in failed.err
    assert _decisions(op) == [] and len(op.facts()["bars"]) == 20

    op.upstox.candle_error = None
    assert _daily(op, final_through=22, go_live=21).code == ExitCode.SUCCESS
    assert _decisions(op) == [_close(21), _close(22)]


def test_a_revised_candle_is_state_and_nothing_is_decided(tmp_path: Path) -> None:
    op = _operator(tmp_path, "revised")
    assert op.sync(21).code == ExitCode.SUCCESS
    stored = op.facts()["bars"]
    op.upstox.revised[21] = Decimal(25050)

    run = _daily(op, final_through=22, go_live=21)

    assert run.code == ExitCode.STATE
    assert "FuturesHistoricalMarketDataConflictError" in run.err
    assert op.facts()["bars"] == stored
    assert _decisions(op) == []


def test_a_missing_expected_candle_is_data_and_nothing_is_decided(tmp_path: Path) -> None:
    op = _operator(tmp_path, "missing")
    op.upstox.omit.add(22)

    run = _daily(op, final_through=22, go_live=21)

    assert run.code == ExitCode.DATA
    assert "FuturesDailySessionCoverageError" in run.err
    assert _decisions(op) == [] and len(op.facts()["bars"]) == 20


# ---------------------------------------------------------------------------
# L. The unresolved 2026-11-08 Muhurat session fails closed
# ---------------------------------------------------------------------------


def test_a_november_backlog_across_muhurat_fails_closed_before_any_request(
    tmp_path: Path,
) -> None:
    op = Operator(tmp_path / "november.sqlite3", _normal_market())

    run = _daily(op, expiration="2026-11-23", final_through="2026-11-05", go_live="2026-11-02")

    assert run.code == ExitCode.PROVIDER
    assert "FuturesSessionResolutionError" in run.err and "2026-11-08" in run.err
    assert op.upstox.candle_requests == []


# ---------------------------------------------------------------------------
# M. Expiry: the guard flattens, then the runner asks for a rollover
# ---------------------------------------------------------------------------


def test_the_expiry_window_and_then_rollover_required(tmp_path: Path) -> None:
    e6, e5 = _E8_BAR + 2, _E8_BAR + 3
    op = _operator(tmp_path, "expiry", _expiry_market(), bootstrap=_E8_BAR - 1)

    run = _daily(op, final_through="2026-10-30", go_live=_E8_BAR)

    assert run.code == ExitCode.SUCCESS, run.err
    assert "Expiry flatten: yes" in _session_block(run.out, e6)
    for bar in range(e5, len(_SESSIONS) + 1):
        assert "Reason: EXPIRY FLATTEN WINDOW" in _session_block(run.out, bar)
    facts = op.facts()
    assert facts["fills"][-1].filled_at.value == _close(e5)  # the flatten fills at the E-5 open
    assert op.valuation(len(_SESSIONS)).contracts[0].position is None
    reference = _daily_reference(
        tmp_path, "expiry-reference", len(_SESSIONS), _expiry_market(), first=_E8_BAR
    )
    assert _final_facts(op, len(_SESSIONS)) == _final_facts(reference, len(_SESSIONS))

    requests = len(op.upstox.candle_requests)
    after = _daily(op, final_through="2026-10-30")

    assert after.code == ExitCode.CONFIGURATION
    assert "STATUS: ROLLOVER REQUIRED" in after.out
    assert "Rollover required" in after.err and "nothing rolls automatically" in after.err
    assert _requests_after(op, requests) == []  # no provider retry beyond expiry
    assert op.facts() == facts


# ---------------------------------------------------------------------------
# N, O. One lock for the whole cycle; an identical rerun adds nothing
# ---------------------------------------------------------------------------


def test_no_other_writer_can_mutate_during_the_cycle(tmp_path: Path) -> None:
    op = _operator(tmp_path, "locked")
    probes: dict[str, Outcome] = {}

    def probing(url, headers, timeout):
        if "historical-candle" in url and not probes:
            probes["paper"] = op.paper(21)
            probes["daily"] = _daily(op, final_through=22, go_live=21)
        return op.upstox(url, headers, timeout)

    run = _daily(op, fetch=probing, final_through=22, go_live=21)

    assert run.code == ExitCode.SUCCESS, run.err
    assert probes["paper"].code == ExitCode.STATE
    assert "Another Northstar operations writer is active" in probes["paper"].err
    assert probes["daily"].code == ExitCode.SUCCESS
    assert probes["daily"].out.startswith("DAILY OPERATION: SKIPPED")
    assert _decisions(op) == [_close(21), _close(22)]


def test_a_held_lock_skips_the_runner_without_mutation(tmp_path: Path) -> None:
    op = _operator(tmp_path, "held")
    before, requests = op.facts(), len(op.upstox.candle_requests)

    with DatabaseOperationsLock(op.database):
        run = _daily(op, final_through=22, go_live=21)

    assert run.code == ExitCode.SUCCESS
    assert run.out.startswith("DAILY OPERATION: SKIPPED")
    assert op.facts() == before and _requests_after(op, requests) == []


def test_an_identical_rerun_adds_no_facts(tmp_path: Path) -> None:
    op = _operator(tmp_path, "repeat")
    assert _daily(op, final_through=22, go_live=21).code == ExitCode.SUCCESS
    facts, requests = op.facts(), len(op.upstox.candle_requests)

    again = _daily(op, final_through=22, go_live=21)

    assert again.code == ExitCode.SUCCESS
    assert f"STATUS: WAITING -- Session {_day(23)} is after" in again.out
    assert op.facts() == facts
    assert _requests_after(op, requests) == []


# ---------------------------------------------------------------------------
# Configuration errors: CONFIGURATION (3), before any request or mutation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("settings", "message"),
    [
        ({"final_through": None}, "NORTHSTAR_FUTURES_FINAL_THROUGH is required"),
        ({"final_through": "2026/09/29"}, "NORTHSTAR_FUTURES_FINAL_THROUGH is invalid"),
        ({"finality": "automatic"}, "NORTHSTAR_FUTURES_DAILY_BAR_FINALITY is invalid"),
        ({"go_live": None}, "set NORTHSTAR_FUTURES_GO_LIVE"),
        ({"go_live": "2026-08-01"}, "is not a trading session"),  # a Saturday
        ({"go_live": "2026-10-20"}, "is not a trading session"),  # an NSE holiday
        ({"go_live": "2026-10-28"}, "is after the expiry"),
        ({"exchange": "CME"}, "operates NSE contracts only"),
        ({"token": None}, "UPSTOX_ANALYTICS_TOKEN is not set"),
    ],
    ids=[
        "approved-without-date",
        "malformed-date",
        "unknown-mode",
        "no-go-live",
        "go-live-weekend",
        "go-live-holiday",
        "go-live-after-expiry",
        "wrong-venue",
        "no-token",
    ],
)
def test_invalid_configuration_is_rejected_without_mutation(
    tmp_path: Path, settings: dict, message: str
) -> None:
    op = _operator(tmp_path, "config")
    before, requests = op.facts(), len(op.upstox.candle_requests)
    values = {"final_through": 22, "go_live": 21, **settings}

    run = _daily(op, **values)

    assert run.code == ExitCode.CONFIGURATION
    assert message in run.err
    assert op.facts() == before and _requests_after(op, requests) == []
    assert _TOKEN not in run.out + run.err


# ---------------------------------------------------------------------------
# Q, R. Clock-free and isolated
# ---------------------------------------------------------------------------


class _NoNow(datetime):
    @classmethod
    def now(cls, tz=None):
        raise AssertionError("datetime.now() read on the chronological path")

    @classmethod
    def utcnow(cls):
        raise AssertionError("datetime.utcnow() read on the chronological path")


def test_the_chronological_path_never_reads_the_clock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from northstar_api import operations, runtime

    op = _operator(tmp_path, "clock-free")
    for module in (cli_module, operations, runtime):
        monkeypatch.setattr(module, "datetime", _NoNow)
    monkeypatch.setattr(cli_module, "_utc_now", _never_read_the_clock)

    run = _daily(op, final_through=23, go_live=21)

    assert run.code == ExitCode.SUCCESS, run.err
    assert _decisions(op) == [_close(21), _close(22), _close(23)]


def test_the_operated_database_holds_only_nifty(tmp_path: Path) -> None:
    op = _operator(tmp_path, "isolation")
    assert _daily(op, final_through=26, go_live=21).code == ExitCode.SUCCESS

    for table in ("futures_ohlcv", "futures_forward_research_records", "futures_paper_orders"):
        assert op.rows(f"SELECT DISTINCT product_code, exchange_code FROM {table}") == [
            ("NIFTY", "NSE")
        ]
    with closing(sqlite3.connect(op.database)) as connection:
        dump = "\n".join(connection.iterdump())
    for forbidden in ("'ES'", "'CME'", "USD"):
        assert forbidden not in dump


# ---------------------------------------------------------------------------
# Operability details
# ---------------------------------------------------------------------------


def test_sqlite_busy_keeps_state_but_says_retry_later() -> None:
    try:
        try:
            raise sqlite3.OperationalError("database is locked")
        except sqlite3.OperationalError as busy:
            raise FuturesPaperTradingStorageError("Futures paper storage is unavailable.") from busy
    except FuturesPaperTradingStorageError as error:
        code, message = cli_module._classify(error)

    assert code == ExitCode.STATE
    assert "database busy" in message and "retry later" in message


def test_a_real_state_conflict_carries_no_busy_hint() -> None:
    code, message = cli_module._classify(FuturesPaperTradingStorageError("invalid data"))

    assert code == ExitCode.STATE
    assert "busy" not in message


def test_daily_help_documents_the_chronological_settings() -> None:
    out = io.StringIO()
    main(["operations", "daily", "--help"], env={}, stdout=out, stderr=io.StringIO())
    text = out.getvalue()

    for name in (
        "NORTHSTAR_FUTURES_DAILY_BAR_FINALITY",
        "NORTHSTAR_FUTURES_FINAL_THROUGH",
        "NORTHSTAR_FUTURES_GO_LIVE",
        "UPSTOX_ANALYTICS_TOKEN",
    ):
        assert name in text
    assert "operator-approved" in text and "reads no clock" in text


def test_market_bars_were_stamped_by_the_production_path(tmp_path: Path) -> None:
    op = _operator(tmp_path, "bars")
    assert _daily(op, final_through=21, go_live=21).code == ExitCode.SUCCESS

    bars = op.facts()["bars"]
    assert len(bars) == 21
    assert bars[-1].point_in_time.value == _close(21)  # the NSE session close, not a label


# ---------------------------------------------------------------------------
# Disabled finality needs no credential at all
# ---------------------------------------------------------------------------


class _TrackingEnv(dict):
    def __init__(self, *args) -> None:
        super().__init__(*args)
        self.read: list[str] = []

    def get(self, key, default=None):
        self.read.append(key)
        return super().get(key, default)

    def __getitem__(self, key):
        self.read.append(key)
        return super().__getitem__(key)


def _daily_tracked(op: Operator, **settings) -> tuple[Outcome, _TrackingEnv, list]:
    env = _TrackingEnv(_env(op, **settings))
    built: list[object] = []
    out, err = io.StringIO(), io.StringIO()
    code = main(
        ["operations", "daily"],
        env=env,
        stdout=out,
        stderr=err,
        clock=_never_read_the_clock,
        upstox_market_sync_runtime=lambda *args: built.append(args),
    )
    return Outcome(code, out.getvalue(), err.getvalue()), env, built


@pytest.mark.parametrize("finality", [None, "disabled"], ids=["unset", "explicit"])
def test_disabled_finality_neither_requires_nor_reads_the_token(
    tmp_path: Path, finality: str | None
) -> None:
    op = _operator(tmp_path, "disabled-no-token")
    before, requests = op.facts(), len(op.upstox.candle_requests)

    run, env, built = _daily_tracked(op, finality=finality, token=None, go_live=21)

    assert run.code == ExitCode.SUCCESS
    assert "Finality mode: disabled" in run.out
    assert "STATUS: WAITING -- Daily-bar finality is not established" in run.out
    assert "UPSTOX_ANALYTICS_TOKEN" not in env.read
    assert built == []  # the Upstox transport is never composed
    assert _requests_after(op, requests) == []
    assert op.facts() == before


def test_operator_approved_without_a_token_is_configuration(tmp_path: Path) -> None:
    op = _operator(tmp_path, "approved-no-token")
    before, requests = op.facts(), len(op.upstox.candle_requests)

    run, env, built = _daily_tracked(op, final_through=22, go_live=21, token=None)

    assert run.code == ExitCode.CONFIGURATION
    assert "UPSTOX_ANALYTICS_TOKEN is not set; operations daily needs Upstox credentials." in (
        run.err
    )
    assert "UPSTOX_ANALYTICS_TOKEN" in env.read
    assert built == [] and _requests_after(op, requests) == []
    assert op.facts() == before
