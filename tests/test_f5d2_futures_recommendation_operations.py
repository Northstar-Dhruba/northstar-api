"""Acceptance: F-5D-2 private futures recommendation operations, disabled by default.

``northstar operations daily`` with NORTHSTAR_FUTURES_RECOMMENDATIONS=enabled
runs, after the unchanged chronological paper workflow and under the same
operations lock: monitor every open recommendation of the product, re-read
the durable state, refuse on an open anomaly or an unsafe rollover, then
generate with an approved configuration. ``northstar recommendations
expire-incomplete`` applies one explicit operator declaration.

SYNTHETIC throughout: INDIA-7's NIFTY market and range-honouring Upstox
transport double (no network: the module refuses every socket), the real NSE
calendar, real temporary SQLite and the production CLI. The production
registry of approved recommendation configurations is empty -- ADR-018 approves
no production parameters -- so a synthetic evaluation configuration is passed
explicitly to ``main``; nothing falls back to it.

The normal market decides HOLD at bar 21, BUY at 22 and 23, SELL at 24. With
the synthetic configuration (validity 2 sessions) the BUY at 22, issued the
next morning, is a LONG valid for bars 23-24: bar 23 activates it, bar 24's
low takes its stop. The SELL at 24 then becomes a SHORT.
"""

from __future__ import annotations

import dataclasses
import hashlib
import io
import socket
import sqlite3
from contextlib import closing
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import (
    FuturesRecommendationGenerationAudit,
    FuturesResearchHistoryCoverage,
    FuturesTradeRecommendationGenerationProfile,
    FuturesTradeRecommendationGenerationStatus,
    FuturesTradeRecommendationIdentityService,
)
from northstar_application.ports import (
    FuturesDailyBarFinality,
    FuturesDailyBarFinalityOutcome,
    FuturesTradeRecommendationQuery,
)
from northstar_core.derivatives import ExpirationDate, QuoteValue
from northstar_core.foundation.value_objects import PointInTime, Timeframe
from northstar_core.futures import FuturesContract
from northstar_core.recommendations.futures import (
    EntryZone,
    FuturesRecommendationGenerationPolicy,
    FuturesResearchDecisionReference,
    FuturesTradeRecommendation,
    PriceTarget,
    RecommendationPolicyVersion,
    RecommendationStatus,
    RecommendationValidity,
    StopLoss,
    TradeDirection,
)
from northstar_core.strategy import ExplanationReason
from northstar_infrastructure.market_data import NSEFuturesTradingSessionResolver, upstox_http
from northstar_infrastructure.persistence import (
    SQLiteFuturesTradeRecommendationRepository,
    SQLiteFuturesTradeRecommendationStore,
)
from northstar_infrastructure.persistence import (
    sqlite_futures_recommendation_generation_audits as audit_codec,
)
from northstar_infrastructure.persistence import (
    sqlite_futures_trade_recommendations as recommendation_module,
)
from test_india7_nifty_incremental_operations_acceptance import (
    _CONTRACT,
    _LOT,
    _SESSIONS,
    _STRATEGY,
    Operator,
    Outcome,
    _day,
)
from test_india8b_nse_chronological_operations import _Clock, _env, _operator

from northstar_api import futures_recommendation_operations as operations_module
from northstar_api.cli import ExitCode, main
from northstar_api.futures_recommendation_operations import (
    APPROVED_FUTURES_RECOMMENDATION_CONFIGURATIONS,
    FuturesRecommendationConfiguration,
)
from northstar_api.operations_lock import DatabaseOperationsLock
from northstar_api.runtime import (
    FuturesRecommendationRuntime,
    build_futures_recommendation_runtime,
    build_upstox_market_sync_runtime,
)
from northstar_api.settings import (
    CONTRACT_CUTOFF_AUTHORITY_VARIABLE,
    CONTRACT_LAST_SESSION_VARIABLE,
    RECOMMENDATION_PROFILE_VARIABLE,
    RECOMMENDATIONS_VARIABLE,
    load_recommendation_settings,
)

_NOV = FuturesContract(_CONTRACT.product, ExpirationDate("2026-11-24"))
_POLICY = FuturesRecommendationGenerationPolicy(
    version=RecommendationPolicyVersion("f4-synthetic-evaluation-v1"),
    approved_strategies=(_STRATEGY,),
    lookback_changes=19,
    entry_multiplier=Decimal("0.25"),
    stop_multiplier=Decimal("30"),
    target_multiplier=Decimal("50"),
    price_increment=Decimal("0.05"),
    min_reward_to_risk=Decimal("1.5"),
    short_enabled=True,
)
_PROFILE = FuturesTradeRecommendationGenerationProfile(
    "f5d-synthetic-evaluation-v1", 2, _POLICY.version
)
_SYNTHETIC = {_PROFILE.version: FuturesRecommendationConfiguration(_POLICY, _PROFILE)}
_AUTHORITY = "synthetic contract-reference fixture, not an exchange circular"
_RECOMMENDATION_TABLES = (
    "futures_trade_recommendations",
    "futures_recommendation_generation_audits",
    "futures_trade_recommendation_steps",
)


@pytest.fixture(scope="module", autouse=True)
def no_network():
    import databento

    def refuse(*args, **kwargs):
        raise AssertionError("F-5D-2 acceptance must not touch the network")

    patcher = pytest.MonkeyPatch()
    patcher.setattr(socket.socket, "connect", refuse)
    patcher.setattr(socket, "create_connection", refuse)
    patcher.setattr(upstox_http, "urlopen", refuse)
    patcher.setattr(databento, "Historical", refuse)
    yield
    patcher.undo()


# ---------------------------------------------------------------------------
# Operator helpers
# ---------------------------------------------------------------------------


def _morning(bar: int) -> datetime:
    """08:30 IST on the bar's trading date: before it opens, after every earlier close."""
    day = date.fromisoformat(_day(bar))
    return datetime(day.year, day.month, day.day, 3, 0, tzinfo=UTC)


def _manifest(op: Operator, bars=range(1, 27)) -> str:
    """fdc1 entries computed by hand from the transport double's rows, not by Northstar."""
    entries = []
    for bar in bars:
        _label, open_, high, low, close, volume, _oi = op.upstox._row(bar)
        text = (
            "northstar.futures-daily-candle-identity/1\n"
            "product=NIFTY\nexchange=NSE\nexpiration=2026-10-27\n"
            f"trading_date={_day(bar)}\ntimeframe=1d\n"
            f"open={open_}\nhigh={high}\nlow={low}\nclose={close}\n"
            f"volume_contracts={volume // _LOT}\n"
        )
        digest = hashlib.sha256(text.encode("ascii")).hexdigest()
        entries.append(f"2026-10-27/{_day(bar)}=fdc1-sha256:{digest}")
    return ",".join(entries)


def _recommendation_env(**overrides: str | None) -> dict[str, str]:
    env = {
        RECOMMENDATIONS_VARIABLE: "enabled",
        RECOMMENDATION_PROFILE_VARIABLE: _PROFILE.version,
        CONTRACT_LAST_SESSION_VARIABLE: "2026-10-27",
        CONTRACT_CUTOFF_AUTHORITY_VARIABLE: _AUTHORITY,
    }
    for name, value in overrides.items():
        variable = {
            "flag": RECOMMENDATIONS_VARIABLE,
            "profile": RECOMMENDATION_PROFILE_VARIABLE,
            "last_session": CONTRACT_LAST_SESSION_VARIABLE,
            "authority": CONTRACT_CUTOFF_AUTHORITY_VARIABLE,
        }[name]
        if value is None:
            env.pop(variable, None)
        else:
            env[variable] = value
    return env


def _cli(
    op: Operator,
    argv: list[str],
    *,
    clock: datetime,
    final_through: int = 22,
    recommendations: dict[str, str] | None = None,
    configurations=_SYNTHETIC,
    runtime=build_futures_recommendation_runtime,
    manifest: str | None = None,
    **env_overrides,
) -> Outcome:
    env = _env(
        op,
        final_through=final_through,
        go_live=21,
        approvals=_manifest(op) if manifest is None else manifest,
        **env_overrides,
    )
    env.update(_recommendation_env() if recommendations is None else recommendations)
    out, err = io.StringIO(), io.StringIO()
    code = main(
        argv,
        env=env,
        stdout=out,
        stderr=err,
        clock=_Clock(clock),
        upstox_market_sync_runtime=lambda path, token, current_instant=None, **kw: (
            build_upstox_market_sync_runtime(
                path, token, fetch=op.upstox, current_instant=current_instant, **kw
            )
        ),
        recommendation_configurations=configurations,
        recommendation_runtime=runtime,
    )
    return Outcome(code, out.getvalue(), err.getvalue())


def _daily(op: Operator, *, clock: datetime, **kwargs) -> Outcome:
    return _cli(op, ["operations", "daily"], clock=clock, **kwargs)


def _declare(op: Operator, identity: str, *, clock: datetime, **kwargs) -> Outcome:
    extra = kwargs.pop("argv", None)
    argv = extra or [
        "recommendations", "expire-incomplete", "--recommendation", identity,
        "--operator", "synthetic-owner", "--case", "CASE-0001",
        "--monitor-policy", "f5c-monitor-v1",
    ]  # fmt: skip
    return _cli(op, argv, clock=clock, **kwargs)


def _tables(op: Operator) -> set[str]:
    with closing(sqlite3.connect(op.database)) as connection:
        return {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }


def _rows(op: Operator, table: str) -> list[tuple]:
    if table not in _tables(op):
        return []
    return op.rows(f"SELECT * FROM {table} ORDER BY 1, 2")  # noqa: S608


def _snapshot(op: Operator) -> dict[str, list[tuple]]:
    return {table: _rows(op, table) for table in sorted(_tables(op))}


def _lifecycles(op: Operator):
    return SQLiteFuturesTradeRecommendationRepository(op.database).get_lifecycles(
        FuturesTradeRecommendationQuery(_CONTRACT.product)
    )


def _outcomes(op: Operator) -> list[str]:
    return sorted(row[12] for row in _rows(op, "futures_recommendation_generation_audits"))


def _first_recommendation(op: Operator) -> FuturesTradeRecommendation:
    """Run the day after bar 22: paper 21-22, then generate the LONG from bar 22's BUY."""
    run = _daily(op, clock=_morning(23), final_through=22)
    assert run.code == ExitCode.SUCCESS, run.err
    (lifecycle,) = _lifecycles(op)
    return lifecycle.recommendation


def _audited_recommendation(
    contract: FuturesContract,
    *,
    decided: datetime,
    eligible: PointInTime,
    through: PointInTime,
    cutoff: PointInTime,
) -> tuple[FuturesTradeRecommendation, FuturesRecommendationGenerationAudit]:
    """A canonical recommendation and the RECORDED audit of the attempt that made it."""
    decision = PointInTime(decided.isoformat())
    source = FuturesResearchDecisionReference(contract, Timeframe("1d"), decision, _STRATEGY)
    issued = PointInTime(decided.replace(hour=12).isoformat())
    recommendation = FuturesTradeRecommendation(
        identity=FuturesTradeRecommendationIdentityService().identity(source, _PROFILE),
        contract=contract,
        direction=TradeDirection.LONG,
        entry=EntryZone(QuoteValue(Decimal("25000")), QuoteValue(Decimal("25010"))),
        stop=StopLoss(QuoteValue(Decimal("24800"))),
        target=PriceTarget(QuoteValue(Decimal("25400"))),
        validity=RecommendationValidity(eligible, through, cutoff),
        source=source,
        policy_version=_POLICY.version,
        issued_at=issued,
        thesis=(ExplanationReason("Synthetic fixture thesis"),),
    )
    session = NSEFuturesTradingSessionResolver().resolve(_CONTRACT.product, decided.date())
    audit = FuturesRecommendationGenerationAudit(
        contract=contract,
        strategy=_STRATEGY,
        policy_version=_POLICY.version,
        profile_version=_PROFILE.version,
        attempted_at=issued,
        status=FuturesTradeRecommendationGenerationStatus.COMPLETED,
        session=session,
        finality=FuturesDailyBarFinality(
            contract, session, FuturesDailyBarFinalityOutcome.FINAL, "synthetic"
        ),
        history_coverage=FuturesResearchHistoryCoverage.VERIFIED,
        proposed=(recommendation.identity,),
        recorded=(recommendation.identity,),
    )
    return recommendation, audit


def _close_of(day: str) -> PointInTime:
    session = NSEFuturesTradingSessionResolver().resolve(_CONTRACT.product, date.fromisoformat(day))
    return session.closes_at


def _record_directly(op: Operator, recommendation, audit) -> None:
    """Insert a canonical definition and its RECORDED audit in one transaction, bypassing P-1."""
    configuration = recommendation_module._configuration_row(_POLICY, _PROFILE)
    row = recommendation_module._definition_row(
        recommendation, _PROFILE.version, recommendation_module._configuration_digest(configuration)
    )
    with closing(sqlite3.connect(op.database)) as connection:
        recommendation_module.initialize_futures_trade_recommendation_schema(connection)
        if not connection.execute(
            "SELECT 1 FROM futures_recommendation_configurations WHERE profile_version = ?",
            (_PROFILE.version,),
        ).fetchone():
            connection.execute(recommendation_module._INSERT_CONFIGURATION, configuration)
        connection.execute(recommendation_module._INSERT_DEFINITION, row)
        audit_codec.insert_audit(connection, audit)
        connection.commit()


# ---------------------------------------------------------------------------
# 1-3. Disabled by default
# ---------------------------------------------------------------------------


def _normalised(outcome: Outcome, op: Operator) -> tuple[int, str]:
    return outcome.code, outcome.out.replace(str(op.database), "<db>")


def test_unset_and_disabled_are_the_existing_operation_exactly(tmp_path: Path) -> None:
    unset, disabled, plain = (_operator(tmp_path, name) for name in ("unset", "disabled", "plain"))

    a = _daily(unset, clock=_morning(23), recommendations={})
    b = _daily(disabled, clock=_morning(23), recommendations={RECOMMENDATIONS_VARIABLE: "disabled"})
    c = _daily(plain, clock=_morning(23), recommendations={}, configurations={})

    assert a.code == b.code == c.code == ExitCode.SUCCESS
    assert _normalised(a, unset)[1].replace("unset", "x") == _normalised(b, disabled)[1].replace(
        "disabled", "x"
    )
    assert "RECOMMENDATIONS" not in a.out + b.out + c.out
    for op in (unset, disabled, plain):
        assert not set(_RECOMMENDATION_TABLES) & _tables(op)


def test_disabled_ignores_every_other_recommendation_setting(tmp_path: Path) -> None:
    op = _operator(tmp_path, "disabled-extra")

    outcome = _daily(
        op,
        clock=_morning(23),
        recommendations=_recommendation_env(flag="disabled", profile="not-approved"),
        configurations=APPROVED_FUTURES_RECOMMENDATION_CONFIGURATIONS,
    )

    assert outcome.code == ExitCode.SUCCESS and "RECOMMENDATIONS" not in outcome.out
    assert not set(_RECOMMENDATION_TABLES) & _tables(op)


@pytest.mark.parametrize("value", ["on", "true", "ENABLED-ish"])
def test_an_invalid_flag_is_a_configuration_error_before_anything_runs(
    tmp_path: Path, value: str
) -> None:
    op = _operator(tmp_path, f"invalid-{value}")
    before = _snapshot(op)

    outcome = _daily(op, clock=_morning(23), recommendations=_recommendation_env(flag=value))

    assert outcome.code == ExitCode.CONFIGURATION
    assert RECOMMENDATIONS_VARIABLE in outcome.err
    assert _snapshot(op) == before


# ---------------------------------------------------------------------------
# 4-7, 22. Enabled: monitor, then generate; P-1; release; real NSE closes
# ---------------------------------------------------------------------------


def test_the_enabled_lifecycle_from_generation_to_release(tmp_path: Path) -> None:
    op = _operator(tmp_path, "enabled")

    # Day 1: paper 21-22, nothing to monitor, generate the LONG from bar 22's BUY.
    first = _daily(op, clock=_morning(23), final_through=22)
    assert first.code == ExitCode.SUCCESS, first.err
    assert "STATUS: COMPLETED -- 2 session(s) processed" in first.out
    assert "Monitored: 0 open recommendation(s), 0 step(s) recorded" in first.out
    (lifecycle,) = _lifecycles(op)
    long = lifecycle.recommendation
    assert long.direction is TradeDirection.LONG and lifecycle.status.value == "WAITING_FOR_ENTRY"
    assert f"  recorded {long.identity.identity} LONG" in first.out
    assert _outcomes(op) == ["RECORDED"]
    # The validity window is the NSE calendar's: closes at 10:10Z, never assumed.
    assert long.validity.eligible_from == _SESSIONS[22].closes_at
    assert long.validity.valid_through == _SESSIONS[23].closes_at
    assert not long.validity.eligible_from.value.endswith("T10:00:00Z")
    assert long.validity.contract_cutoff == _close_of("2026-10-27")
    definition = _rows(op, "futures_trade_recommendations")

    # Day 2: bar 23 activates it; bar 23's BUY is refused while it is open (P-1).
    second = _daily(op, clock=_morning(24), final_through=23)
    assert second.code == ExitCode.SUCCESS, second.err
    assert "WAITING_FOR_ENTRY -> ACTIVE, 1 step(s) recorded" in second.out
    assert "generation rejected: DUPLICATE_OPEN_RECOMMENDATION" in second.out
    (lifecycle,) = _lifecycles(op)
    assert lifecycle.status is RecommendationStatus.ACTIVE
    assert _outcomes(op) == ["RECORDED", "REJECTED"]

    # Day 3: bar 24 takes the stop; the product is released; bar 24's SELL is a SHORT.
    third = _daily(op, clock=_morning(25), final_through=24)
    assert third.code == ExitCode.SUCCESS, third.err
    assert "ACTIVE -> CLOSED_STOP, 1 step(s) recorded" in third.out
    lifecycles = {lc.recommendation.identity: lc for lc in _lifecycles(op)}
    assert lifecycles[long.identity].status is RecommendationStatus.CLOSED_STOP
    (short,) = [
        lc for lc in lifecycles.values() if lc.recommendation is not long and not lc.is_terminal
    ]
    assert short.recommendation.direction is TradeDirection.SHORT
    assert _outcomes(op) == ["RECORDED", "RECORDED", "REJECTED"]
    assert definition[0] in _rows(op, "futures_trade_recommendations")  # never rewritten


def test_a_same_day_rerun_is_idempotent(tmp_path: Path) -> None:
    op = _operator(tmp_path, "rerun")
    _first_recommendation(op)

    retry = _daily(op, clock=_morning(23), final_through=22)
    assert retry.code == ExitCode.SUCCESS and "STATUS: WAITING" in retry.out
    assert "  existing " in retry.out
    after_retry = _snapshot(op)
    again = _daily(op, clock=_morning(23), final_through=22)

    assert again.code == ExitCode.SUCCESS
    assert _snapshot(op) == after_retry
    assert len(_rows(op, "futures_trade_recommendations")) == 1
    assert _outcomes(op) == ["EXISTING_RETURNED", "RECORDED"]


def test_the_waiting_path_still_monitors_an_open_recommendation(tmp_path: Path) -> None:
    op = _operator(tmp_path, "waiting")
    long = _first_recommendation(op)
    # Bar 23 is decided with recommendations disabled, so nothing monitors it then.
    paper = _daily(op, clock=_morning(24), final_through=23, recommendations={})
    assert paper.code == ExitCode.SUCCESS and "RECOMMENDATIONS" not in paper.out

    waiting = _daily(op, clock=_morning(24), final_through=23)

    assert waiting.code == ExitCode.SUCCESS
    assert "STATUS: WAITING" in waiting.out
    assert f"  {long.identity.identity} {_CONTRACT}: WAITING_FOR_ENTRY -> ACTIVE" in waiting.out
    (lifecycle,) = _lifecycles(op)
    assert lifecycle.status is RecommendationStatus.ACTIVE


# ---------------------------------------------------------------------------
# 9-10. Review and finality evidence
# ---------------------------------------------------------------------------


def test_a_missing_manifest_refuses_before_anything_runs(tmp_path: Path) -> None:
    op = _operator(tmp_path, "no-manifest")
    before = _snapshot(op)
    env = _env(op, final_through=22, go_live=21, approvals="")
    del env["NORTHSTAR_FUTURES_APPROVED_CANDLES"]
    env.update(_recommendation_env())

    code = main(["operations", "daily"], env=env, stdout=io.StringIO(), stderr=io.StringIO(),
                clock=_Clock(_morning(23)), recommendation_configurations=_SYNTHETIC)  # fmt: skip

    assert code == ExitCode.CONFIGURATION
    assert _snapshot(op) == before


def test_unreviewed_history_never_produces_a_recommendation(tmp_path: Path) -> None:
    op = _operator(tmp_path, "unreviewed-history")

    outcome = _daily(op, clock=_morning(23), final_through=22, manifest=_manifest(op, (21, 22)))

    assert outcome.code == ExitCode.SUCCESS, outcome.err
    assert "Generation: BLOCKED (RESEARCH_HISTORY_NOT_VERIFIED)" in outcome.out
    assert _rows(op, "futures_trade_recommendations") == []
    assert _outcomes(op) == ["BLOCKED"]


@pytest.mark.parametrize("finality", [None, "operator-approved"])
def test_enabled_requires_operator_reviewed_finality(tmp_path: Path, finality) -> None:
    op = _operator(tmp_path, f"finality-{finality}")
    before = _snapshot(op)

    outcome = _daily(op, clock=_morning(23), finality=finality)

    assert outcome.code == ExitCode.CONFIGURATION
    assert _snapshot(op) == before


def test_a_session_not_yet_final_is_waited_on_not_evaluated(tmp_path: Path) -> None:
    op = _operator(tmp_path, "not-final")
    long = _first_recommendation(op)

    outcome = _daily(op, clock=_morning(24), final_through=22)

    assert outcome.code == ExitCode.SUCCESS
    assert f"{long.identity.identity} {_CONTRACT}: WAITING_FOR_ENTRY -> WAITING_FOR_ENTRY" in (
        outcome.out
    )
    assert "SESSION_NOT_FINAL [SESSION_NOT_FINAL]" in outcome.out
    assert _rows(op, "futures_trade_recommendation_steps") == []


# ---------------------------------------------------------------------------
# 11-13. Integrity, anomaly and rollover guards
# ---------------------------------------------------------------------------


def test_a_definition_without_its_audit_fails_closed(tmp_path: Path) -> None:
    op = _operator(tmp_path, "orphan")
    _first_recommendation(op)
    with closing(sqlite3.connect(op.database)) as connection:
        connection.execute("DELETE FROM futures_recommendation_generation_audit_recommendations")
        connection.commit()
    before = _rows(op, "futures_trade_recommendation_steps")

    outcome = _daily(op, clock=_morning(24), final_through=23)

    assert outcome.code == ExitCode.STATE
    assert "FuturesTradeRecommendationStorageError" in outcome.err
    assert _rows(op, "futures_trade_recommendation_steps") == before


def test_an_open_anomaly_is_monitored_but_blocks_generation(tmp_path: Path) -> None:
    op = _operator(tmp_path, "anomaly")
    _first_recommendation(op)
    november, audit = _audited_recommendation(
        _NOV,
        decided=datetime(2026, 8, 27, 10, 10, tzinfo=UTC),
        eligible=_close_of("2026-11-02"),
        through=_close_of("2026-11-04"),
        cutoff=_close_of("2026-11-23"),
    )
    _record_directly(op, november, audit)
    definitions = _rows(op, "futures_trade_recommendations")

    outcome = _daily(op, clock=_morning(24), final_through=23)

    assert outcome.code == ExitCode.STATE
    assert "OPEN ANOMALY" in outcome.out and "Generation: not run" in outcome.out
    assert "Monitored: 2 open recommendation(s)" in outcome.out
    assert "WAITING_FOR_ENTRY -> ACTIVE" in outcome.out  # monitoring still progressed
    assert _rows(op, "futures_trade_recommendations") == definitions
    assert _outcomes(op) == ["RECORDED", "RECORDED"]  # no generation attempt this run


def test_an_open_recommendation_on_another_expiry_blocks_generation(tmp_path: Path) -> None:
    op = _operator(tmp_path, "rollover")
    november, audit = _audited_recommendation(
        _NOV,
        decided=datetime(2026, 8, 27, 10, 10, tzinfo=UTC),
        eligible=_close_of("2026-11-02"),
        through=_close_of("2026-11-04"),
        cutoff=_close_of("2026-11-23"),
    )
    SQLiteFuturesTradeRecommendationStore(op.database).record(
        (november,), policy=_POLICY, profile=_PROFILE, audit=audit
    )

    outcome = _daily(op, clock=_morning(23), final_through=22)

    assert outcome.code == ExitCode.STATE
    assert "UNSAFE ROLLOVER" in outcome.out and "Unsafe rollover" in outcome.err
    assert str(_NOV) in outcome.out
    assert [lc.recommendation for lc in _lifecycles(op)] == [november]  # nothing expired


# ---------------------------------------------------------------------------
# 14-15. Explicit incomplete-coverage declaration
# ---------------------------------------------------------------------------


def test_a_declaration_expires_a_recommendation_held_past_validity(tmp_path: Path) -> None:
    op = _operator(tmp_path, "declare")
    long = _first_recommendation(op)

    # Bars 23-24 never become final, and validity has ended by bar 26's morning.
    held = _daily(op, clock=_morning(26), final_through=22)
    assert held.code == ExitCode.SUCCESS
    assert "AWAITING_EVIDENCE_PAST_VALIDITY [SESSION_NOT_FINAL]" in held.out
    assert "recommendations expire-incomplete" in held.out
    (lifecycle,) = _lifecycles(op)
    assert not lifecycle.is_terminal

    declared = _declare(op, long.identity.identity, clock=_morning(26))

    assert declared.code == ExitCode.SUCCESS, declared.err
    assert "Status: EXPIRED_WITH_INCOMPLETE_COVERAGE" in declared.out
    (lifecycle,) = _lifecycles(op)
    assert lifecycle.status is RecommendationStatus.EXPIRED_WITH_INCOMPLETE_COVERAGE
    evidence = lifecycle.events[-1].time_evidence
    assert evidence.evaluated_through is None
    assert evidence.attested_by == (
        "northstar.futures-incomplete-coverage/1;monitor_policy=f5c-monitor-v1;"
        f"operator=synthetic-owner;case=CASE-0001;recommendation={long.identity.identity}"
    )
    # Released: the next run may generate again, from evidence alone.
    assert _daily(op, clock=_morning(26), final_through=22).code == ExitCode.SUCCESS


@pytest.mark.parametrize(
    "case, code",
    [
        ("unknown", ExitCode.INPUT),
        ("blank-operator", ExitCode.INPUT),
        ("not-past-validity", ExitCode.STATE),
        ("disabled", ExitCode.CONFIGURATION),
    ],
)
def test_an_invalid_or_inapplicable_declaration_is_refused(
    tmp_path: Path, case: str, code: ExitCode
) -> None:
    op = _operator(tmp_path, f"declare-{case}")
    long = _first_recommendation(op)
    identity = "f" * 64 if case == "unknown" else long.identity.identity
    kwargs: dict = {"clock": _morning(26) if case != "not-past-validity" else _morning(24)}
    if case == "blank-operator":
        kwargs["argv"] = [
            "recommendations", "expire-incomplete", "--recommendation", identity,
            "--operator", " ", "--case", "CASE-1", "--monitor-policy", "v1",
        ]  # fmt: skip
    if case == "disabled":
        kwargs["recommendations"] = {}
    before = _snapshot(op)

    outcome = _declare(op, identity, **kwargs)

    assert outcome.code == code, (outcome.code, outcome.err)
    (lifecycle,) = _lifecycles(op)
    assert not lifecycle.is_terminal
    if case != "not-past-validity":
        assert _snapshot(op) == before


def test_a_declaration_cannot_touch_a_terminal_recommendation(tmp_path: Path) -> None:
    op = _operator(tmp_path, "declare-terminal")
    long = _first_recommendation(op)
    assert _daily(op, clock=_morning(25), final_through=24).code == ExitCode.SUCCESS
    before = _snapshot(op)

    outcome = _declare(op, long.identity.identity, clock=_morning(26), final_through=24)

    assert outcome.code == ExitCode.INPUT
    assert _snapshot(op) == before


# ---------------------------------------------------------------------------
# 16. Calendar coverage
# ---------------------------------------------------------------------------


def test_a_window_in_an_unloaded_calendar_year_fails_closed(tmp_path: Path) -> None:
    op = _operator(tmp_path, "calendar-2027")
    beyond, audit = _audited_recommendation(
        _CONTRACT,
        decided=datetime(2026, 8, 27, 10, 10, tzinfo=UTC),
        eligible=PointInTime("2026-12-31T10:10:00Z"),
        through=PointInTime("2027-01-05T10:10:00Z"),
        cutoff=PointInTime("2027-01-28T10:10:00Z"),
    )
    SQLiteFuturesTradeRecommendationStore(op.database).record(
        (beyond,), policy=_POLICY, profile=_PROFILE, audit=audit
    )

    outcome = _daily(op, clock=_morning(23), final_through=22)

    assert outcome.code == ExitCode.PROVIDER  # the existing calendar classification
    assert "FuturesSessionResolutionError" in outcome.err
    assert _rows(op, "futures_trade_recommendation_steps") == []


# ---------------------------------------------------------------------------
# 18-19. Locking and a busy database
# ---------------------------------------------------------------------------


def test_lock_contention_is_the_existing_skip(tmp_path: Path) -> None:
    op = _operator(tmp_path, "locked")
    before = _snapshot(op)

    with DatabaseOperationsLock(op.database):
        outcome = _daily(op, clock=_morning(23))
        declared = _declare(op, "a" * 64, clock=_morning(23))

    assert outcome.code == ExitCode.SUCCESS and "DAILY OPERATION: SKIPPED" in outcome.out
    assert declared.code == ExitCode.STATE
    assert _snapshot(op) == before


def test_a_busy_database_leaves_no_partial_recommendation(tmp_path: Path, monkeypatch) -> None:
    op = _operator(tmp_path, "busy")
    monkeypatch.setattr(recommendation_module, "_BUSY_TIMEOUT_SECONDS", 0.2)

    class BusyStore(SQLiteFuturesTradeRecommendationStore):
        def record(self, *args, **kwargs):
            holder = sqlite3.connect(op.database, isolation_level=None)
            holder.execute("BEGIN IMMEDIATE")
            try:
                return super().record(*args, **kwargs)
            finally:
                holder.rollback()
                holder.close()

    def runtime(path: Path) -> FuturesRecommendationRuntime:
        return dataclasses.replace(
            build_futures_recommendation_runtime(path), store=BusyStore(path)
        )

    outcome = _daily(op, clock=_morning(23), final_through=22, runtime=runtime)

    assert outcome.code == ExitCode.STATE
    assert "database busy" in outcome.err
    assert _rows(op, "futures_trade_recommendations") == []
    assert _outcomes(op) == []


# ---------------------------------------------------------------------------
# 20-21. Never-guessed cutoff and never-defaulted policy
# ---------------------------------------------------------------------------


def test_without_a_configured_cutoff_generation_is_blocked(tmp_path: Path) -> None:
    op = _operator(tmp_path, "no-cutoff")

    outcome = _daily(
        op,
        clock=_morning(23),
        recommendations=_recommendation_env(last_session=None, authority=None),
    )

    assert outcome.code == ExitCode.SUCCESS, outcome.err
    assert "Generation: BLOCKED (CONTRACT_CUTOFF_UNAVAILABLE)" in outcome.out
    assert _rows(op, "futures_trade_recommendations") == []


@pytest.mark.parametrize(
    "overrides",
    [
        {"last_session": "2026-10-25"},  # a Sunday
        {"last_session": "2026-10-28"},  # after the expiration date
        {"authority": None},
        {"last_session": None},
        {"last_session": "27-10-2026"},
    ],
    ids=["non-session", "after-expiry", "no-authority", "no-date", "malformed"],
)
def test_a_contradictory_cutoff_configuration_is_refused(tmp_path: Path, overrides) -> None:
    op = _operator(tmp_path, "bad-cutoff")
    before = _snapshot(op)

    outcome = _daily(op, clock=_morning(23), recommendations=_recommendation_env(**overrides))

    assert outcome.code == ExitCode.CONFIGURATION
    assert _snapshot(op) == before


def test_the_cutoff_authority_is_labelled_as_the_operators_statement() -> None:

    source = operations_module.operator_cutoff_source(
        NSEFuturesTradingSessionResolver(),
        _CONTRACT,
        load_recommendation_settings(_recommendation_env()),
    )
    cutoff = source.resolve(_CONTRACT)

    assert cutoff.authority == f"operator-configured: {_AUTHORITY}"
    assert cutoff.last_session.closes_at == _close_of("2026-10-27")
    assert source.resolve(_NOV) is None


@pytest.mark.parametrize(
    "configurations, profile",
    [
        (APPROVED_FUTURES_RECOMMENDATION_CONFIGURATIONS, _PROFILE.version),
        (_SYNTHETIC, "f5d-unknown-profile"),
        (_SYNTHETIC, None),
    ],
    ids=["production-registry", "unknown-profile", "no-profile"],
)
def test_without_an_approved_configuration_nothing_runs(
    tmp_path: Path, configurations, profile
) -> None:
    op = _operator(tmp_path, "no-policy")
    before = _snapshot(op)

    outcome = _daily(
        op,
        clock=_morning(23),
        recommendations=_recommendation_env(profile=profile),
        configurations=configurations,
    )

    assert outcome.code == ExitCode.CONFIGURATION
    assert _snapshot(op) == before
    assert not set(_RECOMMENDATION_TABLES) & _tables(op)


def test_the_production_registry_approves_nothing() -> None:
    assert dict(APPROVED_FUTURES_RECOMMENDATION_CONFIGURATIONS) == {}


def test_a_configuration_for_another_strategy_is_refused(tmp_path: Path) -> None:
    op = _operator(tmp_path, "other-strategy")
    from northstar_core.strategy import StrategyIdentity

    other = FuturesRecommendationGenerationPolicy(
        **{
            **{f: getattr(_POLICY, f) for f in _POLICY.__dataclass_fields__},
            "approved_strategies": (StrategyIdentity("synthetic-other-v1"),),
        }
    )
    configurations = {_PROFILE.version: FuturesRecommendationConfiguration(other, _PROFILE)}

    outcome = _daily(op, clock=_morning(23), configurations=configurations)

    assert outcome.code == ExitCode.CONFIGURATION


# ---------------------------------------------------------------------------
# 23-26. Boundaries
# ---------------------------------------------------------------------------


def test_enabled_with_databento_is_refused(tmp_path: Path) -> None:
    op = _operator(tmp_path, "databento")
    before = _snapshot(op)

    outcome = _daily(op, clock=_morning(23), provider="databento", exchange="CME")

    assert outcome.code == ExitCode.CONFIGURATION
    assert _snapshot(op) == before


def test_recommendation_operations_reach_no_provider_publication_or_broker() -> None:
    import ast

    source = Path(operations_module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = {
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    for name in imported:
        for fragment in ("upstox", "databento", "broker", "paper_trading", "requests", "http",
                         "smtp", "socket"):  # fmt: skip
            assert fragment not in name, name


def test_enabled_recommendations_add_no_provider_request(tmp_path: Path) -> None:
    plain, enabled = _operator(tmp_path, "requests-plain"), _operator(tmp_path, "requests-on")

    _daily(plain, clock=_morning(23), recommendations={})
    _daily(enabled, clock=_morning(23))

    assert plain.upstox.candle_requests == enabled.upstox.candle_requests
    assert plain.upstox.current_day_requests == enabled.upstox.current_day_requests
