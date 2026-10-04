"""Tests for per-contract runtime wiring of the NSE pre-expiry flatten guard.

The DatabaseRuntime stays generic: one runtime serves every contract in its
database, and ``paper_session_for(contract)`` picks the session by the
contract's own exchange. NSE gets the session guarded by the real
NSEFuturesTradingSessionResolver with K = 5; every other venue keeps the
unguarded session, exactly as before.

The October end-to-end runs real ``paper run`` commands through the production
runtime on temporary SQLite, on real NSE sessions. NIFTY October 2026 expires
on Tuesday 2026-10-27 (E); Tuesday 2026-10-20 is an NSE holiday, so:

    E-8 Wed 10-14   a steady rise decides BUY -> long 2
    E-7 Thu 10-15   not yet protected
    E-6 Fri 10-16   expiry flatten: SELL 2, filled at the E-5 open
    E-5 Mon 10-19 .. E Tue 10-27   no reopening

No provider is contacted and no credential is needed.
"""

from __future__ import annotations

import dataclasses
import inspect
import io
import sqlite3
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import (
    FuturesExpiryWindowError,
    FuturesPaperExecutionIdentityService,
)
from northstar_application.ports import (
    FuturesForwardResearchRecordQuery,
    FuturesSessionResolutionError,
)
from northstar_core.derivatives import ExpirationDate, QuoteValue
from northstar_core.foundation.value_objects import (
    ExchangeCode,
    PointInTime,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.futures import FuturesContract, FuturesOHLCVBar, FuturesProductReference
from northstar_core.paper_trading import PaperPortfolioIdentity
from northstar_infrastructure.market_data import (
    ExchangeCalendarFuturesTradingSessionResolver,
    NSEFuturesTradingSessionResolver,
    SQLiteFuturesHistoricalMarketDataStore,
)

from northstar_api import cli, runtime
from northstar_api.cli import ExitCode, main
from northstar_api.runtime import (
    NSE_EXPIRY_FLATTEN_SESSIONS,
    DatabaseRuntime,
    build_database_runtime,
)
from northstar_api.settings import MARKET_DATA_PROVIDER_VARIABLE

_NSE = ExchangeCode("NSE")
_NIFTY = FuturesProductReference(Symbol("NIFTY"), _NSE)
_NIFTY_OCT = FuturesContract(_NIFTY, ExpirationDate("2026-10-27"))
_NIFTY_NOV = FuturesContract(_NIFTY, ExpirationDate("2026-11-23"))
_ES = FuturesProductReference(Symbol("ES"), ExchangeCode("CME"))
_ES_DEC = FuturesContract(_ES, ExpirationDate("2026-12-18"))
_DAILY = Timeframe("1d")

_E = date(2026, 10, 27)
_E1 = date(2026, 10, 26)
_E2 = date(2026, 10, 23)
_E3 = date(2026, 10, 22)
_E4 = date(2026, 10, 21)
_E5 = date(2026, 10, 19)
_E6 = date(2026, 10, 16)
_E7 = date(2026, 10, 15)
_E8 = date(2026, 10, 14)

_STRATEGY = "alpha"
_PORTFOLIO = "nifty-paper-alpha"

# Real NSE sessions, ending well before the un-notified 2026-11-08 Muhurat session.
_NSE_SESSIONS = NSEFuturesTradingSessionResolver().sessions_in_range(
    _NIFTY, date(2026, 8, 3), date(2026, 11, 6)
)
_CME_SESSIONS = ExchangeCalendarFuturesTradingSessionResolver().sessions_in_range(
    _ES, date(2026, 6, 1), date(2026, 7, 31)
)


def _nse_close(day: date) -> str:
    return next(s for s in _NSE_SESSIONS if s.trading_date == day).closes_at.value


@dataclass(frozen=True)
class Outcome:
    code: int
    out: str
    err: str

    @property
    def lines(self) -> list[str]:
        return self.out.splitlines()


def _cli(argv: list[str], *, env: dict | None = None, **kwargs) -> Outcome:
    out, err = io.StringIO(), io.StringIO()
    code = main(argv, env={} if env is None else env, stdout=out, stderr=err, **kwargs)
    return Outcome(code, out.getvalue(), err.getvalue())


def _section(outcome: Outcome, title: str) -> list[str]:
    lines = outcome.lines
    start = lines.index(title) + 1
    end = next((i for i in range(start, len(lines)) if lines[i] == ""), len(lines))
    return lines[start:end]


def _bar(contract: FuturesContract, instant: str, close: Decimal) -> FuturesOHLCVBar:
    # The open differs from the close so a fill's price shows which bar it used.
    return FuturesOHLCVBar(
        contract=contract,
        point_in_time=PointInTime(instant),
        timeframe=_DAILY,
        open=QuoteValue(close - 1),
        high=QuoteValue(close + 2),
        low=QuoteValue(close - 2),
        close=QuoteValue(close),
        volume=Quantity(Decimal("5000")),
    )


def _seed_rise(database: Path, contract: FuturesContract, sessions, base: int) -> None:
    """Fifteen flat closes, then a steady rise: the built-in strategy decides BUY."""
    closes = [Decimal(base)] * 15 + [
        Decimal(base + 1 + index) for index in range(len(sessions) - 15)
    ]
    build_database_runtime(database)
    SQLiteFuturesHistoricalMarketDataStore(database).store(
        tuple(
            _bar(contract, session.closes_at.value, close)
            for session, close in zip(sessions, closes, strict=True)
        )
    )


def _nse_window(first: date, last: date):
    return tuple(s for s in _NSE_SESSIONS if first <= s.trading_date <= last)


def _rows(database: Path, table: str) -> int:
    with sqlite3.connect(database) as connection:
        return connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _economics(database: Path, contract: FuturesContract, point_value: str, currency: str):
    product = contract.product
    return _cli(
        [
            "economics", "set", "--database", str(database),
            "--product", product.product_code.value, "--exchange", product.exchange_code.value,
            "--expiration", contract.expiration_date.value,
            "--point-value", point_value, "--currency", currency,
        ]
    )  # fmt: skip


def _paper_run(
    database: Path, contract: FuturesContract, as_of: str, portfolio: str = _PORTFOLIO, **kwargs
) -> Outcome:
    product = contract.product
    return _cli(
        [
            "paper", "run", "--database", str(database),
            "--product", product.product_code.value, "--exchange", product.exchange_code.value,
            "--expiration", contract.expiration_date.value,
            "--strategy", _STRATEGY, "--portfolio", portfolio, "--target", "2",
            "--as-of", as_of,
        ],
        **kwargs,
    )  # fmt: skip


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "northstar.sqlite3"


def _guard_of(session):
    return session._run._decide._expiry_guard


# ---------------------------------------------------------------------------
# 1-7. Runtime selection by the contract's own exchange
# ---------------------------------------------------------------------------


def test_the_database_runtime_stays_generic(database: Path) -> None:
    assert list(inspect.signature(build_database_runtime).parameters) == ["path"]
    rt = build_database_runtime(database)

    assert isinstance(rt, DatabaseRuntime)
    assert _guard_of(rt.paper_session) is None
    assert set(rt.expiry_guarded_paper_sessions) == {"NSE"}


def test_a_runtime_built_without_guarded_sessions_still_works(database: Path) -> None:
    rt = build_database_runtime(database)
    bare = dataclasses.replace(rt, expiry_guarded_paper_sessions={})

    assert bare.paper_session_for(_NIFTY_OCT) is rt.paper_session


@pytest.mark.parametrize("product", ["NIFTY", "BANKNIFTY"])
def test_an_nse_contract_selects_the_guarded_session(database: Path, product: str) -> None:
    rt = build_database_runtime(database)
    contract = FuturesContract(
        FuturesProductReference(Symbol(product), _NSE), ExpirationDate("2026-10-27")
    )

    session = rt.paper_session_for(contract)
    guard = _guard_of(session)

    assert session is not rt.paper_session
    assert isinstance(guard._resolver, NSEFuturesTradingSessionResolver)
    assert guard.policy.sessions_before_expiry == 5 == NSE_EXPIRY_FLATTEN_SESSIONS


@pytest.mark.parametrize("venue", ["CME", "CBOT", "NYMEX", "COMEX", "EUREX", "BSE", "MCX"])
def test_every_other_venue_keeps_the_exact_unguarded_session(database: Path, venue: str) -> None:
    rt = build_database_runtime(database)
    contract = FuturesContract(
        FuturesProductReference(Symbol("ES"), ExchangeCode(venue)), ExpirationDate("2026-12-18")
    )

    session = rt.paper_session_for(contract)

    assert session is rt.paper_session
    assert _guard_of(session) is None


def test_guarded_and_unguarded_sessions_share_the_same_ports(database: Path) -> None:
    rt = build_database_runtime(database)
    guarded = rt.paper_session_for(_NIFTY_OCT)._run._decide
    plain = rt.paper_session._run._decide

    assert guarded._forward_repository is plain._forward_repository
    assert guarded._order_repository is plain._order_repository is rt.order_repository


def test_the_nse_guard_fails_closed_for_a_cme_contract(database: Path) -> None:
    guard = _guard_of(build_database_runtime(database).paper_session_for(_NIFTY_OCT))

    with pytest.raises(FuturesSessionResolutionError, match="Unsupported futures venue: CME"):
        guard.assess(_ES_DEC, PointInTime(_CME_SESSIONS[24].closes_at.value))


def test_selection_rejects_a_non_contract(database: Path) -> None:
    with pytest.raises(TypeError, match="FuturesContract"):
        build_database_runtime(database).paper_session_for(_NIFTY)  # type: ignore[arg-type]


def test_selection_consults_only_the_contract_exchange() -> None:
    source = inspect.getsource(DatabaseRuntime.paper_session_for).lower()

    assert "exchange_code" in source
    for forbidden in ("provider", "upstox", "databento", "token", "api_key", "env"):
        assert forbidden not in source


# ---------------------------------------------------------------------------
# 7-8, 16-18. CLI routing, provider independence, no K configuration
# ---------------------------------------------------------------------------


class StopSession(Exception):
    pass


class SpySession:
    def __init__(self, name: str, calls: list) -> None:
        self.name, self.calls = name, calls

    def execute(self, contract, *args):
        self.calls.append((self.name, contract))
        raise StopSession


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


_PROVIDER_ENVS = [
    {},
    {"UPSTOX_ANALYTICS_TOKEN": "upstox-placeholder"},
    {"DATABENTO_API_KEY": "databento-placeholder"},
    {MARKET_DATA_PROVIDER_VARIABLE: "upstox", "UPSTOX_ANALYTICS_TOKEN": "upstox-placeholder"},
    {MARKET_DATA_PROVIDER_VARIABLE: "databento", "DATABENTO_API_KEY": "databento-placeholder"},
]


@pytest.mark.parametrize("env", _PROVIDER_ENVS, ids=["none", "upstox", "databento", "p-up", "p-db"])
@pytest.mark.parametrize(
    ("contract", "expected"), [(_NIFTY_OCT, "guarded"), (_ES_DEC, "plain")], ids=["nse", "cme"]
)
def test_paper_run_routes_by_contract_exchange_whatever_the_provider(
    database: Path, env: dict, contract: FuturesContract, expected: str
) -> None:
    calls: list = []
    rt = dataclasses.replace(
        build_database_runtime(database),
        paper_session=SpySession("plain", calls),
        expiry_guarded_paper_sessions={"NSE": SpySession("guarded", calls)},
    )
    tracking = TrackingEnv(env)

    _paper_run(
        database, contract, "2026-10-16T10:10:00Z", env=tracking, database_runtime=lambda p: rt
    )

    assert calls == [(expected, contract)]
    assert tracking.read == []


def test_no_k_option_exists_on_paper_commands(tmp_path: Path) -> None:
    rejected = _cli(
        [
            "paper", "run", "--database", str(tmp_path / "x.sqlite3"),
            "--product", "NIFTY", "--exchange", "NSE",
            "--expiration", "2026-10-27", "--strategy", "a", "--portfolio", "p", "--target", "1",
            "--as-of", "2026-10-16T10:10:00Z", "--sessions-before-expiry", "3",
        ]
    )  # fmt: skip

    assert rejected.code == ExitCode.INPUT
    assert "unrecognized arguments" in rejected.err
    for argv in (["paper", "run", "--help"], ["operations", "daily", "--help"]):
        text = _cli(argv).out.lower()
        assert "flatten" not in text and "sessions-before" not in text
    assert not (tmp_path / "x.sqlite3").exists()  # refused before any runtime was built


def test_no_k_environment_variable_exists() -> None:
    from northstar_api import settings

    for module in (settings, cli, runtime):
        source = Path(module.__file__).read_text(encoding="utf-8")
        assert "NORTHSTAR_FUTURES_EXPIRY" not in source
        assert 'FLATTEN_SESSIONS"' not in source
    assert runtime.NSE_EXPIRY_FLATTEN_SESSIONS == 5


# ---------------------------------------------------------------------------
# 9-12. End to end: NIFTY October on the real runtime and SQLite
# ---------------------------------------------------------------------------


@pytest.fixture
def october(database: Path) -> Path:
    sessions = _nse_window(date(2026, 8, 3), _E)
    sessions = sessions[-(25 + 8) :]  # 25 sessions through E-8, then E-7 .. E
    assert sessions[24].trading_date == _E8 and sessions[-1].trading_date == _E
    _seed_rise(database, _NIFTY_OCT, sessions, 25000)
    assert _economics(database, _NIFTY_OCT, "65", "INR").code == 0
    return database


def test_the_october_contract_is_flattened_before_expiry(october: Path) -> None:
    entry = _paper_run(october, _NIFTY_OCT, _nse_close(_E8))
    assert entry.code == 0, entry.err
    assert _section(entry, "DECISION")[0] == "Action: BUY"
    assert _section(entry, "ORDER")[:3] == ["State: PENDING", "Side: BUY", "Contracts: 2"]
    assert "Expiry flatten: yes" not in entry.out

    before = _paper_run(october, _NIFTY_OCT, _nse_close(_E7))
    assert before.code == 0, before.err
    assert "Expiry flatten: yes" not in _section(before, "ORDER")

    flatten = _paper_run(october, _NIFTY_OCT, _nse_close(_E6))
    assert flatten.code == 0, flatten.err
    assert _section(flatten, "ORDER")[:4] == [
        "State: PENDING",
        "Side: SELL",
        "Contracts: 2",
        "Expiry flatten: yes",
    ]

    for day in (_E5, _E4, _E3, _E2, _E1, _E):
        protected = _paper_run(october, _NIFTY_OCT, _nse_close(day))
        assert protected.code == 0, protected.err
        assert _section(protected, "ORDER") == [
            "State: NO ACTION",
            "Reason: EXPIRY FLATTEN WINDOW",
        ]

    assert _rows(october, "futures_paper_orders") == 2
    assert _rows(october, "futures_paper_fills") == 2


def _play_october(database: Path) -> dict[date, Outcome]:
    outcomes = {}
    for day in (_E8, _E7, _E6, _E5):
        outcomes[day] = _paper_run(database, _NIFTY_OCT, _nse_close(day))
        assert outcomes[day].code == 0, outcomes[day].err
    return outcomes


def test_the_flatten_fills_at_the_e5_open(october: Path) -> None:
    outcomes = _play_october(october)
    history = outcomes[_E5].lines
    start = history.index(f"Decision {_nse_close(_E6)}:")
    flatten = history[start + 1 : start + 9]

    e5_close = next(s for s in _NSE_SESSIONS if s.trading_date == _E5)
    # E-5 is session index 27: close 25000 + 1 + (27 - 15) = 25013, open one below.
    expected_open = Decimal("25012")
    assert "  State: FILLED" in flatten
    assert "  Expiry flatten: yes" in flatten
    assert f"  Simulated fill price: {expected_open}" in flatten
    assert f"  Fill observable from: {e5_close.closes_at.value}" in flatten


def test_the_flatten_order_identity_is_the_ordinary_one(october: Path) -> None:
    outcomes = _play_october(october)

    rt = build_database_runtime(october)
    [record] = [
        r
        for r in rt.forward_repository.get_records(
            FuturesForwardResearchRecordQuery(_NIFTY_OCT, _DAILY)
        )
        if r.decision_instant.value == _nse_close(_E6)
    ]
    expected = FuturesPaperExecutionIdentityService().order_identity(
        record, PaperPortfolioIdentity(_PORTFOLIO)
    )
    assert f"ID: {expected.identity}" in _section(outcomes[_E6], "ORDER")


def test_an_october_rerun_is_idempotent(october: Path) -> None:
    _play_october(october)
    first = _paper_run(october, _NIFTY_OCT, _nse_close(_E5))
    orders, fills = _rows(october, "futures_paper_orders"), _rows(october, "futures_paper_fills")

    second = _paper_run(october, _NIFTY_OCT, _nse_close(_E5))

    assert second.code == 0
    assert second.out == first.out
    assert (_rows(october, "futures_paper_orders"), _rows(october, "futures_paper_fills")) == (
        orders,
        fills,
    )


def test_paper_status_after_the_flatten_shows_a_flat_portfolio(october: Path) -> None:
    _play_october(october)

    status = _cli(
        [
            "paper", "status", "--database", str(october), "--strategy", _STRATEGY,
            "--portfolio", _PORTFOLIO, "--as-of", _nse_close(_E5),
        ]
    )  # fmt: skip

    assert status.code == 0, status.err
    assert _section(status, "PORTFOLIO (all contracts)") == ["Flat"]


# ---------------------------------------------------------------------------
# 9. CME is unchanged, in the same database, with the NSE calendar untouchable
# ---------------------------------------------------------------------------


@pytest.fixture
def cme(database: Path) -> Path:
    _seed_rise(database, _ES_DEC, _CME_SESSIONS[:25], 7600)
    assert _economics(database, _ES_DEC, "50", "USD").code == 0
    return database


def test_a_cme_paper_run_never_consults_the_nse_calendar(
    cme: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(*args, **kwargs):
        raise AssertionError("the NSE calendar must not be consulted for CME")

    monkeypatch.setattr(NSEFuturesTradingSessionResolver, "sessions_in_range", refuse)
    monkeypatch.setattr(NSEFuturesTradingSessionResolver, "resolve", refuse)

    outcome = _paper_run(cme, _ES_DEC, _CME_SESSIONS[24].closes_at.value, portfolio="es-alpha")

    assert outcome.code == 0, outcome.err
    assert _section(outcome, "ORDER")[:3] == ["State: PENDING", "Side: BUY", "Contracts: 2"]
    assert "Expiry flatten" not in outcome.out


def test_a_cme_paper_run_is_identical_without_any_guarded_session(tmp_path: Path) -> None:
    outputs = []
    for name, strip in (("wired", False), ("bare", True)):
        database = tmp_path / f"{name}.sqlite3"
        _seed_rise(database, _ES_DEC, _CME_SESSIONS[:25], 7600)
        _economics(database, _ES_DEC, "50", "USD")

        def factory(path, strip=strip):
            rt = build_database_runtime(path)
            return dataclasses.replace(rt, expiry_guarded_paper_sessions={}) if strip else rt

        outcome = _paper_run(
            database,
            _ES_DEC,
            _CME_SESSIONS[24].closes_at.value,
            portfolio="es-alpha",
            database_runtime=factory,
        )
        outputs.append((outcome.code, outcome.out, outcome.err))

    assert outputs[0] == outputs[1]


# ---------------------------------------------------------------------------
# 13-15. Error classification and fail-closed calendar cases
# ---------------------------------------------------------------------------


def test_an_expiry_window_error_classifies_as_data() -> None:
    code, message = cli._classify(FuturesExpiryWindowError("expiry is not a session"))

    assert code == ExitCode.DATA
    assert code != ExitCode.INTERNAL
    assert message.startswith("FuturesExpiryWindowError:")


def test_a_session_resolution_error_still_classifies_as_provider() -> None:
    code, _ = cli._classify(FuturesSessionResolutionError("Muhurat timings not notified"))

    assert code == ExitCode.PROVIDER


def test_an_expiration_on_an_nse_holiday_exits_data_and_writes_no_execution(
    database: Path,
) -> None:
    contract = FuturesContract(_NIFTY, ExpirationDate("2026-11-24"))
    _seed_rise(database, contract, _nse_window(date(2026, 8, 3), _E8)[-25:], 25000)

    outcome = _paper_run(database, contract, _nse_close(_E8))

    assert outcome.code == ExitCode.DATA
    assert "FuturesExpiryWindowError" in outcome.err
    assert "2026-11-24" in outcome.err
    assert "Traceback" not in outcome.out + outcome.err
    assert _rows(database, "futures_paper_orders") == 0
    assert _rows(database, "futures_paper_fills") == 0


def test_a_november_countdown_across_the_muhurat_session_exits_provider(database: Path) -> None:
    _seed_rise(database, _NIFTY_NOV, _nse_window(date(2026, 8, 3), date(2026, 11, 2))[-25:], 25000)

    outcome = _paper_run(database, _NIFTY_NOV, _nse_close(date(2026, 11, 2)))

    assert outcome.code == ExitCode.PROVIDER
    assert "FuturesSessionResolutionError" in outcome.err
    assert "2026-11-08" in outcome.err
    assert _rows(database, "futures_paper_orders") == 0
    assert _rows(database, "futures_paper_fills") == 0


# ---------------------------------------------------------------------------
# 19. Read-only snapshot paths are untouched
# ---------------------------------------------------------------------------


def test_the_snapshot_is_shared_and_guard_free(database: Path) -> None:
    rt = build_database_runtime(database)

    assert not hasattr(rt.snapshot, "_expiry_guard")
    assert "expiry" not in inspect.getsource(type(rt.snapshot).__init__).lower()
