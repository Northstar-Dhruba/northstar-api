"""Tests for provider-selected futures market-data runtime composition.

``market-data sync`` selects its provider with ``--provider`` (default
``databento``) and reads only that provider's secret. ``operations daily``
selects with NORTHSTAR_FUTURES_MARKET_DATA_PROVIDER and refuses Upstox, because
it decides completed sessions from the wall clock.

No test contacts a provider. The Upstox end-to-end tests run the real
composition -- CLI, runtime builder, Upstox adapter, native-daily use case, NSE
calendar and SQLite store -- with only the adapter's HTTP transport faked. The
token below is a placeholder, not a credential.
"""

from __future__ import annotations

import ast
import gzip
import io
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.error import HTTPError

import pytest
from northstar_application.application_services import (
    AcquireFuturesDailyHistoryUseCase,
    AcquireFuturesNativeDailyHistoryUseCase,
    AggregateFuturesDailySessionBarUseCase,
    FuturesDailyAcquisitionResult,
)
from northstar_application.ports import (
    FuturesDailyHistoricalAcquisitionQuery,
    FuturesHistoricalMarketDataQuery,
)
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import (
    ExchangeCode,
    PointInTime,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.futures import FuturesContract, FuturesProductReference
from northstar_infrastructure.market_data import (
    DatabentoFuturesHistoricalMarketDataSource,
    ExchangeCalendarFuturesTradingSessionResolver,
    NSEFuturesTradingSessionResolver,
    SQLiteFuturesHistoricalMarketDataRepository,
    SQLiteFuturesHistoricalMarketDataStore,
    UpstoxFuturesNativeDailyMarketDataSource,
)
from northstar_infrastructure.market_data.upstox_instrument_master import (
    NSE_INSTRUMENT_MASTER_URL,
)

from northstar_api import cli, runtime
from northstar_api.cli import ExitCode, main
from northstar_api.runtime import (
    build_market_sync_runtime,
    build_upstox_market_sync_runtime,
)
from northstar_api.settings import (
    MARKET_DATA_PROVIDER_VARIABLE,
    DashboardSettingsError,
    FuturesMarketDataProvider,
    load_market_data_provider,
    parse_market_data_provider,
)

_DATABENTO_KEY = "db-SECRET-NEVER-PRINTED-0123456789"
_UPSTOX_TOKEN = "upstox-PLACEHOLDER-TOKEN-NEVER-PRINTED-" + "x" * 298

_NIFTY = FuturesProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_NIFTY_OCT = FuturesContract(_NIFTY, ExpirationDate("2026-10-27"))
_ES_DEC = FuturesContract(
    FuturesProductReference(Symbol("ES"), ExchangeCode("CME")), ExpirationDate("2026-12-18")
)
_LOT = 65
_IST = timezone(timedelta(hours=5, minutes=30))

_UPSTOX_SYNC = [
    "market-data", "sync", "--product", "NIFTY", "--exchange", "NSE",
    "--expiration", "2026-10-27", "--start", "2026-09-29", "--end", "2026-09-30",
    "--provider", "upstox",
]  # fmt: skip
_DATABENTO_SYNC = [
    "market-data", "sync", "--product", "ES", "--exchange", "CME",
    "--expiration", "2026-12-18", "--start", "2026-09-14", "--end", "2026-09-16",
]  # fmt: skip


@dataclass(frozen=True)
class Outcome:
    code: int
    out: str
    err: str

    @property
    def text(self) -> str:
        return self.out + self.err


def _cli(argv: list[str], *, env: dict | None = None, **kwargs: Any) -> Outcome:
    out, err = io.StringIO(), io.StringIO()
    code = main(argv, env={} if env is None else env, stdout=out, stderr=err, **kwargs)
    return Outcome(code, out.getvalue(), err.getvalue())


def _assert_no_secret(outcome: Outcome) -> None:
    assert "Traceback" not in outcome.text
    assert _UPSTOX_TOKEN not in outcome.text
    assert _DATABENTO_KEY not in outcome.text


class TrackingEnv(dict):
    def __init__(self, *args: Any) -> None:
        super().__init__(*args)
        self.read: list[str] = []

    def get(self, key, default=None):
        self.read.append(key)
        return super().get(key, default)

    def __getitem__(self, key):
        self.read.append(key)
        return super().__getitem__(key)


class StubAcquisition:
    def __init__(self) -> None:
        self.queries: list[FuturesDailyHistoricalAcquisitionQuery] = []

    def execute(self, query: FuturesDailyHistoricalAcquisitionQuery):
        self.queries.append(query)
        return FuturesDailyAcquisitionResult(query, 2, 2)


class Runtimes:
    """Records which provider runtime the CLI composed, and with which secret."""

    def __init__(self) -> None:
        self.databento: list[tuple[Path, str]] = []
        self.upstox: list[tuple[Path, str]] = []
        self.acquisition = StubAcquisition()

    def build_databento(self, path: Path, api_key: str) -> StubAcquisition:
        self.databento.append((path, api_key))
        return self.acquisition

    def build_upstox(self, path: Path, token: str) -> StubAcquisition:
        self.upstox.append((path, token))
        return self.acquisition

    def kwargs(self) -> dict[str, Any]:
        return {
            "market_sync_runtime": self.build_databento,
            "upstox_market_sync_runtime": self.build_upstox,
        }


def _never_called_clock() -> datetime:
    raise AssertionError("the clock must not be read on this path")


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "northstar.sqlite3"


# ---------------------------------------------------------------------------
# Upstox fake HTTP (documented v3 shape; master served gzipped)
# ---------------------------------------------------------------------------


def _expiry_ms(day: date) -> int:
    instant = datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=_IST)
    return int((instant - datetime(1970, 1, 1, tzinfo=UTC)).total_seconds()) * 1000


def _master() -> bytes:
    def future(key: str, expiry: date) -> dict[str, Any]:
        return {
            "segment": "NSE_FO",
            "exchange": "NSE",
            "instrument_type": "FUT",
            "underlying_symbol": "NIFTY",
            "expiry": _expiry_ms(expiry),
            "instrument_key": key,
            "lot_size": _LOT,
            "trading_symbol": f"NIFTY FUT {expiry.strftime('%d %b %y').upper()}",
        }

    records = [
        {"segment": "NSE_INDEX", "exchange": "NSE", "instrument_type": "INDEX",
         "instrument_key": "NSE_INDEX|Nifty 50", "trading_symbol": "NIFTY"},
        future("NSE_FO|48704", date(2026, 10, 27)),
        future("NSE_FO|61471", date(2026, 11, 24)),
    ]  # fmt: skip
    return gzip.compress(json.dumps(records).encode("utf-8"))


def _candles(*rows: tuple[str, int]) -> bytes:
    candles = [
        [f"{day}T00:00:00+05:30", 22800, 22950, 22650, 22870.5, contracts * _LOT, 18_000_000]
        for day, contracts in rows
    ]
    return json.dumps({"status": "success", "data": {"candles": candles}}).encode("utf-8")


class FakeUpstox:
    """Routes the master and candle URLs; records URL and header names only."""

    def __init__(self, candles: bytes | None = None, error: Exception | None = None) -> None:
        self.candles = (
            _candles(("2026-09-30", 58953), ("2026-09-29", 109189)) if candles is None else candles
        )
        self.error = error
        self.urls: list[str] = []
        self.authorization: list[str | None] = []

    def __call__(self, url: str, headers: dict[str, str], timeout: float) -> bytes:
        self.urls.append(url)
        self.authorization.append(headers.get("Authorization"))
        if url == NSE_INSTRUMENT_MASTER_URL:
            return _master()
        if self.error is not None:
            raise self.error
        return self.candles


def _upstox_runtime(fake: FakeUpstox):
    def build(path: Path, token: str) -> AcquireFuturesNativeDailyHistoryUseCase:
        return build_upstox_market_sync_runtime(path, token, fetch=fake)

    return build


def _stored_daily_bars(database: Path, contract: FuturesContract = _NIFTY_OCT):
    return SQLiteFuturesHistoricalMarketDataRepository(database).get_bars(
        FuturesHistoricalMarketDataQuery(contract, Timeframe("1d"))
    )


# ---------------------------------------------------------------------------
# Provider vocabulary
# ---------------------------------------------------------------------------


def test_the_supported_providers_are_exactly_databento_and_upstox() -> None:
    assert [p.value for p in FuturesMarketDataProvider] == ["databento", "upstox"]


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("databento", FuturesMarketDataProvider.DATABENTO),
        ("upstox", FuturesMarketDataProvider.UPSTOX),
        (" Upstox ", FuturesMarketDataProvider.UPSTOX),
    ],
)
def test_a_provider_name_parses(text: str, expected: FuturesMarketDataProvider) -> None:
    assert parse_market_data_provider(text) is expected


@pytest.mark.parametrize("text", ["zerodha", "", "databento,upstox", "DATABENTO_API_KEY"])
def test_an_unsupported_provider_name_fails_clearly(text: str) -> None:
    with pytest.raises(ValueError, match="must be one of databento, upstox"):
        parse_market_data_provider(text)


@pytest.mark.parametrize("env", [{}, {MARKET_DATA_PROVIDER_VARIABLE: "  "}])
def test_an_unset_provider_variable_keeps_databento(env: dict[str, str]) -> None:
    assert load_market_data_provider(env) is FuturesMarketDataProvider.DATABENTO


def test_the_provider_is_never_inferred_from_credentials() -> None:
    env = {"UPSTOX_ANALYTICS_TOKEN": _UPSTOX_TOKEN}

    assert load_market_data_provider(env) is FuturesMarketDataProvider.DATABENTO


def test_an_unsupported_provider_variable_fails_clearly() -> None:
    with pytest.raises(DashboardSettingsError, match=MARKET_DATA_PROVIDER_VARIABLE):
        load_market_data_provider({MARKET_DATA_PROVIDER_VARIABLE: "zerodha"})


# ---------------------------------------------------------------------------
# Runtime composition
# ---------------------------------------------------------------------------


def test_databento_composition_is_unchanged(database: Path) -> None:
    acquisition = build_market_sync_runtime(database, _DATABENTO_KEY)

    assert type(acquisition) is AcquireFuturesDailyHistoryUseCase
    assert type(acquisition._session_resolver) is ExchangeCalendarFuturesTradingSessionResolver
    assert type(acquisition._source) is DatabentoFuturesHistoricalMarketDataSource
    assert type(acquisition._aggregator) is AggregateFuturesDailySessionBarUseCase
    assert type(acquisition._store) is SQLiteFuturesHistoricalMarketDataStore


def test_upstox_composition_builds_the_native_daily_path(database: Path) -> None:
    acquisition = build_upstox_market_sync_runtime(database, _UPSTOX_TOKEN)

    assert type(acquisition) is AcquireFuturesNativeDailyHistoryUseCase
    assert type(acquisition._session_resolver) is NSEFuturesTradingSessionResolver
    assert type(acquisition._source) is UpstoxFuturesNativeDailyMarketDataSource
    assert type(acquisition._store) is SQLiteFuturesHistoricalMarketDataStore
    assert not hasattr(acquisition, "_aggregator")
    assert database.is_file()


def test_upstox_composition_never_constructs_the_databento_path(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("the Databento path must not be constructed for Upstox")

    for name in (
        "DatabentoFuturesHistoricalMarketDataSource",
        "ExchangeCalendarFuturesTradingSessionResolver",
        "AggregateFuturesDailySessionBarUseCase",
        "AcquireFuturesDailyHistoryUseCase",
    ):
        monkeypatch.setattr(runtime, name, refuse)
    fake = FakeUpstox()

    outcome = _cli(
        [*_UPSTOX_SYNC, "--database", str(database)],
        env={"UPSTOX_ANALYTICS_TOKEN": _UPSTOX_TOKEN},
        upstox_market_sync_runtime=_upstox_runtime(fake),
    )

    assert outcome.code == ExitCode.SUCCESS, outcome.err


def test_the_upstox_token_reaches_infrastructure_without_being_exposed(database: Path) -> None:
    fake = FakeUpstox()

    acquisition = build_upstox_market_sync_runtime(database, _UPSTOX_TOKEN, fetch=fake)
    acquisition.execute(
        FuturesDailyHistoricalAcquisitionQuery(_NIFTY_OCT, date(2026, 9, 29), date(2026, 9, 30))
    )

    master, candles = fake.authorization
    assert master is None  # the public master is fetched without credentials
    assert candles == f"Bearer {_UPSTOX_TOKEN}"
    for text in (repr(acquisition), repr(acquisition._source), str(acquisition._source)):
        assert _UPSTOX_TOKEN not in text


def test_the_composition_root_reads_no_clock_for_upstox() -> None:
    tree = ast.parse(Path(runtime.__file__).read_text(encoding="utf-8"))
    builder = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "build_upstox_market_sync_runtime"
    )

    # The builder reads no clock: it only forwards an instant its caller captured.
    for node in ast.walk(builder):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in {"now", "utcnow", "today", "time", "monotonic"}
        if isinstance(node, ast.Name):
            assert node.id != "clock"
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            for cutoff in ("16:00", "19:00", "21:00", "previous day", "next morning"):
                assert cutoff not in node.value
    assert [a.arg for a in builder.args.args] == ["path", "access_token"]
    assert [a.arg for a in builder.args.kwonlyargs] == ["fetch", "current_instant"]
    assert [ast.unparse(d) for d in builder.args.kw_defaults] == ["None", "None"]


def test_manual_upstox_sync_supplies_no_current_instant(database: Path) -> None:
    """Manual sync stays historical-only: no instant, no current-day request, no clock."""
    fake = FakeUpstox()
    received: list[dict] = []

    def build(path: Path, token: str, **kwargs) -> AcquireFuturesNativeDailyHistoryUseCase:
        received.append(kwargs)
        return build_upstox_market_sync_runtime(path, token, fetch=fake, **kwargs)

    outcome = _cli(
        [*_UPSTOX_SYNC, "--database", str(database)],
        env={"UPSTOX_ANALYTICS_TOKEN": _UPSTOX_TOKEN},
        upstox_market_sync_runtime=build,
        clock=_never_called_clock,
    )

    assert outcome.code == ExitCode.SUCCESS, outcome.err
    assert received == [{}]
    assert not any("/historical-candle/intraday/" in url for url in fake.urls)


# ---------------------------------------------------------------------------
# market-data sync: provider selection
# ---------------------------------------------------------------------------


def test_default_sync_builds_the_databento_path(database: Path) -> None:
    runtimes = Runtimes()
    env = TrackingEnv({"DATABENTO_API_KEY": _DATABENTO_KEY})

    outcome = _cli([*_DATABENTO_SYNC, "--database", str(database)], env=env, **runtimes.kwargs())

    assert outcome.code == ExitCode.SUCCESS
    assert runtimes.databento == [(database, _DATABENTO_KEY)]
    assert runtimes.upstox == []
    assert env.read == ["DATABENTO_API_KEY"]
    assert "Provider: databento" in outcome.out
    _assert_no_secret(outcome)


def test_explicit_databento_selection_builds_the_databento_path(database: Path) -> None:
    runtimes = Runtimes()

    outcome = _cli(
        [*_DATABENTO_SYNC, "--database", str(database), "--provider", "databento"],
        env={"DATABENTO_API_KEY": _DATABENTO_KEY},
        **runtimes.kwargs(),
    )

    assert outcome.code == ExitCode.SUCCESS
    assert runtimes.databento == [(database, _DATABENTO_KEY)]
    assert runtimes.upstox == []


def test_upstox_selection_builds_the_upstox_path_with_only_its_token(database: Path) -> None:
    runtimes = Runtimes()
    env = TrackingEnv(
        {"UPSTOX_ANALYTICS_TOKEN": _UPSTOX_TOKEN, "DATABENTO_API_KEY": _DATABENTO_KEY}
    )

    outcome = _cli([*_UPSTOX_SYNC, "--database", str(database)], env=env, **runtimes.kwargs())

    assert outcome.code == ExitCode.SUCCESS
    assert runtimes.upstox == [(database, _UPSTOX_TOKEN)]
    assert runtimes.databento == []
    assert env.read == ["UPSTOX_ANALYTICS_TOKEN"]
    assert "Provider: upstox" in outcome.out
    _assert_no_secret(outcome)


def test_databento_does_not_require_an_upstox_token(database: Path) -> None:
    runtimes = Runtimes()
    env = TrackingEnv({"DATABENTO_API_KEY": _DATABENTO_KEY})

    outcome = _cli([*_DATABENTO_SYNC, "--database", str(database)], env=env, **runtimes.kwargs())

    assert outcome.code == ExitCode.SUCCESS
    assert "UPSTOX_ANALYTICS_TOKEN" not in env.read


@pytest.mark.parametrize("env", [{}, {"UPSTOX_ANALYTICS_TOKEN": "   "}])
def test_a_missing_upstox_token_fails_clearly(database: Path, env: dict[str, str]) -> None:
    runtimes = Runtimes()

    outcome = _cli([*_UPSTOX_SYNC, "--database", str(database)], env=env, **runtimes.kwargs())

    assert outcome.code == ExitCode.CONFIGURATION
    assert "UPSTOX_ANALYTICS_TOKEN is not set; market-data sync needs Upstox credentials." in (
        outcome.err
    )
    assert runtimes.upstox == [] and runtimes.databento == []


def test_a_databento_key_does_not_stand_in_for_the_upstox_token(database: Path) -> None:
    runtimes = Runtimes()

    outcome = _cli(
        [*_UPSTOX_SYNC, "--database", str(database)],
        env={"DATABENTO_API_KEY": _DATABENTO_KEY},
        **runtimes.kwargs(),
    )

    assert outcome.code == ExitCode.CONFIGURATION
    assert runtimes.databento == [] and runtimes.upstox == []


def test_an_unsupported_provider_fails_clearly(database: Path) -> None:
    runtimes = Runtimes()

    outcome = _cli(
        [*_DATABENTO_SYNC, "--database", str(database), "--provider", "zerodha"],
        env={"DATABENTO_API_KEY": _DATABENTO_KEY},
        **runtimes.kwargs(),
    )

    assert outcome.code == ExitCode.INPUT
    assert "invalid choice: 'zerodha'" in outcome.err
    assert "databento" in outcome.err and "upstox" in outcome.err
    assert runtimes.databento == [] and runtimes.upstox == []


def test_the_explicit_range_is_forwarded_unchanged(database: Path) -> None:
    runtimes = Runtimes()

    _cli(
        [*_UPSTOX_SYNC, "--database", str(database)],
        env={"UPSTOX_ANALYTICS_TOKEN": _UPSTOX_TOKEN},
        **runtimes.kwargs(),
    )

    assert runtimes.acquisition.queries == [
        FuturesDailyHistoricalAcquisitionQuery(_NIFTY_OCT, date(2026, 9, 29), date(2026, 9, 30))
    ]


def test_upstox_sync_never_reads_the_clock(database: Path) -> None:
    outcome = _cli(
        [*_UPSTOX_SYNC, "--database", str(database)],
        env={"UPSTOX_ANALYTICS_TOKEN": _UPSTOX_TOKEN},
        upstox_market_sync_runtime=_upstox_runtime(FakeUpstox()),
        clock=_never_called_clock,
    )

    assert outcome.code == ExitCode.SUCCESS, outcome.err


def test_a_future_dated_range_is_forwarded_without_a_finality_check(database: Path) -> None:
    """No cutoff is applied: eligibility is the caller's, even for dates ahead of today."""
    runtimes = Runtimes()
    argv = [
        *_UPSTOX_SYNC[:8],
        "--start",
        "2099-01-05",
        "--end",
        "2099-01-09",
        "--provider",
        "upstox",
    ]

    outcome = _cli(
        [*argv, "--database", str(database)],
        env={"UPSTOX_ANALYTICS_TOKEN": _UPSTOX_TOKEN},
        clock=_never_called_clock,
        **runtimes.kwargs(),
    )

    assert outcome.code == ExitCode.SUCCESS
    assert runtimes.acquisition.queries[0].start_trading_date == date(2099, 1, 5)
    assert runtimes.acquisition.queries[0].end_trading_date == date(2099, 1, 9)


def test_sync_help_documents_the_provider_choice() -> None:
    outcome = _cli(["market-data", "sync", "--help"])

    assert outcome.code == 0
    assert "--provider" in outcome.out
    assert "{databento,upstox}" in outcome.out
    assert "default: databento" in outcome.out


# ---------------------------------------------------------------------------
# market-data sync: Upstox end to end through the real composition
# ---------------------------------------------------------------------------


def test_upstox_sync_persists_session_close_bars_through_the_real_runtime(
    database: Path,
) -> None:
    fake = FakeUpstox()

    outcome = _cli(
        [*_UPSTOX_SYNC, "--database", str(database)],
        env={"UPSTOX_ANALYTICS_TOKEN": _UPSTOX_TOKEN},
        upstox_market_sync_runtime=_upstox_runtime(fake),
    )

    assert outcome.code == ExitCode.SUCCESS, outcome.err
    assert "SYNC: COMPLETED" in outcome.out
    assert "Sessions in range: 2" in outcome.out
    assert fake.urls == [
        NSE_INSTRUMENT_MASTER_URL,
        "https://api.upstox.com/v3/historical-candle/NSE_FO%7C48704/days/1/2026-09-30/2026-09-29",
    ]

    bars = _stored_daily_bars(database)
    sessions = NSEFuturesTradingSessionResolver().sessions_in_range(
        _NIFTY, date(2026, 9, 29), date(2026, 9, 30)
    )
    assert [bar.point_in_time for bar in bars] == [s.closes_at for s in sessions]
    assert bars[0].point_in_time == PointInTime("2026-09-29T10:10:00Z")
    assert [bar.volume for bar in bars] == [Quantity(Decimal(109189)), Quantity(Decimal(58953))]
    assert all(bar.contract == _NIFTY_OCT for bar in bars)
    _assert_no_secret(outcome)


def test_an_upstox_rerun_is_idempotent(database: Path) -> None:
    for _ in range(2):
        outcome = _cli(
            [*_UPSTOX_SYNC, "--database", str(database)],
            env={"UPSTOX_ANALYTICS_TOKEN": _UPSTOX_TOKEN},
            upstox_market_sync_runtime=_upstox_runtime(FakeUpstox()),
        )
        assert outcome.code == ExitCode.SUCCESS, outcome.err

    assert len(_stored_daily_bars(database)) == 2


def test_a_missing_upstox_session_is_a_data_error_and_stores_nothing(database: Path) -> None:
    fake = FakeUpstox(candles=_candles(("2026-09-30", 58953)))

    outcome = _cli(
        [*_UPSTOX_SYNC, "--database", str(database)],
        env={"UPSTOX_ANALYTICS_TOKEN": _UPSTOX_TOKEN},
        upstox_market_sync_runtime=_upstox_runtime(fake),
    )

    assert outcome.code == ExitCode.DATA
    assert "FuturesDailySessionCoverageError" in outcome.err
    assert "2026-09-29" in outcome.err
    assert "Nothing from this range was stored." in outcome.err
    assert _stored_daily_bars(database) == ()
    _assert_no_secret(outcome)


@pytest.mark.parametrize(
    ("status", "error_name"),
    [(401, "UpstoxAuthenticationError"), (429, "UpstoxProviderUnavailableError")],
)
def test_an_upstox_provider_failure_is_a_provider_error_without_the_token(
    database: Path, status: int, error_name: str
) -> None:
    error = HTTPError("u", status, "provider error", {}, io.BytesIO(b""))
    fake = FakeUpstox(error=error)

    outcome = _cli(
        [*_UPSTOX_SYNC, "--database", str(database)],
        env={"UPSTOX_ANALYTICS_TOKEN": _UPSTOX_TOKEN},
        upstox_market_sync_runtime=_upstox_runtime(fake),
    )

    assert outcome.code == ExitCode.PROVIDER
    assert error_name in outcome.err
    assert "Nothing from this range was stored." in outcome.err
    _assert_no_secret(outcome)


def test_a_secret_echoed_by_a_failure_is_redacted(database: Path) -> None:
    def leaky_runtime(path: Path, token: str):
        class Leaky:
            def execute(self, query):
                raise cli.UpstoxMarketDataSourceError(f"provider echoed {token}")

        return Leaky()

    outcome = _cli(
        [*_UPSTOX_SYNC, "--database", str(database)],
        env={"UPSTOX_ANALYTICS_TOKEN": _UPSTOX_TOKEN},
        upstox_market_sync_runtime=leaky_runtime,
    )

    assert outcome.code == ExitCode.PROVIDER
    assert "[REDACTED]" in outcome.err
    _assert_no_secret(outcome)


# ---------------------------------------------------------------------------
# operations daily: clock-decided ranges stay Databento-only
# ---------------------------------------------------------------------------


def _operation_env(database: Path, **extra: str) -> dict[str, str]:
    return {
        "NORTHSTAR_DATABASE": str(database),
        "NORTHSTAR_FUTURES_PRODUCT": "NIFTY",
        "NORTHSTAR_FUTURES_EXCHANGE": "NSE",
        "NORTHSTAR_FUTURES_EXPIRATION": "2026-10-27",
        "NORTHSTAR_STRATEGY": "alpha",
        "NORTHSTAR_PORTFOLIO": "futures-paper-alpha",
        "NORTHSTAR_TARGET": "1",
        **extra,
    }


def test_operations_daily_with_upstox_fails_closed_by_default(database: Path) -> None:
    """INDIA-8B supersedes the INDIA-4 refusal: Upstox now runs the chronological
    operation, whose unset finality mode is disabled -- no clock, no acquisition."""
    built: list[object] = []
    env = TrackingEnv(
        _operation_env(
            database,
            **{
                MARKET_DATA_PROVIDER_VARIABLE: "upstox",
                "UPSTOX_ANALYTICS_TOKEN": _UPSTOX_TOKEN,
                "NORTHSTAR_FUTURES_GO_LIVE": "2026-09-29",
            },
        )
    )

    outcome = _cli(
        ["operations", "daily"],
        env=env,
        daily_sync_runtime=lambda *args: built.append(args),
        upstox_market_sync_runtime=lambda *args: built.append(args),
        clock=_never_called_clock,
    )

    assert outcome.code == ExitCode.SUCCESS
    assert "Finality mode: disabled" in outcome.out
    assert "STATUS: WAITING -- Daily-bar finality is not established" in outcome.out
    assert built == []
    assert "DATABENTO_API_KEY" not in env.read
    _assert_no_secret(outcome)


def test_operations_daily_rejects_an_unsupported_provider(database: Path) -> None:
    outcome = _cli(
        ["operations", "daily"],
        env=_operation_env(database, **{MARKET_DATA_PROVIDER_VARIABLE: "zerodha"}),
        clock=_never_called_clock,
    )

    assert outcome.code == ExitCode.CONFIGURATION
    assert "must be one of databento, upstox" in outcome.err


@pytest.mark.parametrize("provider", [None, "databento"])
def test_operations_daily_keeps_the_databento_path(database: Path, provider: str | None) -> None:
    """Unset or explicit databento passes selection and reaches the Databento secret check."""
    extra = {} if provider is None else {MARKET_DATA_PROVIDER_VARIABLE: provider}

    outcome = _cli(
        ["operations", "daily"],
        env=_operation_env(database, **extra),
        clock=_never_called_clock,
    )

    assert outcome.code == ExitCode.CONFIGURATION
    assert "DATABENTO_API_KEY is not set; operations daily needs Databento credentials." in (
        outcome.err
    )


def test_daily_help_documents_the_provider_variable() -> None:
    outcome = _cli(["operations", "daily", "--help"])

    assert MARKET_DATA_PROVIDER_VARIABLE in outcome.out
    assert "--provider" not in outcome.out
