"""INDIA-8G-A1: ``northstar finality-evidence observe``.

The Upstox HTTP transport is INDIA-7's range-honouring double over real NSE
sessions; the token is a placeholder and the network is refused for the whole
module. Every test injects a ``database_runtime`` that fails if called, so the
command provably never opens a Northstar database.
"""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError

import pytest
from northstar_infrastructure.market_data import (
    UpstoxDailyCandleEvidenceCollector,
    UpstoxMarketDataSourceError,
)
from test_india7_nifty_incremental_operations_acceptance import (
    _LOT,
    _TOKEN,
    FakeUpstox,
    Outcome,
    _day,
    _normal_market,
    no_network,  # noqa: F401 - module-scoped autouse fixture
)

from northstar_api.cli import ExitCode, build_parser, main
from northstar_api.runtime import build_database_runtime

_SCHEMA = "northstar.upstox-daily-candle-observation/1"


class Clock:
    def __init__(self, *instants: datetime) -> None:
        self.instants = list(instants)
        self.reads = 0

    def __call__(self) -> datetime:
        self.reads += 1
        return self.instants.pop(0)


class TrackingEnv(dict):
    def __init__(self, values: dict) -> None:
        super().__init__(values)
        self.read: list[str] = []

    def get(self, key, default=None):
        self.read.append(key)
        return super().get(key, default)


def _no_database(path):
    raise AssertionError("finality-evidence observe must never open a Northstar database")


def _instants() -> tuple[datetime, datetime]:
    return (
        datetime(2026, 9, 2, 10, 45, 0, 250000, tzinfo=UTC),
        datetime(2026, 9, 2, 10, 45, 1, tzinfo=UTC),
    )


def _observe(
    evidence: Path,
    *,
    fake: FakeUpstox | None = None,
    bar: int = 21,
    trading_date: str | None = None,
    exchange: str = "NSE",
    env: dict | None = None,
    clock: Clock | None = None,
) -> tuple[Outcome, FakeUpstox, Clock]:
    fake = fake or FakeUpstox(_normal_market())
    clock = clock or Clock(*_instants())
    out, err = io.StringIO(), io.StringIO()
    code = main(
        [
            "finality-evidence",
            "observe",
            "--evidence",
            str(evidence),
            "--product",
            "NIFTY",
            "--exchange",
            exchange,
            "--expiration",
            "2026-10-27",
            "--trading-date",
            trading_date or _day(bar),
        ],  # fmt: skip
        env={"UPSTOX_ANALYTICS_TOKEN": _TOKEN} if env is None else env,
        stdout=out,
        stderr=err,
        clock=clock,
        database_runtime=_no_database,
        upstox_candle_evidence=lambda token, clk: UpstoxDailyCandleEvidenceCollector(
            token, clock=clk, fetch=fake
        ),
    )
    return Outcome(code, out.getvalue(), err.getvalue()), fake, clock


def _records(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


@pytest.fixture
def evidence(tmp_path: Path) -> Path:
    return tmp_path / "evidence.jsonl"


# ---------------------------------------------------------------------------
# Parsing and isolation of the command surface
# ---------------------------------------------------------------------------


def test_the_command_takes_only_evidence_contract_and_trading_date() -> None:
    parser = build_parser()
    groups = next(a for a in parser._actions if a.dest == "group")
    observe = groups.choices["finality-evidence"]._actions[-1].choices["observe"]

    options = {opt for action in observe._actions for opt in action.option_strings}
    assert options == {
        "-h", "--help", "--evidence", "--product", "--exchange", "--expiration", "--trading-date",
    }  # fmt: skip


def test_a_missing_argument_is_input(evidence: Path) -> None:
    code = main(
        ["finality-evidence", "observe", "--evidence", str(evidence)],
        env={},
        stdout=io.StringIO(),
        stderr=io.StringIO(),
    )

    assert code == ExitCode.INPUT


# ---------------------------------------------------------------------------
# Successful observations
# ---------------------------------------------------------------------------


def test_one_observation_is_appended_with_the_injected_clock(evidence: Path) -> None:
    outcome, fake, clock = _observe(evidence)

    assert outcome.code == ExitCode.SUCCESS, outcome.err
    (record,) = _records(evidence)
    market = fake.market
    assert record["schema"] == _SCHEMA
    assert (record["requested_at"], record["received_at"]) == (
        "2026-09-02T10:45:00.25Z",
        "2026-09-02T10:45:01Z",
    )
    assert clock.reads == 2
    assert record["contract"] == {"product": "NIFTY", "exchange": "NSE", "expiration": "2026-10-27"}
    assert record["trading_date"] == _day(21)
    assert record["candle"]["close"] == str(int(market.close(21)))
    assert record["volume_contracts"] == str(market.volumes[20])
    assert record["candle"]["volume"] == str(market.volumes[20] * _LOT)
    assert fake.candle_requests == [(_day(21), _day(21))]
    assert outcome.out.splitlines()[:7] == [
        "CANDLE EVIDENCE: OBSERVATION RECORDED",
        "Provider: upstox",
        "Contract: NIFTY@NSE 2026-10-27",
        f"Trading date: {_day(21)}",
        "Requested at: 2026-09-02T10:45:00.25Z",
        "Received at: 2026-09-02T10:45:01Z",
        "Candle present: yes",
    ]
    assert f"Close: {int(market.close(21))}" in outcome.out
    assert f"Volume (contracts): {market.volumes[20]}" in outcome.out


def test_repeated_identical_observations_are_each_appended(evidence: Path) -> None:
    _observe(evidence)
    later = Clock(
        datetime(2026, 9, 2, 16, 0, tzinfo=UTC), datetime(2026, 9, 2, 16, 0, 1, tzinfo=UTC)
    )
    outcome, _, _ = _observe(evidence, clock=later)

    assert outcome.code == ExitCode.SUCCESS
    first, second = _records(evidence)
    assert first["candle"] == second["candle"]
    assert second["requested_at"] == "2026-09-02T16:00:00Z"


def test_an_absent_candle_is_recorded_and_succeeds(evidence: Path) -> None:
    fake = FakeUpstox(_normal_market(), omit={21})

    outcome, _, _ = _observe(evidence, fake=fake)

    assert outcome.code == ExitCode.SUCCESS
    assert "Candle present: no" in outcome.out
    assert "Open:" not in outcome.out
    (record,) = _records(evidence)
    assert record["candle"] is None


def test_the_output_draws_no_conclusion(evidence: Path) -> None:
    outcome, _, _ = _observe(evidence)

    rendered = "\n".join(
        line for line in outcome.out.splitlines() if not line.startswith("Evidence file:")
    ).lower()
    for word in ("final", "stable", "safe", "recommend", "valid"):
        assert word not in rendered


# ---------------------------------------------------------------------------
# Refusals
# ---------------------------------------------------------------------------


def test_a_missing_token_is_configuration_and_writes_nothing(evidence: Path) -> None:
    outcome, fake, _ = _observe(evidence, env={})

    assert outcome.code == ExitCode.CONFIGURATION
    assert "UPSTOX_ANALYTICS_TOKEN is not set" in outcome.err
    assert not evidence.exists() and fake.candle_requests == []


@pytest.mark.parametrize("day", ["2026-10-03", "2026-10-02"], ids=["saturday", "holiday"])
def test_a_non_session_date_is_input(evidence: Path, day: str) -> None:
    outcome, fake, _ = _observe(evidence, trading_date=day)

    assert outcome.code == ExitCode.INPUT
    assert "is not an NSE futures trading session" in outcome.err
    assert not evidence.exists() and fake.candle_requests == []


def test_an_unresolved_calendar_stays_fail_closed(evidence: Path) -> None:
    outcome, fake, _ = _observe(evidence, trading_date="2026-11-08")  # Muhurat

    assert outcome.code == ExitCode.PROVIDER
    assert "FuturesSessionResolutionError" in outcome.err
    assert not evidence.exists() and fake.candle_requests == []


def test_another_venue_is_input(evidence: Path) -> None:
    outcome, _, _ = _observe(evidence, exchange="CME")

    assert outcome.code == ExitCode.INPUT
    assert "supports NSE contracts only" in outcome.err


def _provider_error(status: int, body: bytes) -> HTTPError:
    return HTTPError("https://api.upstox.com/v3/x", status, "error", {}, io.BytesIO(body))


@pytest.mark.parametrize(
    ("error", "named"),
    [
        (
            _provider_error(
                400,
                json.dumps(
                    {"status": "error", "errors": [{"errorCode": "UDAPI100011", "message": "x"}]}
                ).encode(),
            ),
            "UpstoxInvalidInstrumentKeyError",
        ),
        (_provider_error(403, b"error code: 1010"), "UpstoxAccessBlockedError"),
        (_provider_error(503, b""), "UpstoxProviderUnavailableError"),
    ],
    ids=["invalid-instrument", "cloudflare", "unavailable"],
)
def test_provider_failures_are_provider_and_append_nothing(
    evidence: Path, error: HTTPError, named: str
) -> None:
    evidence.write_text("", encoding="utf-8")
    fake = FakeUpstox(_normal_market(), candle_error=error)

    outcome, _, _ = _observe(evidence, fake=fake)

    assert outcome.code == ExitCode.PROVIDER
    assert named in outcome.err
    assert evidence.read_text(encoding="utf-8") == ""
    assert _TOKEN not in outcome.out + outcome.err


def test_an_expired_contract_is_provider(evidence: Path) -> None:
    fake = FakeUpstox(_normal_market(), master_has_contract=False)

    outcome, _, _ = _observe(evidence, fake=fake)

    assert outcome.code == ExitCode.PROVIDER
    assert "UpstoxInstrumentResolutionError" in outcome.err
    assert not evidence.exists()


def test_an_unusable_evidence_path_is_configuration(tmp_path: Path) -> None:
    outcome, fake, _ = _observe(tmp_path / "missing" / "evidence.jsonl")

    assert outcome.code == ExitCode.CONFIGURATION
    assert "cannot be used" in outcome.err
    assert fake.candle_requests == []


def test_a_truncated_evidence_file_is_state_and_untouched(evidence: Path) -> None:
    evidence.write_bytes(b'{"schema": "northstar.upstox')

    outcome, fake, _ = _observe(evidence)

    assert outcome.code == ExitCode.STATE
    assert "TruncatedUpstoxCandleEvidenceError" in outcome.err
    assert evidence.read_bytes() == b'{"schema": "northstar.upstox'
    assert fake.candle_requests == []


def test_the_token_is_redacted_even_from_an_error_carrying_it(evidence: Path) -> None:
    class Leaky:
        def observe(self, contract, trading_date):
            raise UpstoxMarketDataSourceError(f"boom {_TOKEN}")

    out, err = io.StringIO(), io.StringIO()
    code = main(
        [
            "finality-evidence",
            "observe",
            "--evidence",
            str(evidence),
            "--product",
            "NIFTY",
            "--exchange",
            "NSE",
            "--expiration",
            "2026-10-27",
            "--trading-date",
            _day(21),
        ],  # fmt: skip
        env={"UPSTOX_ANALYTICS_TOKEN": _TOKEN},
        stdout=out,
        stderr=err,
        database_runtime=_no_database,
        upstox_candle_evidence=lambda token, clock: Leaky(),
    )

    assert code == ExitCode.PROVIDER
    assert "boom [REDACTED]" in err.getvalue()
    assert _TOKEN not in out.getvalue() + err.getvalue()


# ---------------------------------------------------------------------------
# Production isolation
# ---------------------------------------------------------------------------


def test_only_the_token_is_read_from_the_environment(evidence: Path) -> None:
    env = TrackingEnv(
        {
            "UPSTOX_ANALYTICS_TOKEN": _TOKEN,
            "NORTHSTAR_DATABASE": "C:/never.sqlite3",
            "NORTHSTAR_FUTURES_DAILY_BAR_FINALITY": "operator-approved",
            "NORTHSTAR_FUTURES_FINAL_THROUGH": "2026-09-01",
            "NORTHSTAR_FUTURES_GO_LIVE": "2026-09-01",
        }
    )

    outcome, _, _ = _observe(evidence, env=env)

    assert outcome.code == ExitCode.SUCCESS
    assert env.read == ["UPSTOX_ANALYTICS_TOKEN"]


def test_the_canonical_database_is_untouched(tmp_path: Path) -> None:
    canonical = tmp_path / "northstar.sqlite3"
    build_database_runtime(canonical)
    before = canonical.read_bytes()

    outcome, _, _ = _observe(tmp_path / "evidence.jsonl")

    assert outcome.code == ExitCode.SUCCESS
    assert canonical.read_bytes() == before
    assert sorted(p.name for p in tmp_path.iterdir()) == ["evidence.jsonl", "northstar.sqlite3"]
    assert not any(p.name.endswith(".operations.lock") for p in tmp_path.iterdir())
