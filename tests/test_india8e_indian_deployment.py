"""INDIA-8E: the isolated Indian Futures deployment artifacts.

Inexpensive, deterministic facts about ``deploy/india`` -- compose, Caddy,
systemd, the example environment and the runbooks -- checked as text, as the
existing CME deployment facts are. Docker behaviour itself is manual
acceptance on the deployment host; nothing here starts a container.

The CME deployment in ``deploy/`` is reference evidence and must stay exactly
as it was: its project, services, images, volume and units are pinned too.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

from northstar_api.settings import (
    DAILY_BAR_FINALITY_VARIABLE,
    FINAL_THROUGH_VARIABLE,
    GO_LIVE_VARIABLE,
    MARKET_DATA_PROVIDER_VARIABLE,
    OPERATION_VARIABLES,
    FuturesDailyBarFinalityMode,
    load_dashboard_settings,
    load_operation_settings,
    load_session_operation_settings,
)

_API = Path(__file__).resolve().parents[1]
_DEPLOY = _API / "deploy"
_INDIA = _DEPLOY / "india"
_RUNBOOK = _API.parent / "northstar-docs" / "operations" / "Indian-Futures-Deployment-Runbook.md"
_TOKEN = "UPSTOX_ANALYTICS_TOKEN"
_CME_VOLUME = "northstar_northstar-data"
_SESSION_VARIABLES = (DAILY_BAR_FINALITY_VARIABLE, FINAL_THROUGH_VARIABLE, GO_LIVE_VARIABLE)
_DESTRUCTIVE = (
    re.compile(r"\bdown\b[^\n]*(\s-v\b|\s--volumes\b)"),
    re.compile(r"\bvolume\s+rm\b"),
    re.compile(r"\bvolume\s+prune\b"),
    re.compile(r"\brm\b[^\n]*\s-v\b"),
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _services(compose: str) -> dict[str, str]:
    """Split the compose file's services into their text blocks by indentation."""
    body = compose.split("\nservices:\n", 1)[1].split("\nvolumes:\n", 1)[0]
    blocks: dict[str, str] = {}
    name = None
    for line in body.splitlines():
        header = re.fullmatch(r"  ([a-z-]+):", line)
        if header:
            name = header.group(1)
            blocks[name] = ""
        elif name is not None:
            blocks[name] += line + "\n"
    return blocks


def _anchor(compose: str) -> str:
    return compose.split("x-futures-settings: &futures-settings\n", 1)[1].split("\n\n", 1)[0]


def _directives(text: str) -> str:
    """Return the text without its comment lines, which may name what they exclude."""
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def _project(compose: str) -> str:
    match = re.search(r"^name: (\S+)$", compose, re.MULTILINE)
    assert match is not None
    return match.group(1)


def _volume_names(compose: str) -> dict[str, str]:
    """Return each top-level volume key and the Docker volume name it creates."""
    block = compose.split("\nvolumes:\n", 1)[1]
    project = _project(compose)
    names: dict[str, str] = {}
    key = None
    for line in block.splitlines():
        header = re.fullmatch(r"  ([a-z-]+):", line)
        if header:
            key = header.group(1)
            names[key] = f"{project}_{key}"  # Compose's default naming
        explicit = re.fullmatch(r"    name: (\S+)", line)
        if explicit and key is not None:
            names[key] = explicit.group(1)
    return names


def _env_example(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in _read(path).splitlines():
        if line and not line.startswith("#"):
            key, _, value = line.partition("=")
            values[key] = value.strip("'")
    return values


def _code_blocks(markdown: str) -> list[str]:
    return re.findall(r"```[a-z]*\n(.*?)```", markdown, re.DOTALL)


@pytest.fixture(scope="module")
def india() -> str:
    return _directives(_read(_INDIA / "compose.yaml"))


@pytest.fixture(scope="module")
def cme() -> str:
    return _read(_DEPLOY / "compose.yaml")


# ---------------------------------------------------------------------------
# A-D. Project, volume, images, shared database
# ---------------------------------------------------------------------------


def test_the_indian_compose_project_is_distinct(india: str, cme: str) -> None:
    assert _project(india) == "northstar-india"
    assert _project(cme) == "northstar"
    assert set(_services(india)) == {"india-api", "india-operations", "india-web"}
    assert not set(_services(india)) & set(_services(cme))


def test_the_indian_volumes_are_distinct_and_never_the_cme_volume(india: str, cme: str) -> None:
    names = _volume_names(india)
    assert names == {
        "northstar-india-data": "northstar-india-data",
        "northstar-india-caddy-data": "northstar-india-caddy-data",
        "northstar-india-caddy-config": "northstar-india-caddy-config",
    }
    assert _volume_names(cme)["northstar-data"] == _CME_VOLUME
    assert not set(names.values()) & set(_volume_names(cme).values())
    for path in _INDIA.rglob("*"):
        if path.is_file() and path.suffix != ".md":
            text = _directives(_read(path))
            assert _CME_VOLUME not in text, path
            assert not re.search(r"^\s*- northstar-data:", text, re.MULTILINE), path


def test_the_indian_images_are_distinct(india: str, cme: str) -> None:
    services = _services(india)
    assert "image: northstar-api:india" in services["india-api"]
    assert "image: northstar-api:india" in services["india-operations"]
    assert "image: northstar-web:india" in services["india-web"]
    india_images = set(re.findall(r"image: (\S+)", india))
    cme_images = set(re.findall(r"image: (\S+)", cme))
    assert india_images == {"northstar-api:india", "northstar-web:india"}
    assert cme_images == {"northstar-api:local", "northstar-web:local"}


def test_api_and_operations_share_the_indian_database(india: str) -> None:
    services = _services(india)
    database = re.search(r"NORTHSTAR_DATABASE: \$\{NORTHSTAR_DATABASE:-([^}]+)\}", india)
    assert database is not None and database.group(1) == "/data/northstar.sqlite3"
    for name in ("india-api", "india-operations"):
        assert "- northstar-india-data:/data" in services[name]
        assert "<<: *futures-settings" in services[name]
    assert "northstar-india-data" not in services["india-web"]
    # Builds use this checkout's workspace root and the shared Dockerfiles.
    assert "context: ../../..\n      dockerfile: northstar-api/deploy/Dockerfile.api" in india


# ---------------------------------------------------------------------------
# E-F. Environment confinement, with the merged setting names
# ---------------------------------------------------------------------------


def test_the_api_gets_observability_settings_but_never_the_token(india: str) -> None:
    anchor = _anchor(india)
    for name in (*OPERATION_VARIABLES, *_SESSION_VARIABLES):
        assert re.search(rf"^  {name}: ", anchor, re.MULTILINE), name
    assert f"{DAILY_BAR_FINALITY_VARIABLE}: ${{{DAILY_BAR_FINALITY_VARIABLE}:-disabled}}" in anchor
    api = _services(india)["india-api"]
    assert "<<: *futures-settings" in api
    assert "NORTHSTAR_WEB_ORIGIN" in api
    assert _TOKEN not in anchor and _TOKEN not in api
    assert MARKET_DATA_PROVIDER_VARIABLE not in anchor and MARKET_DATA_PROVIDER_VARIABLE not in api
    assert "ports:" not in api


def test_only_the_operation_gets_the_token_and_the_upstox_provider(india: str) -> None:
    services = _services(india)
    operations = services["india-operations"]
    assert f"{_TOKEN}: ${{{_TOKEN}:-}}" in operations
    assert f"{MARKET_DATA_PROVIDER_VARIABLE}: upstox" in operations
    assert "<<: *futures-settings" in operations
    assert "command: [northstar, operations, daily]" in operations
    assert "DATABENTO" not in india
    assert india.count(_TOKEN) == 2  # the one key and its interpolation
    for name in ("india-api", "india-web"):
        assert _TOKEN not in services[name]


def test_the_site_is_loopback_only_beside_the_cme_ports(india: str) -> None:
    web = _services(india)["india-web"]
    published = re.findall(r'^      - "(\S+)"$', web.split("    ports:\n", 1)[1], re.MULTILINE)
    assert published == ["127.0.0.1:${NORTHSTAR_INDIA_WEB_PORT:-8081}:8080"]
    assert india.count("ports:") == 1 and "443" not in india
    assert "- ./Caddyfile:/etc/caddy/Caddyfile:ro" in web


def test_the_example_environment_matches_the_compose_file_and_the_code(india: str) -> None:
    example = _env_example(_INDIA / ".env.example")
    assert set(example) == {
        _TOKEN,
        "NORTHSTAR_BASIC_AUTH_USER",
        "NORTHSTAR_BASIC_AUTH_HASH",
        "NORTHSTAR_INDIA_WEB_PORT",
        "NORTHSTAR_WEB_ORIGIN",
        *OPERATION_VARIABLES,
        *_SESSION_VARIABLES,
    }
    assert set(re.findall(r"\$\{([A-Z_]+)", india)) <= set(example)

    values = {**example, "NORTHSTAR_BASIC_AUTH_HASH": ""}
    operation = load_operation_settings(values)
    contract = operation.contract
    assert (
        contract.product.product_code.value,
        contract.product.exchange_code.value,
        contract.expiration_date.value,
    ) == ("NIFTY", "NSE", "2026-10-27")
    session = load_session_operation_settings(values)
    assert session.finality_mode is FuturesDailyBarFinalityMode.DISABLED
    assert (session.final_through, session.go_live) == (None, None)
    assert load_dashboard_settings(values).operations == session


# ---------------------------------------------------------------------------
# G, N, O. No credentials, no time-based finality, no automatic rollover
# ---------------------------------------------------------------------------


def test_no_credential_is_tracked_in_the_indian_deployment() -> None:
    example = _env_example(_INDIA / ".env.example")
    assert example[_TOKEN] == ""
    assert example["NORTHSTAR_BASIC_AUTH_USER"] == ""
    assert example["NORTHSTAR_BASIC_AUTH_HASH"] == ""
    for path in _INDIA.rglob("*"):
        if path.is_file():
            text = _read(path)
            assert not re.search(r"eyJ[A-Za-z0-9_-]{10,}", text), path  # JWT
            assert not re.search(r"\$2[aby]\$\d\d\$", text), path  # bcrypt
            assert not re.search(rf"{_TOKEN}=\S", text), path


def test_no_finality_time_or_automatic_rollover_is_configured(india: str) -> None:
    example = _env_example(_INDIA / ".env.example")
    keys = set(example) | set(re.findall(r"^\s+([A-Z][A-Z_]+):", india, re.MULTILINE))
    assert not [key for key in keys if re.search(r"ROLLOVER|DELAY|CUTOFF|HOUR|TIME|_K$", key)]
    assert example[DAILY_BAR_FINALITY_VARIABLE] == "disabled"
    assert example[FINAL_THROUGH_VARIABLE] == "" and example[GO_LIVE_VARIABLE] == ""
    for text in (
        india,
        _read(_INDIA / ".env.example"),
        _read(_INDIA / "systemd" / "northstar-india-daily.service"),
    ):
        lowered = text.lower()
        for forbidden in ("21:00", "next morning", "minutes after", " ist "):
            assert forbidden not in lowered
    timer = _read(_INDIA / "systemd" / "northstar-india-daily.timer")
    assert "OnCalendar=hourly" in timer
    assert "not a finality rule" in timer


# ---------------------------------------------------------------------------
# H. The CME deployment is untouched
# ---------------------------------------------------------------------------


def test_the_cme_deployment_is_unchanged(cme: str) -> None:
    assert set(_services(cme)) == {"api", "operations", "web"}
    assert "- northstar-data:/data" in _services(cme)["api"]
    assert '"443:443"' in _services(cme)["web"]
    assert "DATABENTO_API_KEY" in _services(cme)["operations"]
    for path in (
        _DEPLOY / "compose.yaml",
        _DEPLOY / "Caddyfile",
        _DEPLOY / ".env.example",
        _DEPLOY / "systemd" / "northstar-daily.service",
        _DEPLOY / "systemd" / "northstar-daily.timer",
    ):
        assert "india" not in _read(path).lower(), path
    service = _read(_DEPLOY / "systemd" / "northstar-daily.service")
    assert "WorkingDirectory=/opt/northstar/northstar-api/deploy\n" in service
    assert "ExecStart=/usr/bin/docker compose run --rm --no-deps operations\n" in service
    assert "OnCalendar=*-*-* 23:30:00 UTC" in _read(_DEPLOY / "systemd" / "northstar-daily.timer")
    assert re.search(
        r"handle_path /api/\* \{\s*reverse_proxy api:8000\s*\}", _read(_DEPLOY / "Caddyfile")
    )


# ---------------------------------------------------------------------------
# I. systemd units address only the Indian project and directory
# ---------------------------------------------------------------------------


def test_the_indian_units_address_only_the_indian_stack() -> None:
    units = sorted(path.name for path in (_INDIA / "systemd").iterdir())
    assert units == ["northstar-india-daily.service", "northstar-india-daily.timer"]
    service = _read(_INDIA / "systemd" / "northstar-india-daily.service")
    assert "WorkingDirectory=/opt/northstar-india/northstar-api/deploy/india\n" in service
    assert (
        "ExecStart=/usr/bin/docker compose --project-name northstar-india "
        "run --rm --no-deps india-operations\n"
    ) in service
    timer = _read(_INDIA / "systemd" / "northstar-india-daily.timer")
    for unit in (service, timer):
        assert "northstar-daily" not in unit
        assert "/opt/northstar/" not in unit
    # A timer without Unit= activates the service of the same name.
    assert "Unit=" not in timer


# ---------------------------------------------------------------------------
# J-K. Runbooks never use destructive volume commands
# ---------------------------------------------------------------------------


def _assert_no_destructive_command(text: str, where: str) -> None:
    for pattern in _DESTRUCTIVE:
        assert not pattern.search(text), f"{where}: {pattern.pattern}"


def test_the_indian_artifacts_hold_no_destructive_volume_command() -> None:
    for path in _INDIA.rglob("*"):
        if path.is_file():
            _assert_no_destructive_command(_read(path), str(path))


def test_the_runbook_commands_are_never_destructive() -> None:
    if not _RUNBOOK.is_file():
        pytest.skip("northstar-docs is not checked out beside northstar-api")
    runbook = _read(_RUNBOOK)
    blocks = _code_blocks(runbook)
    assert len(blocks) > 10
    for block in blocks:
        _assert_no_destructive_command(block, "runbook command")
        assert "northstar_northstar-data" not in block
        assert "/opt/northstar/" not in block  # never operates the CME checkout
    assert "docker compose up -d india-api" in runbook
    assert "2026-11-23" in runbook and "Muhurat" in runbook


# ---------------------------------------------------------------------------
# L. The Indian site proxies only the read-only Futures surface
# ---------------------------------------------------------------------------


def test_the_indian_caddyfile_proxies_only_read_only_routes() -> None:
    caddy = _read(_INDIA / "Caddyfile")
    directives = _directives(caddy)
    assert re.search(r"^:8080 \{$", directives, re.MULTILINE)
    assert "basic_auth" in directives
    assert directives.count("reverse_proxy") == 1
    assert re.search(
        r"handle_path /api/\* \{\s*"
        r"@read \{\s*method GET HEAD\s*path /health /futures/dashboard\s*\}\s*"
        r"handle @read \{\s*reverse_proxy india-api:8000\s*\}\s*"
        r"handle \{\s*respond 404\s*\}\s*\}",
        directives,
    )
    for route in ("/analyze", "/watchlist", "POST", "api:8000\n"):
        assert route not in directives.replace("india-api:8000", "")


# ---------------------------------------------------------------------------
# M. Secrets ignored, templates tracked
# ---------------------------------------------------------------------------


@pytest.mark.skipif(shutil.which("git") is None, reason="git is not available")
@pytest.mark.parametrize(
    ("path", "ignored"),
    [
        (".env", True),
        ("deploy/.env", True),
        ("deploy/india/.env", True),
        (".env.example", False),
        ("deploy/.env.example", False),
        ("deploy/india/.env.example", False),
    ],
)
def test_real_environment_files_are_ignored_and_examples_are_not(path: str, ignored: bool) -> None:
    result = subprocess.run(  # noqa: S603 - fixed arguments
        ["git", "check-ignore", "-q", "--no-index", path],  # noqa: S607
        cwd=_API,
        check=False,
    )
    assert (result.returncode == 0) is ignored
