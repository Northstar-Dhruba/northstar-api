"""Isolated real-Docker acceptance of the India Windows operations wrapper. OPT-IN ONLY.

    NORTHSTAR_DOCKER_ACCEPTANCE=1  +  Windows  +  Windows PowerShell  +  a Linux Docker engine

Without the explicit opt-in every test here is skipped at collection: nothing
calls Docker on import, during collection, in an ordinary pytest run or in CI.
The fixture checks the opt-in again itself, and every Docker call refuses to
run without it, so disabling pytest's skip handling cannot start a session.

Isolation boundary
------------------
The production compose.yaml pins its SQLite volume by explicit name
(northstar-india-data), so no copy of it, under any project name, is isolated.
This module therefore never reads, copies or references it. Each session:

- generates its own compose.yaml from scratch (JSON, which Compose reads as
  YAML) with literal values only, in fresh directories under the system temp
  directory, outside the checkout and the production log directory;
- uses one unique project, northstar-india-acceptance-<id>, one explicitly
  named local volume <project>-data without driver options and one explicitly
  named INTERNAL bridge network <project>-net, so no container can reach a
  provider;
- runs the existing northstar-api:india image with pull_policy never; it never
  builds, pulls, retags or removes an image;
- passes no provider token, finality disabled, a valid go-live and synthetic
  identities, and scrubs the subprocess environment to an allowlist, so no
  UPSTOX_*, NORTHSTAR_*, COMPOSE_* or DOCKER_* value is inherited;
- passes -DeploymentDirectory, -LogDirectory and -ProjectName to the wrapper
  on every invocation (the wrapper refuses an acceptance run otherwise);
- never attaches any container to a production volume.

Safety gate, before any container runs
---------------------------------------
The engine and context are checked; the image must already exist; every
rendered project is inspected with `docker compose config --format json` and
checked against an allowlist (only the session volume, the internal session
network, the approved image with pull_policy never and synthetic NORTHSTAR_*
settings; no other setting in force); no session resource may already exist;
and the production containers, volume identity, image and last-run.json are
recorded. Any failure aborts the module before an operations container runs.
The session's exact names are then printed and written to session.json.

Production non-interference
---------------------------
Production is compared with its recorded baseline after every destructive
scenario and at the end, and a read-only `docker events` stream watches the
production volume for the whole session: any create, mount, unmount or
destroy event on it fails the run, as does the stream ending early or never
seeing the session volume's own mounts. Neither is proof that the production
SQLite content is unchanged -- that cannot be read without touching
production -- they prove that no container attached to the volume and that
production's containers, volume and image were not recreated.

Cleanup
-------
Every session container is removed by immutable ID after its Compose project
label is verified, then confirmed gone; every step is attempted even when
another fails, and only a confirmed "not found" counts as already absent.
Never a prune, never production's `compose down`, never a prefix match. When
a test or a cleanup step fails, containers are still removed but the session
volume, network and directories are kept as evidence, their exact names are
printed, and every cleanup error is reported after the production check.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import pytest


def _opted_in() -> bool:
    return os.environ.get("NORTHSTAR_DOCKER_ACCEPTANCE") == "1"


_POWERSHELL = shutil.which("powershell.exe") if sys.platform == "win32" else None
pytestmark = pytest.mark.skipif(
    not (_opted_in() and sys.platform == "win32" and _POWERSHELL),
    reason="real-Docker acceptance is opt-in: set NORTHSTAR_DOCKER_ACCEPTANCE=1 on Windows",
)

_API = Path(__file__).resolve().parents[1]
_CHECKOUT = _API.parent
_WRAPPER = _API / "deploy" / "india" / "windows" / "Invoke-NorthstarIndiaOperations.ps1"
_IMAGE = "northstar-api:india"
_SERVICE = "india-operations"
_PRODUCTION_PROJECT = "northstar-india"
_PRODUCTION_VOLUME = "northstar-india-data"
_PRODUCTION_NAMES = {
    "northstar-india-data", "northstar-india-caddy-data", "northstar-india-caddy-config",
    "northstar_northstar-data", "northstar-india_default", "northstar_default",
}  # fmt: skip
_PREFIX = "northstar-india-acceptance-"
_ALLOWED_CONTEXTS = {"desktop-linux", "default"}
_LABEL = "com.docker.compose.project"
_NAME = re.compile(r"[a-z0-9][a-z0-9_.-]+")
# Docker's not-found answers for containers, volumes and networks; anything else is an error.
_NOT_FOUND = re.compile(
    r"(?i)\bno such (container|object|volume|network)\b|\bnetwork \S+ not found\b"
)
_ENV_ALLOWLIST = {
    "SYSTEMROOT", "SYSTEMDRIVE", "WINDIR", "COMSPEC", "PATH", "PATHEXT", "TEMP", "TMP",
    "USERPROFILE", "HOMEDRIVE", "HOMEPATH", "LOCALAPPDATA", "APPDATA", "PROGRAMDATA",
    "PROGRAMFILES", "PROGRAMFILES(X86)", "PROGRAMW6432", "COMMONPROGRAMFILES",
    "USERNAME", "USERDOMAIN", "COMPUTERNAME", "NUMBER_OF_PROCESSORS",
    "PROCESSOR_ARCHITECTURE", "OS", "PSMODULEPATH",
}  # fmt: skip
_TRADING_TABLES = (
    "futures_forward_research_records", "futures_paper_orders", "futures_paper_fills",
    "futures_ohlcv", "futures_contract_economics",
)  # fmt: skip
_SYNTHETIC_ENV_KEYS = {
    "NORTHSTAR_DATABASE", "NORTHSTAR_FUTURES_PRODUCT", "NORTHSTAR_FUTURES_EXCHANGE",
    "NORTHSTAR_FUTURES_EXPIRATION", "NORTHSTAR_STRATEGY", "NORTHSTAR_PORTFOLIO",
    "NORTHSTAR_TARGET", "NORTHSTAR_FUTURES_MARKET_DATA_PROVIDER",
    "NORTHSTAR_FUTURES_DAILY_BAR_FINALITY", "NORTHSTAR_FUTURES_GO_LIVE",
    "NORTHSTAR_ACCEPTANCE_MARKER",
}  # fmt: skip
# Allowlists: any other setting in force in the rendered model is refused.
_TOP_LEVEL_KEYS = {"name", "services", "volumes", "networks"}
_VOLUME_KEYS = {"name", "driver"}
_NETWORK_KEYS = {"name", "driver", "internal"}
_SERVICE_KEYS = {
    "image", "pull_policy", "command", "entrypoint", "environment", "volumes", "networks",
}  # fmt: skip
_MOUNT_KEYS = {"type", "source", "target", "read_only"}
_LOCK_HOLDER = (
    "import time\n"
    "from pathlib import Path\n"
    "from northstar_api.operations_lock import DatabaseOperationsLock\n"
    "with DatabaseOperationsLock(Path('/data/northstar.sqlite3')):\n"
    "    print('LOCKED', flush=True)\n"
    "    time.sleep(300)\n"
)
_INSPECT_DATABASE = (
    "import hashlib, json, sqlite3\n"
    "c = sqlite3.connect('file:/data/northstar.sqlite3?mode=ro', uri=True)\n"
    "q = \"SELECT name FROM sqlite_master WHERE type='table'\"\n"
    "tables = sorted(r[0] for r in c.execute(q))\n"
    "rows = {t: sorted(map(repr, c.execute('SELECT * FROM ' + t).fetchall())) for t in tables}\n"
    "print(json.dumps({'tables': tables, 'counts': {t: len(v) for t, v in rows.items()},\n"
    "  'digest': hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest(),\n"
    "  'integrity': c.execute('PRAGMA integrity_check').fetchone()[0]}))\n"
)


# ---------------------------------------------------------------------------
# Session: names, environment, rendering
# ---------------------------------------------------------------------------


@dataclass
class Session:
    sid: str
    project: str
    volume: str
    network: str
    root: Path
    env: dict[str, str]
    image_id: str = ""
    started: float = 0.0
    baseline: dict = field(default_factory=dict)
    containers: set[str] = field(default_factory=set)
    deployments: dict[str, Path] = field(default_factory=dict)
    evidence: list[dict] = field(default_factory=list)
    database: dict = field(default_factory=dict)
    events: subprocess.Popen | None = None
    events_path: Path | None = None

    @property
    def holder(self) -> str:
        return f"{self.project}-lock-holder"


def _require_opt_in() -> None:
    """Checked at setup too: the skip mark is not the only guard."""
    if not _opted_in():
        pytest.skip("real-Docker acceptance needs NORTHSTAR_DOCKER_ACCEPTANCE=1")
    if sys.platform != "win32" or not _POWERSHELL:
        pytest.skip("real-Docker acceptance runs on Windows with Windows PowerShell only")


def _scrubbed_environment() -> dict[str, str]:
    env = {key: value for key, value in os.environ.items() if key.upper() in _ENV_ALLOWLIST}
    for key in env:
        assert not re.match(r"(UPSTOX|NORTHSTAR|COMPOSE|DOCKER)_", key.upper()), key
    return env


def _operations_service(session: Session, command: list[str], **environment: str) -> dict:
    return {
        "image": _IMAGE,
        "pull_policy": "never",
        "command": command,
        "environment": {
            "NORTHSTAR_DATABASE": "/data/northstar.sqlite3",
            "NORTHSTAR_FUTURES_PRODUCT": "NIFTY",
            "NORTHSTAR_FUTURES_EXCHANGE": "NSE",
            "NORTHSTAR_FUTURES_EXPIRATION": "2026-10-27",
            "NORTHSTAR_STRATEGY": f"acceptance-strategy-{session.sid}",
            "NORTHSTAR_PORTFOLIO": f"acceptance-portfolio-{session.sid}",
            "NORTHSTAR_TARGET": "1",
            "NORTHSTAR_FUTURES_MARKET_DATA_PROVIDER": "upstox",
            "NORTHSTAR_FUTURES_DAILY_BAR_FINALITY": "disabled",
            "NORTHSTAR_FUTURES_GO_LIVE": "2026-10-26",
            **environment,
        },
        "volumes": [{"type": "volume", "source": "acceptance-data", "target": "/data"}],
        "networks": ["acceptance"],
    }


def _compose(session: Session, services: dict) -> str:
    """A compose.yaml written from scratch; JSON is a subset of YAML."""
    return json.dumps(
        {
            "name": session.project,
            "services": services,
            "volumes": {"acceptance-data": {"name": session.volume}},
            "networks": {"acceptance": {"name": session.network, "internal": True}},
        },
        indent=2,
    )


_OPERATIONS = ["northstar", "operations", "daily"]
_SYNTHETIC_DOTENV = (
    "# Synthetic acceptance settings: no secrets, never production.\n"
    "NORTHSTAR_FUTURES_DAILY_BAR_FINALITY=disabled\n"
    "NORTHSTAR_FUTURES_FINAL_THROUGH=\n"
)


def _render(session: Session) -> None:
    sleep = lambda seconds: ["python", "-c", f"import time; time.sleep({seconds})"]  # noqa: E731
    projects = {
        "normal": {_SERVICE: _operations_service(session, _OPERATIONS)},
        "sleeper": {_SERVICE: _operations_service(session, sleep(600))},
        "interrupt": {_SERVICE: _operations_service(session, sleep(45))},
        "lock": {
            _SERVICE: _operations_service(session, _OPERATIONS),
            "lock-holder": _operations_service(session, ["python", "-c", _LOCK_HOLDER]),
        },
        "crlf": {
            _SERVICE: _operations_service(
                session,
                _OPERATIONS,
                NORTHSTAR_ACCEPTANCE_MARKER="${NORTHSTAR_ACCEPTANCE_MARKER:?marker missing}",
            )
        },  # fmt: skip
        "invalid": {
            _SERVICE: _operations_service(
                session,
                _OPERATIONS,
                NORTHSTAR_ACCEPTANCE_MARKER="${NORTHSTAR_ACCEPTANCE_REQUIRED:?required}",
            )
        },  # fmt: skip
        "missing-env": {_SERVICE: _operations_service(session, _OPERATIONS)},
    }
    for name, services in projects.items():
        directory = session.root / f"deploy ({name})"
        directory.mkdir()
        (directory / "compose.yaml").write_text(_compose(session, services), encoding="utf-8")
        if name == "crlf":
            (directory / ".env").write_bytes(
                b"NORTHSTAR_FUTURES_DAILY_BAR_FINALITY=disabled\r\n"
                b"NORTHSTAR_FUTURES_FINAL_THROUGH=\r\n"
                b"NORTHSTAR_ACCEPTANCE_MARKER=crlf-ok\r\n"
            )
        elif name != "missing-env":
            (directory / ".env").write_text(_SYNTHETIC_DOTENV, encoding="utf-8")
        session.deployments[name] = directory


# ---------------------------------------------------------------------------
# Docker helpers: every call is session-scoped or a read-only production probe
# ---------------------------------------------------------------------------


def _refuse_without_opt_in() -> None:
    if not _opted_in():
        raise RuntimeError("refusing to call Docker without NORTHSTAR_DOCKER_ACCEPTANCE=1")


def _docker(session: Session, *args: str, timeout: float = 120, env=None):
    _refuse_without_opt_in()
    return subprocess.run(  # noqa: S603 - fixed docker arguments
        ["docker", *args],  # noqa: S607
        env=env or session.env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=timeout,
        check=False,
    )


def _first_line(text: str) -> str:
    return next((line.strip() for line in text.splitlines() if line.strip()), "")


def _compose_args(session: Session, name: str) -> list[str]:
    directory = session.deployments[name]
    return [
        "compose", "--project-directory", str(directory),
        "--file", str(directory / "compose.yaml"), "--project-name", session.project,
    ]  # fmt: skip


def _inspect(session: Session, kind: str, reference: str) -> dict | None:
    """The inspect record; None only for a confirmed not-found, an error otherwise."""
    result = _docker(session, kind, "inspect", reference)
    if result.returncode == 0:
        records = json.loads(result.stdout)
        assert isinstance(records, list) and len(records) == 1, f"{kind} {reference}: ambiguous"
        return records[0]
    if _NOT_FOUND.search(result.stderr) and result.stdout.strip() in ("", "[]"):
        return None
    error = _first_line(result.stderr)
    raise AssertionError(f"{kind} inspect {reference} failed (exit {result.returncode}): {error}")


def _labels(kind: str, record: dict) -> dict:
    labels = (
        (record.get("Config") or {}).get("Labels") if kind == "container" else record.get("Labels")
    )
    return labels or {}


def _owner(session: Session, kind: str, reference: str) -> str | None:
    """The Compose project label, "" when unlabelled, None when confirmed absent."""
    record = _inspect(session, kind, reference)
    return None if record is None else _labels(kind, record).get(_LABEL, "")


def _container_state(session: Session, reference: str) -> dict | None:
    record = _inspect(session, "container", reference)
    if record is None:
        return None
    state = record.get("State") or {}
    return {
        "id": record["Id"],
        "name": str(record.get("Name", "")).lstrip("/"),
        "owner": _labels("container", record).get(_LABEL, ""),
        "status": state.get("Status"),
        "running": state.get("Running") is True,
        "exit_code": state.get("ExitCode"),
    }


def _container_running(session: Session, reference: str) -> bool:
    state = _container_state(session, reference)
    return state is not None and state["running"]


def _container_exists(session: Session, reference: str) -> bool:
    return _container_state(session, reference) is not None


def _session_containers(session: Session) -> list[tuple[str, str]]:
    """(full ID, name) of every container carrying exactly this session's project label."""
    label = f"label={_LABEL}={session.project}"
    result = _docker(session, "ps", "-a", "--no-trunc", "--filter", label,
                     "--format", "{{.ID}} {{.Names}}")  # fmt: skip
    assert result.returncode == 0, f"session containers could not be listed: {result.stderr}"
    pairs = []
    for line in result.stdout.splitlines():
        identifier, _, name = line.strip().partition(" ")
        if identifier:
            pairs.append((identifier, name))
    return pairs


def _production_snapshot(session: Session) -> dict:
    """Read-only identity of production: containers, volume, image and status file."""
    listed = _docker(session, "ps", "-a", "--filter", f"label={_LABEL}={_PRODUCTION_PROJECT}",
                     "--format", "{{.ID}}")  # fmt: skip
    assert listed.returncode == 0, "production containers could not be listed"
    containers = {}
    for identifier in sorted(listed.stdout.split()):
        details = _docker(
            session, "inspect", "--format",
            "{{.Id}}|{{.Name}}|{{.Created}}|{{.State.StartedAt}}|{{.State.Status}}|"
            "{{.RestartCount}}|{{.Image}}",
            identifier,
        )  # fmt: skip
        assert details.returncode == 0, f"production container {identifier} could not be read"
        containers[identifier] = details.stdout.strip()
    volume_format = "{{.Name}}|{{.CreatedAt}}|{{.Mountpoint}}|{{.Driver}}"
    volume = _docker(session, "volume", "inspect", "--format", volume_format, _PRODUCTION_VOLUME)
    if volume.returncode != 0:
        assert _NOT_FOUND.search(volume.stderr), "the production volume could not be inspected"
    image = _docker(session, "image", "inspect", "--format", "{{.Id}}", _IMAGE)
    status = Path(os.environ["LOCALAPPDATA"]) / "Northstar" / "india-operations" / "last-run.json"
    status_hash = hashlib.sha256(status.read_bytes()).hexdigest() if status.exists() else None
    return {
        "containers": containers,
        "volume": volume.stdout.strip() if volume.returncode == 0 else "<absent>",
        "image": image.stdout.strip() if image.returncode == 0 else "<absent>",
        "status": status_hash or "<absent>",
    }


# ---------------------------------------------------------------------------
# Production volume events: a read-only stream for the whole session
# ---------------------------------------------------------------------------


def _start_volume_events(session: Session) -> None:
    """Stream volume events for the production volume and, as a canary, the session's.

    Repeated filters of one kind are OR'ed by Docker. A live stream is used, not
    a later history query, because the engine keeps only a short event history.
    """
    _refuse_without_opt_in()
    session.events_path = session.root / "volume-events.jsonl"
    with (
        open(session.events_path, "wb") as stdout,
        open(session.root / "volume-events.stderr.txt", "wb") as stderr,
    ):
        session.events = subprocess.Popen(  # noqa: S603
            ["docker", "events", "--since", f"{int(session.started) - 2}",  # noqa: S607
             "--filter", "type=volume", "--filter", f"volume={_PRODUCTION_VOLUME}",
             "--filter", f"volume={session.volume}", "--format", "{{json .}}"],
            env=session.env, stdout=stdout, stderr=stderr,
        )  # fmt: skip
    time.sleep(2)
    assert session.events.poll() is None, (
        "docker events could not be started: production non-interference cannot be verified"
    )


def _volume_events(session: Session) -> list[dict]:
    assert session.events is not None and session.events_path is not None, "no event stream"
    code = session.events.poll()
    assert code is None, (
        f"the docker events stream ended (exit {code}): "
        "production non-interference cannot be verified"
    )
    text = session.events_path.read_text(encoding="utf-8", errors="replace")
    return [json.loads(line) for line in text.split("\n")[:-1] if line.strip()]


def _actor(event: dict) -> dict:
    return event.get("Actor") or {}


def _stop_volume_events(session: Session) -> None:
    if session.events is not None and session.events.poll() is None:
        session.events.terminate()  # this module's own child process only
        with contextlib.suppress(subprocess.TimeoutExpired):
            session.events.wait(timeout=10)


def _assert_production_unchanged(session: Session) -> None:
    problems = []
    snapshot = _production_snapshot(session)
    if snapshot != session.baseline:
        problems.append(f"identity before {session.baseline}, after {snapshot}")
    touched = [e for e in _volume_events(session) if _actor(e).get("ID") == _PRODUCTION_VOLUME]
    if touched:
        problems.append("production volume events: " + "; ".join(
            f"{e.get('Action')} by container "
            f"{str((_actor(e).get('Attributes') or {}).get('container', '?'))[:12]}"
            for e in touched
        ))  # fmt: skip
    assert not problems, "PRODUCTION CHANGED during acceptance: " + " | ".join(problems)


def _final_production_check(session: Session) -> None:
    try:
        _assert_production_unchanged(session)
        if session.database:  # scenario B mounted the session volume: the stream must show it
            mounts = [
                e for e in _volume_events(session)
                if _actor(e).get("ID") == session.volume and e.get("Action") == "mount"
            ]  # fmt: skip
            assert mounts, (
                "the event stream saw no session volume mount although scenario B made one: "
                "production non-interference cannot be verified"
            )
    finally:
        _stop_volume_events(session)


# ---------------------------------------------------------------------------
# Safety gate
# ---------------------------------------------------------------------------


def _in_force(value) -> bool:
    """Compose renders a setting that is not in force as absent, null, false or empty."""
    if value is None or value is False:
        return False
    if isinstance(value, str | list | dict):
        return len(value) > 0
    return True


def _model_problems(session: Session, model: dict, services: set[str]) -> list[str]:
    """Everything that makes a rendered model not exactly session-owned and isolated.

    A session-prefixed name is not enough: a local volume can bind any path,
    production's data included, through driver options, so the backing
    configuration of every volume and network is checked as well.
    """
    problems: list[str] = []

    def check(condition: bool, message: str) -> None:
        if not condition:
            problems.append(message)

    try:
        check(model.get("name") == session.project, "the rendered project is not the session's")
        check(session.project.startswith(_PREFIX) and session.project != _PRODUCTION_PROJECT,
              "the session project name is not an acceptance name")  # fmt: skip
        for key, value in model.items():
            check(key in _TOP_LEVEL_KEYS or not _in_force(value), f"top-level {key} is in force")
        volumes = model.get("volumes") or {}
        networks = model.get("networks") or {}
        check(set(volumes) == {"acceptance-data"}, "volumes are not exactly the session volume")
        for volume in volumes.values():
            check(volume.get("name") == session.volume, "volume name is not the session's")
            check(volume.get("driver") in (None, "local"), "volume driver is not local")
            check(not _in_force(volume.get("driver_opts")), "volume sets driver_opts")
            check(not _in_force(volume.get("external")), "volume is external")
            for key, value in volume.items():
                check(key in _VOLUME_KEYS or not _in_force(value), f"volume sets {key}")
        check(set(networks) == {"acceptance"}, "networks are not exactly the session network")
        for network in networks.values():
            check(network.get("name") == session.network, "network name is not the session's")
            check(network.get("internal") is True, "network is not internal")
            check(network.get("driver") in (None, "bridge"), "network driver is not bridge")
            check(not _in_force(network.get("driver_opts")), "network sets driver_opts")
            check(not _in_force(network.get("external")), "network is external")
            for key, value in network.items():
                check(key in _NETWORK_KEYS or not _in_force(value), f"network sets {key}")
        check(not {session.volume, session.network} & _PRODUCTION_NAMES, "a production name")
        check(set(model.get("services") or {}) == services, "services are not the expected ones")
        for name, service in (model.get("services") or {}).items():
            for key, value in service.items():
                check(key in _SERVICE_KEYS or not _in_force(value), f"{name} sets {key}")
            check(service.get("image") == _IMAGE, f"{name} does not use {_IMAGE}")
            check(service.get("pull_policy") == "never", f"{name} is not pull_policy never")
            mounts = service.get("volumes") or []
            check([(m.get("type"), m.get("source"), m.get("target")) for m in mounts]
                  == [("volume", "acceptance-data", "/data")], f"{name} mounts")  # fmt: skip
            for mount in mounts:
                for key, value in mount.items():
                    check(key in _MOUNT_KEYS or not _in_force(value), f"{name} mount sets {key}")
            check(not [m for m in mounts if "docker.sock" in json.dumps(m)], f"{name} socket")
            attached = service.get("networks") or {}
            check(isinstance(attached, dict) and set(attached) == {"acceptance"},
                  f"{name} is not attached to exactly the session network")  # fmt: skip
            if isinstance(attached, dict):
                check(not any(_in_force(v) for v in attached.values()), f"{name} attachment")
            environment = service.get("environment") or {}
            check(
                set(environment) <= _SYNTHETIC_ENV_KEYS, f"{name} passes a non-synthetic variable"
            )
            check(environment.get("NORTHSTAR_DATABASE") == "/data/northstar.sqlite3", f"{name} db")
            check(environment.get("NORTHSTAR_FUTURES_DAILY_BAR_FINALITY") == "disabled",
                  f"{name} finality is not disabled")  # fmt: skip
            # The provider's name is a setting; any other value naming it could be a credential.
            provider = "NORTHSTAR_FUTURES_MARKET_DATA_PROVIDER"
            check(not [k for k, v in environment.items()
                       if k != provider and "UPSTOX" in str(v).upper()],
                  f"{name} passes a provider value")  # fmt: skip
            check(environment.get("NORTHSTAR_FUTURES_MARKET_DATA_PROVIDER") == "upstox",
                  f"{name} provider is not the synthetic setting")  # fmt: skip
    except (AttributeError, TypeError) as error:
        problems.append(f"the rendered model has an unexpected shape ({type(error).__name__})")
    return problems


def _verify_model(session: Session, model: dict, services: set[str]) -> None:
    """Fail closed unless the rendered model is exactly session-owned and isolated."""
    problems = _model_problems(session, model, services)
    assert not problems, f"rendered model refused: {problems}"


def _gate(session: Session) -> None:
    session.started = time.time()
    context = _docker(session, "context", "show")
    assert context.returncode == 0 and context.stdout.strip() in _ALLOWED_CONTEXTS, (
        "the docker CLI is not on the local Docker Desktop context"
    )
    engine = _docker(session, "info", "--format", "{{.OSType}}", timeout=60)
    assert engine.returncode == 0 and engine.stdout.strip() == "linux", "no Linux engine"
    image = _docker(session, "image", "inspect", "--format", "{{.Id}}", _IMAGE)
    assert image.returncode == 0, f"{_IMAGE} is not built; this harness never builds or pulls"
    session.image_id = image.stdout.strip()

    expected = {"lock": {_SERVICE, "lock-holder"}}
    for name in session.deployments:
        rendered = _docker(session, *_compose_args(session, name), "config", "--format", "json")
        if name == "invalid":
            assert rendered.returncode != 0, "the invalid scenario unexpectedly validated"
            continue
        assert rendered.returncode == 0, f"{name}: the rendered model could not be inspected"
        model = json.loads(rendered.stdout)
        _verify_model(session, model, expected.get(name, {_SERVICE}))
        if name == "crlf":
            marker = model["services"][_SERVICE]["environment"]["NORTHSTAR_ACCEPTANCE_MARKER"]
            assert marker == "crlf-ok", "CRLF .env value was not read cleanly"

    assert _inspect(session, "volume", session.volume) is None, (
        "the session volume already exists; refusing to adopt it"
    )
    assert _inspect(session, "network", session.network) is None, (
        "the session network already exists; refusing to adopt it"
    )
    assert _session_containers(session) == [], "session containers already exist"
    assert not _container_exists(session, session.holder)
    session.baseline = _production_snapshot(session)


# ---------------------------------------------------------------------------
# Cleanup: exact, ID- and label-verified session resources only
# ---------------------------------------------------------------------------


def _stop_cli_processes(session: Session, container: str) -> None:
    """End docker CLI processes left by an interrupted wrapper.

    Only docker.exe / docker-compose.exe whose command line names both this
    exact container and this session's project.
    """
    assert _NAME.fullmatch(container) and _NAME.fullmatch(session.project)
    script = (
        "$ErrorActionPreference = 'Stop'; "
        "Get-CimInstance Win32_Process | Where-Object { "
        "($_.Name -eq 'docker.exe' -or $_.Name -eq 'docker-compose.exe') -and "
        f"$_.CommandLine -like '*{container}*' -and "
        f"$_.CommandLine -like '*{session.project}*' }} | "
        "ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"
    )
    result = subprocess.run(  # noqa: S603
        [_POWERSHELL, "-NoProfile", "-NonInteractive", "-Command", script],
        env=session.env, capture_output=True, text=True, timeout=60, check=False,
    )  # fmt: skip
    assert result.returncode == 0, f"docker CLI processes for {container} could not be stopped"


def _remove_container(session: Session, reference: str) -> None:
    """Remove one session container by immutable ID, after verifying its label."""
    record = _inspect(session, "container", reference)
    if record is None:
        return
    identifier = record["Id"]
    owner = _labels("container", record).get(_LABEL, "")
    assert owner == session.project, f"refusing to remove {reference}: owned by {owner!r}"
    result = _docker(session, "rm", "--force", identifier)
    deadline = time.monotonic() + 30
    while _inspect(session, "container", identifier) is not None:
        assert time.monotonic() < deadline, (
            f"container {reference} ({identifier[:12]}) still exists after rm "
            f"(exit {result.returncode}: {_first_line(result.stderr)})"
        )
        time.sleep(0.5)
    tolerated = _NOT_FOUND.search(result.stderr) or "already in progress" in result.stderr
    assert result.returncode == 0 or tolerated, (
        f"rm of container {reference} exited {result.returncode}: {_first_line(result.stderr)}"
    )


def _remove_session_object(session: Session, kind: str, name: str) -> None:
    """Remove the session network or volume, after verifying its exact name and label."""
    record = _inspect(session, kind, name)
    if record is None:
        return
    owner = (record.get("Labels") or {}).get(_LABEL, "")
    assert record.get("Name") == name and owner == session.project, (
        f"refusing to remove {kind} {name}: owned by {owner!r}"
    )
    reference = record["Id"] if kind == "network" else name
    result = _docker(session, kind, "rm", reference)
    assert result.returncode == 0, (
        f"{kind} rm {name} exited {result.returncode}: {_first_line(result.stderr)}"
    )
    assert _inspect(session, kind, reference) is None, f"{kind} {name} still exists after rm"


def _attempt(errors: list[str], step: str, action, *args) -> None:
    """Run one cleanup step; record its failure instead of abandoning the others."""
    try:
        action(*args)
    except Exception as error:  # noqa: BLE001 - every step is attempted and every error reported
        errors.append(f"{step}: {type(error).__name__}: {error}")


def _remove_session_containers(session: Session) -> list[str]:
    errors: list[str] = []
    listed: list[tuple[str, str]] = []
    try:
        listed = _session_containers(session)
    except Exception as error:  # noqa: BLE001
        errors.append(f"listing session containers: {error}")
    for name in sorted(session.containers | {session.holder} | {n for _, n in listed if n}):
        _attempt(errors, f"stopping CLI processes for {name}", _stop_cli_processes, session, name)
        _attempt(errors, f"removing container {name}", _remove_container, session, name)
    for identifier, name in listed:  # by immutable ID too, in case a name lookup failed
        _attempt(errors, f"removing container {name} ({identifier[:12]})",
                 _remove_container, session, identifier)  # fmt: skip
    try:
        remaining = _session_containers(session)
        if remaining:
            errors.append(f"session containers remain: {remaining}")
    except Exception as error:  # noqa: BLE001
        errors.append(f"verifying container cleanup: {error}")
    return errors


def _remaining(session: Session) -> list[str]:
    notes = []
    for kind, name in (("volume", session.volume), ("network", session.network)):
        try:
            if _inspect(session, kind, name) is not None:
                notes.append(f"kept {kind}: {name}")
        except Exception as error:  # noqa: BLE001
            notes.append(f"{kind} {name}: state unknown ({error})")
    notes.append(f"kept directory: {session.root}")
    return notes


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


def _announce(request: pytest.FixtureRequest, lines: list[str]) -> None:
    """Write to the terminal now, past pytest's output capture."""
    capture = request.config.pluginmanager.getplugin("capturemanager")
    context = capture.global_and_fixture_disabled() if capture else contextlib.nullcontext()
    with context:
        for line in lines:
            print(line, flush=True)  # noqa: T201


def _session_lines(session: Session) -> list[str]:
    return [
        "",
        f"ACCEPTANCE SESSION {session.project} (gate passed)",
        f"  volume    {session.volume}",
        f"  network   {session.network}",
        f"  directory {session.root}",
        "  Leftovers, if any, carry exactly these names; remove only them, never by prefix.",
    ]


@pytest.fixture(scope="module")
def acceptance(request: pytest.FixtureRequest):
    _require_opt_in()  # before anything else: no directory, no Docker call
    sid = uuid.uuid4().hex[:12]
    project = f"{_PREFIX}{sid}"
    root = Path(tempfile.mkdtemp(prefix=f"{project}-"))
    session = Session(sid, project, f"{project}-data", f"{project}-net", root,
                      _scrubbed_environment())  # fmt: skip
    try:
        resolved = root.resolve()
        assert _CHECKOUT.resolve() not in resolved.parents, "session root is inside the checkout"
        production_logs = Path(os.environ["LOCALAPPDATA"]) / "Northstar" / "india-operations"
        assert production_logs.resolve() not in [resolved, *resolved.parents]
        _render(session)
        _gate(session)  # creates nothing
        _start_volume_events(session)
    except BaseException:
        _stop_volume_events(session)
        shutil.rmtree(root, ignore_errors=True)  # rendered files only; no Docker resource exists
        raise
    (root / "session.json").write_text(json.dumps({
        "project": session.project, "volume": session.volume, "network": session.network,
        "directory": str(session.root), "started": session.started,
    }, indent=2), encoding="utf-8")  # fmt: skip
    _announce(request, _session_lines(session))
    failed_before = request.session.testsfailed
    try:
        yield session
    finally:
        _teardown(request, session, request.session.testsfailed > failed_before)


def _teardown(request: pytest.FixtureRequest, session: Session, failed: bool) -> None:
    errors: list[str] = []
    if failed:
        _attempt(errors, "writing evidence.json", (session.root / "evidence.json").write_text,
                 json.dumps(session.evidence, indent=2), "utf-8")  # fmt: skip
    errors += _remove_session_containers(session)
    keep = failed or bool(errors)
    if not keep:
        for kind, name in (("network", session.network), ("volume", session.volume)):
            _attempt(errors, f"removing {kind} {name}", _remove_session_object, session, kind, name)
    # Always, whatever happened above: the production non-interference check.
    _attempt(errors, "production non-interference", _final_production_check, session)
    keep = keep or bool(errors)
    lines = [f"ACCEPTANCE SESSION {session.project} teardown"]
    if keep:
        lines += ["  " + note for note in _remaining(session)]
    else:
        shutil.rmtree(session.root, ignore_errors=True)
        lines.append("  all session resources removed")
    lines += [f"  ERROR {error}" for error in errors]
    _announce(request, lines)
    if errors:
        # A teardown error: reported in addition to, never instead of, a test failure.
        pytest.fail("acceptance teardown: " + " || ".join(errors), pytrace=False)


# ---------------------------------------------------------------------------
# Wrapper invocation: output to files, waited on by process handle
# ---------------------------------------------------------------------------


@dataclass
class Wrapper:
    process: subprocess.Popen
    stdout: Path
    stderr: Path

    @property
    def returncode(self) -> int | None:
        return self.process.returncode

    def output(self) -> str:
        return (self.stdout.read_text(encoding="utf-8", errors="replace")
                + self.stderr.read_text(encoding="utf-8", errors="replace"))  # fmt: skip


def _wrapper_command(session: Session, deployment: str, logs: Path, *extra: str) -> list[str]:
    return [
        _POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
        "-File", str(_WRAPPER),
        "-DeploymentDirectory", str(session.deployments[deployment]),
        "-LogDirectory", str(logs),
        "-ProjectName", session.project,
        "-PreflightTimeoutSeconds", "60",
        *extra,
    ]  # fmt: skip


def _start_wrapper(session: Session, deployment: str, logs: Path, scenario: str,
                   *extra: str, env=None) -> Wrapper:  # fmt: skip
    """Start the wrapper with output to uniquely named files, never pipes.

    The docker CLI processes it starts inherit its handles; with pipes, waiting
    for end-of-file would wait for them, not for the wrapper.
    """
    _refuse_without_opt_in()
    stem = session.root / f"{re.sub(r'[^A-Za-z0-9]+', '-', scenario)}-{uuid.uuid4().hex[:8]}"
    stdout, stderr = Path(f"{stem}.stdout.txt"), Path(f"{stem}.stderr.txt")
    with open(stdout, "wb") as out, open(stderr, "wb") as err:
        process = subprocess.Popen(  # noqa: S603
            _wrapper_command(session, deployment, logs, *extra),
            env=env or session.env, stdout=out, stderr=err,
        )  # fmt: skip
    return Wrapper(process, stdout, stderr)


def _run_wrapper(session: Session, deployment: str, scenario: str, *extra: str, env=None):
    logs = session.root / f"logs ({scenario})"
    wrapper = _start_wrapper(session, deployment, logs, scenario, *extra, env=env)
    try:
        wrapper.process.wait(timeout=600)
    except subprocess.TimeoutExpired:
        wrapper.process.kill()  # this wrapper only; cleanup ends its docker CLI by exact name
        wrapper.process.wait(timeout=30)
        raise AssertionError(f"scenario {scenario}: the wrapper did not finish in 600 s") from None
    status = _status(session, logs)
    if status and status.get("containerName"):
        session.containers.add(status["containerName"])
    return wrapper, logs, status


def _status(session: Session, logs: Path) -> dict | None:
    path = logs / "last-run.json"
    if not path.exists():
        return None
    status = json.loads(path.read_text(encoding="utf-8"))
    assert status["schema"] == "northstar.india-operations.last-run/1"
    assert status["projectName"] == session.project
    return status


def _status_while_running(session: Session, logs: Path) -> dict | None:
    """The status file mid-run: meeting the atomic replace is fine, a partial file never is."""
    try:
        return _status(session, logs)
    except (PermissionError, FileNotFoundError):
        return None


def _database(session: Session) -> dict:
    """Inspect the session database through a read-only mount of the session volume."""
    assert _owner(session, "volume", session.volume) == session.project
    name = f"{session.project}-inspect-{uuid.uuid4().hex[:6]}"
    session.containers.add(name)
    result = _docker(
        session, "run", "--rm", "--pull", "never", "--network", "none", "--name", name,
        "--label", f"{_LABEL}={session.project}",
        "--mount", f"type=volume,source={session.volume},target=/data,readonly",
        "--entrypoint", "python", _IMAGE, "-c", _INSPECT_DATABASE,
    )  # fmt: skip
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _assert_empty_trading_state(snapshot: dict) -> None:
    assert set(_TRADING_TABLES) <= set(snapshot["tables"])
    assert {table: snapshot["counts"][table] for table in _TRADING_TABLES} == dict.fromkeys(
        _TRADING_TABLES, 0
    )
    assert snapshot["integrity"] == "ok"


# ---------------------------------------------------------------------------
# A, F, D, E: no operations container
# ---------------------------------------------------------------------------


def test_a_preflight_only_runs_nothing(acceptance: Session) -> None:
    result, logs, status = _run_wrapper(acceptance, "normal", "A preflight", "-PreflightOnly")

    assert result.returncode == 0, result.output()
    assert status is None  # last-run.json is not written by a preflight
    log = next((logs / "logs").glob("*.log")).read_text(encoding="utf-8")
    assert "acceptance isolation verified" in log and "preflight passed" in log
    assert _session_containers(acceptance) == []
    assert _inspect(acceptance, "volume", acceptance.volume) is None


def test_f_crlf_env_values_are_read_without_carriage_returns(acceptance: Session) -> None:
    rendered = _docker(acceptance, *_compose_args(acceptance, "crlf"), "config", "--format", "json")
    environment = json.loads(rendered.stdout)["services"][_SERVICE]["environment"]

    result, logs, _ = _run_wrapper(acceptance, "crlf", "F crlf", "-PreflightOnly")

    assert environment["NORTHSTAR_ACCEPTANCE_MARKER"] == "crlf-ok"
    assert result.returncode == 0, result.output()
    log = next((logs / "logs").glob("*.log")).read_text(encoding="utf-8")
    assert "finality mode disabled; final-through <unset>" in log


@pytest.mark.parametrize(
    "override",
    [{"DOCKER_HOST": "npipe:////./pipe/northstar_acceptance_missing_engine"},
     {"DOCKER_CONTEXT": "northstar-acceptance-missing-context"}],
    ids=["missing-endpoint", "missing-context"],
)  # fmt: skip
def test_d_an_unreachable_engine_exits_10_without_stopping_docker(
    acceptance: Session, override: dict
) -> None:
    result, _, status = _run_wrapper(
        acceptance, "normal", f"D {next(iter(override))}", env={**acceptance.env, **override}
    )

    assert result.returncode == 10, result.output()
    assert (status["outcome"], status["exitClass"]) == ("FAILED", "DOCKER_UNAVAILABLE")
    assert _session_containers(acceptance) == []


@pytest.mark.parametrize(
    ("deployment", "reason"),
    [("invalid", "does not validate"), ("missing-env", "required file not found")],
)
def test_e_invalid_configuration_and_missing_env_exit_11(
    acceptance: Session, deployment: str, reason: str
) -> None:
    result, _, status = _run_wrapper(acceptance, deployment, f"E {deployment}")

    assert result.returncode == 11, result.output()
    assert reason in status["reason"]
    assert _session_containers(acceptance) == []
    assert _inspect(acceptance, "volume", acceptance.volume) is None


# ---------------------------------------------------------------------------
# B, C: the real operation on a disposable database
# ---------------------------------------------------------------------------


def test_b_the_first_operation_waits_on_a_disposable_database(acceptance: Session) -> None:
    result, _, status = _run_wrapper(acceptance, "normal", "B first")

    assert result.returncode == 0, result.output()
    assert (status["outcome"], status["exitCode"], status["northstarExitCode"]) == (
        "WAITING", 0, 0
    )  # fmt: skip
    assert "finality is not established" in status["statusLine"]
    assert not _container_exists(acceptance, status["containerName"])
    assert _owner(acceptance, "volume", acceptance.volume) == acceptance.project
    snapshot = _database(acceptance)
    _assert_empty_trading_state(snapshot)
    acceptance.database = snapshot


def test_c_a_repeat_operation_waits_and_changes_no_trading_state(acceptance: Session) -> None:
    assert acceptance.database, "scenario B must run first"

    result, _, status = _run_wrapper(acceptance, "normal", "C repeat")

    assert result.returncode == 0, result.output()
    assert status["outcome"] == "WAITING"
    assert _database(acceptance) == acceptance.database


# ---------------------------------------------------------------------------
# G, J: bounded timeout and atomic status
# ---------------------------------------------------------------------------


def test_g_a_timeout_stops_and_removes_only_the_test_container(acceptance: Session) -> None:
    logs = acceptance.root / "logs (G timeout)"
    started = time.monotonic()
    wrapper = _start_wrapper(acceptance, "sleeper", logs, "G timeout", "-TimeoutSeconds", "20")
    seen = []
    while wrapper.process.poll() is None:
        assert time.monotonic() - started < 300, "the wrapper did not end its own timeout"
        if (logs / "last-run.json").exists():
            try:
                seen.append(_status(acceptance, logs)["outcome"])
            except (PermissionError, FileNotFoundError, json.JSONDecodeError) as error:
                assert not isinstance(error, json.JSONDecodeError), "a partial status file was read"
        time.sleep(0.2)
    status = _status(acceptance, logs)
    acceptance.containers.add(status["containerName"])

    assert wrapper.returncode == 12, wrapper.output()
    assert "RUNNING" in seen
    assert (status["outcome"], status["exitClass"]) == ("FAILED", "TIMEOUT")
    assert not _container_exists(acceptance, status["containerName"])
    assert _owner(acceptance, "volume", acceptance.volume) == acceptance.project
    _assert_production_unchanged(acceptance)


# ---------------------------------------------------------------------------
# H: interruption, recorded honestly
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("kill", ["wrapper-only", "process-tree"])
def test_h_an_interrupted_wrapper_is_recorded_honestly(acceptance: Session, kill: str) -> None:
    logs = acceptance.root / f"logs (H {kill})"
    wrapper = _start_wrapper(acceptance, "interrupt", logs, f"H {kill}")
    deadline = time.monotonic() + 180
    container = None
    while container is None:
        assert time.monotonic() < deadline, "the interrupt scenario never started its container"
        assert wrapper.process.poll() is None, wrapper.output()
        status = _status_while_running(acceptance, logs)
        if status:
            state = _container_state(acceptance, status["containerName"])
            if state is not None and state["running"]:
                container = state
        time.sleep(0.5)
    assert container["owner"] == acceptance.project
    acceptance.containers.add(container["name"])

    killed = time.monotonic()
    taskkill_exit = None
    if kill == "wrapper-only":
        wrapper.process.kill()  # TerminateProcess on this powershell.exe only, as "End task" would
    else:
        ended = subprocess.run(  # noqa: S603 - this test's own wrapper and its children only
            ["taskkill", "/PID", str(wrapper.process.pid), "/T", "/F"],  # noqa: S607
            capture_output=True, text=True, timeout=60, check=False,
        )  # fmt: skip
        taskkill_exit = ended.returncode  # a child that exits mid-walk can make this non-zero
    wrapper.process.wait(timeout=30)  # the process handle, not end-of-file on inherited pipes
    after_kill = _container_state(acceptance, container["id"])
    record = {
        "scenario": f"H {kill}",
        "wrapper_pid": wrapper.process.pid,
        "taskkill_exit": taskkill_exit,
        "container_id": container["id"],
        "container_name": container["name"],
        "seconds_from_kill_to_observation": round(time.monotonic() - killed, 3),
        "status_after_kill": _status(acceptance, logs)["outcome"],
        "container_after_kill": after_kill,
    }
    finished = time.monotonic() + 150
    while _container_running(acceptance, container["id"]) and time.monotonic() < finished:
        time.sleep(1)
    record["container_after_wait"] = _container_state(acceptance, container["id"])
    record["auto_removed_after_exit"] = record["container_after_wait"] is None
    acceptance.evidence.append(record)
    print(json.dumps(record))  # noqa: T201

    _stop_cli_processes(acceptance, container["name"])
    _remove_container(acceptance, container["id"])
    assert record["status_after_kill"] == "RUNNING"  # the stale status is the honest record
    assert not (record["container_after_wait"] or {}).get("running"), "it never finished"
    assert _container_state(acceptance, container["id"]) is None
    _assert_production_unchanged(acceptance)


# ---------------------------------------------------------------------------
# I, K: the real database lock, and recovery
# ---------------------------------------------------------------------------


def test_i_a_held_database_lock_is_skipped_then_recovers(acceptance: Session) -> None:
    assert not _container_exists(acceptance, acceptance.holder)
    acceptance.containers.add(acceptance.holder)
    started = _docker(
        acceptance, *_compose_args(acceptance, "lock"), "run", "-d", "--rm", "--no-deps",
        "--name", acceptance.holder, "lock-holder",
    )  # fmt: skip
    assert started.returncode == 0, started.stderr
    deadline = time.monotonic() + 120
    while "LOCKED" not in _docker(acceptance, "logs", acceptance.holder).stdout:
        assert time.monotonic() < deadline, "the lock holder never took the lock"
        time.sleep(0.5)
    assert _owner(acceptance, "container", acceptance.holder) == acceptance.project

    skipped, _, skipped_status = _run_wrapper(acceptance, "lock", "I skipped")
    _docker(acceptance, "stop", "--time", "2", acceptance.holder)
    _remove_container(acceptance, acceptance.holder)
    recovered, _, recovered_status = _run_wrapper(acceptance, "lock", "I recovered")

    assert skipped.returncode == 0, skipped.output()
    assert skipped_status["outcome"] == "SKIPPED"
    assert recovered.returncode == 0, recovered.output()
    assert recovered_status["outcome"] == "WAITING"
    _assert_production_unchanged(acceptance)


def test_k_recovery_leaves_a_valid_unlocked_database(acceptance: Session) -> None:
    result, _, status = _run_wrapper(acceptance, "normal", "K recovery")

    assert result.returncode == 0, result.output()
    assert status["outcome"] == "WAITING"  # not SKIPPED: no lock survived G, H or I
    snapshot = _database(acceptance)
    _assert_empty_trading_state(snapshot)
    assert snapshot == acceptance.database
    running = [n for i, n in _session_containers(acceptance) if _container_running(acceptance, i)]
    assert running == []


# ---------------------------------------------------------------------------
# L: production non-interference
# ---------------------------------------------------------------------------


def test_l_production_is_unchanged_at_session_end(acceptance: Session) -> None:
    _assert_production_unchanged(acceptance)
    assert acceptance.baseline["image"] == acceptance.image_id
