"""Windows Task Scheduler wrapper for the India operation (Stage 1).

    deploy/india/windows/Invoke-NorthstarIndiaOperations.ps1
    deploy/india/windows/Register-NorthstarIndiaOperationsTask.ps1
    deploy/india/windows/northstar-india-operations.task.xml

Static facts are checked as text on every platform, as the other India
deployment artifacts are. Behaviour is checked by running the real wrapper
under Windows PowerShell against a fake ``docker`` (a .cmd shim over a Python
script) in temporary directories whose paths contain spaces and parentheses.
No real Docker, container, database, scheduled task or deployment is touched:
the task definition is validated by Task Scheduler's own parser without
registering it.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

_API = Path(__file__).resolve().parents[1]
_WINDOWS = _API / "deploy" / "india" / "windows"
_WRAPPER = _WINDOWS / "Invoke-NorthstarIndiaOperations.ps1"
_INSTALLER = _WINDOWS / "Register-NorthstarIndiaOperationsTask.ps1"
_TEMPLATE = _WINDOWS / "northstar-india-operations.task.xml"
_TASK_NS = {"t": "http://schemas.microsoft.com/windows/2004/02/mit/task"}
_TOKEN_VARIABLE = "UPSTOX_" + "ANALYTICS_TOKEN"
_POWERSHELL = shutil.which("powershell.exe")
_windows = pytest.mark.skipif(
    sys.platform != "win32" or _POWERSHELL is None,
    reason="the wrapper's behaviour is exercised under Windows PowerShell only",
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _directives(script: str) -> str:
    """Return a PowerShell script without its comment block and comment lines."""
    without_blocks = re.sub(r"<#.*?#>", "", script, flags=re.DOTALL)
    return "\n".join(
        line for line in without_blocks.splitlines() if not line.lstrip().startswith("#")
    )


@pytest.fixture(scope="module")
def wrapper() -> str:
    return _directives(_read(_WRAPPER))


@pytest.fixture(scope="module")
def installer() -> str:
    return _directives(_read(_INSTALLER))


# ---------------------------------------------------------------------------
# Static: the command, the containers it may touch, .env and finality
# ---------------------------------------------------------------------------


def test_the_windows_artifacts_exist() -> None:
    assert sorted(path.name for path in _WINDOWS.iterdir()) == [
        "Invoke-NorthstarIndiaOperations.ps1",
        "Register-NorthstarIndiaOperationsTask.ps1",
        "northstar-india-operations.task.xml",
    ]


def test_the_wrapper_runs_only_the_existing_india_operations_service(wrapper: str) -> None:
    assert "$ProjectName = 'northstar-india'" in wrapper
    assert "$Service = 'india-operations'" in wrapper
    assert "@('run', '--rm', '-T', '--no-deps', '--name', $ContainerName, $Service)" in wrapper
    assert "'compose', '--project-directory', $deployment, '--file', $composeFile," in wrapper
    assert "'--project-name', $ProjectName" in wrapper
    # One run invocation, and it passes no command, environment or volume of its own.
    assert len(re.findall(r"'run'", wrapper)) == 1
    lowered = wrapper.lower()
    for forbidden in (
        "market-data", "paper", "economics", "finality-evidence", "'options'",
        "india-api", "india-web", "'exec'", "'down'", "'kill'", "'up'", "'start'",
        "'-e'", "'--env'", "'--volume'", "'-v'", "'--entrypoint'", "--filter", "'ps'",
    ):  # fmt: skip
        assert forbidden not in lowered, forbidden


def test_cleanup_addresses_only_this_runs_container(wrapper: str) -> None:
    assert '$ContainerName = "northstar-india-operations-$RunId".ToLowerInvariant()' in wrapper
    stops = re.findall(r"@\('stop'[^)]*\)", wrapper)
    removals = re.findall(r"@\('rm'[^)]*\)", wrapper)
    assert stops == ["@('stop', '--time', \"$StopGraceSeconds\", $ContainerName)"]
    assert removals == ["@('rm', '--force', $ContainerName)"]


def test_the_wrapper_never_writes_env_or_finality(wrapper: str) -> None:
    for writer in ("Set-Content", "Add-Content", "Out-File", "New-Item", "Clear-Content"):
        assert writer not in wrapper
    writes = re.findall(r"\[System\.IO\.File\]::(\w+)\(([^,)]*)", wrapper)
    targets = {(method, target.strip()) for method, target in writes}
    assert targets == {
        ("AppendAllText", "$script:LogFile"),
        ("WriteAllText", "$temporary"),
        ("Replace", "$temporary"),
        ("Move", "$temporary"),
        ("Exists", "$target"),
        ("Exists", "$required"),
        ("ReadAllLines", "$envFile"),
    }
    assert wrapper.count("$envFile") == 3  # defined, required, read once
    assert "$env:NORTHSTAR" not in wrapper
    assert not re.search(r"NORTHSTAR_FUTURES_FINAL_THROUGH\s*=", wrapper)
    assert "Read-EnvSetting $lines 'NORTHSTAR_FUTURES_FINAL_THROUGH'" in wrapper


def test_exit_codes_are_distinct_from_northstar_and_docker(wrapper: str) -> None:
    assert "$ExitDockerUnavailable = 10" in wrapper
    assert "$ExitDeploymentInvalid = 11" in wrapper
    assert "$ExitTimeout = 12" in wrapper
    assert "$ExitWrapperError = 13" in wrapper
    assert wrapper.rstrip().endswith("exit $exitCode")


def test_no_windows_artifact_configures_time_based_finality_or_rollover() -> None:
    for path in _WINDOWS.iterdir():
        lowered = _read(path).lower()
        for forbidden in ("21:00", "next morning", "minutes after", " ist ", "rollover="):
            assert forbidden not in lowered, (path.name, forbidden)
        assert "final_through=" not in lowered.replace(" ", "")


def test_the_task_template_is_disabled_hourly_and_single_instance() -> None:
    root = ET.fromstring(_read(_TEMPLATE))
    find = lambda path: root.find(path, _TASK_NS).text  # noqa: E731
    assert find("t:Settings/t:Enabled") == "false"
    assert find("t:Settings/t:MultipleInstancesPolicy") == "IgnoreNew"
    assert find("t:Settings/t:StartWhenAvailable") == "true"
    assert find("t:Settings/t:ExecutionTimeLimit") == "PT55M"
    assert find("t:Triggers/t:TimeTrigger/t:Repetition/t:Interval") == "PT1H"
    assert find("t:Principals/t:Principal/t:LogonType") == "InteractiveToken"
    assert find("t:Principals/t:Principal/t:RunLevel") == "LeastPrivilege"
    assert root.find(".//t:Password", _TASK_NS) is None
    assert set(re.findall(r"\{\{([A-Z_]+)\}\}", _read(_TEMPLATE))) == {
        "USER_ID", "POWERSHELL", "ARGUMENTS", "WORKING_DIRECTORY",
    }  # fmt: skip


def test_the_wrapper_timeout_is_inside_the_task_limit(wrapper: str) -> None:
    timeout = int(re.search(r"\$TimeoutSeconds = (\d+)", wrapper).group(1))
    assert timeout + 2 * 60 < 55 * 60  # room for the grace stop and cleanup


def test_the_installer_registers_disabled_and_never_enables(installer: str) -> None:
    assert installer.count("Register-ScheduledTask") == 1
    for forbidden in ("Enable-ScheduledTask", "Start-ScheduledTask", "Unregister-ScheduledTask",
                      "-Force", "-Password", "-User ", "HighestAvailable", ".env"):  # fmt: skip
        assert forbidden not in installer, forbidden
    assert "'The task definition must be registered disabled.'" in installer


# ---------------------------------------------------------------------------
# Behaviour, against a fake docker
# ---------------------------------------------------------------------------

_FAKE_DOCKER = r"""
import json, os, pathlib, sys, time

args = sys.argv[1:]
state = pathlib.Path(os.environ["FAKE_DOCKER_STATE"])
scenario = json.loads(pathlib.Path(os.environ["FAKE_DOCKER_SCENARIO"]).read_text())
with open(os.environ["FAKE_DOCKER_LOG"], "a", encoding="utf-8") as log:
    log.write(json.dumps(args) + "\n")


def out(text):
    sys.stdout.write(text)
    sys.stdout.flush()


if args[:1] == ["info"]:
    (state / "info.pid").write_text(str(os.getpid()))
    time.sleep(scenario.get("info_sleep", 0))
    if scenario.get("docker_available", True):
        out(scenario.get("engine", "27.3.1 linux") + "\n")
        sys.exit(0)
    sys.stderr.write("error during connect: the docker engine pipe was not found\n")
    sys.exit(1)
if args[:2] == ["compose", "version"]:
    out("2.29.7\n")
    sys.exit(0)
if args[:1] == ["compose"] and "config" in args:
    if scenario.get("config_ok", True):
        sys.exit(0)
    sys.stderr.write("required variable is missing: CONFIG-SECRET-SENTINEL\n")
    sys.exit(15)
if args[:2] == ["image", "inspect"]:
    if scenario.get("image_present", True):
        out("sha256:feedface\n")
        sys.exit(0)
    sys.exit(1)
if args[:1] == ["stop"]:
    (state / ("stopped-" + args[-1])).write_text("1")
    sys.exit(0)
if args[:1] == ["rm"]:
    sys.exit(0)
if args[:1] == ["compose"] and "run" in args:
    name = args[args.index("--name") + 1]
    (state / ("run-" + name + ".pid")).write_text(str(os.getpid()))
    lock = state / "db.lock"
    held = None
    if scenario.get("simulate_lock"):
        try:
            held = os.open(lock, os.O_CREAT | os.O_EXCL)
        except FileExistsError:
            out("DAILY OPERATION: SKIPPED\nAnother Northstar operations writer is active.\n")
            sys.exit(0)
    try:
        out(scenario.get("stdout", ""))
        sys.stderr.write(scenario.get("stderr", ""))
        sys.stderr.flush()
        deadline = time.time() + scenario.get("run_sleep", 0)
        release = scenario.get("hold_until")
        while time.time() < deadline or (release and not (state / release).exists()):
            if (state / ("stopped-" + name)).exists() and not scenario.get("ignore_stop"):
                sys.exit(137)
            time.sleep(0.1)
        out(scenario.get("stdout_after", ""))
        sys.exit(scenario.get("exit_code", 0))
    finally:
        if held is not None:
            os.close(held)
            lock.unlink()
sys.stderr.write("fake docker: unexpected arguments\n")
sys.exit(99)
"""

_COMPLETED = (
    "CHRONOLOGICAL DAILY OPERATION\nContract: NIFTY@NSE 2026-10-27\n\n"
    "STATUS: COMPLETED -- 1 session(s) processed\n"
)


class Harness:
    """A deployment, a log directory and a fake docker, all under awkward paths."""

    def __init__(self, root: Path) -> None:
        self.deployment = root / "Local Disk(E)" / "Codes" / "Northstar" / "deploy" / "india"
        self.logs = root / "Northstar logs (operations)"
        self.bin = root / "fake docker (bin)"
        self.state = root / "state"
        for directory in (self.deployment, self.bin, self.state):
            directory.mkdir(parents=True)
        self.secret = "fake-secret-" + uuid.uuid4().hex
        (self.deployment / "compose.yaml").write_text("name: northstar-india\n", encoding="utf-8")
        self.env_file = self.deployment / ".env"
        self.env_file.write_bytes(
            (
                f"{_TOKEN_VARIABLE}={self.secret}\r\n"
                "NORTHSTAR_FUTURES_DAILY_BAR_FINALITY=operator-approved\r\n"
                "NORTHSTAR_FUTURES_FINAL_THROUGH=2026-10-08\r\n"
            ).encode()
        )
        script = self.bin / "fake_docker.py"
        script.write_text(_FAKE_DOCKER, encoding="utf-8")
        self.docker = self.bin / "docker.cmd"
        self.docker.write_text(
            f'@echo off\r\n"{sys.executable}" "{script}" %*\r\nexit %ERRORLEVEL%\r\n',
            encoding="utf-8",
        )
        self.calls_file = root / "docker-calls.jsonl"
        self.scenario_file = root / "scenario.json"
        self.scenario()

    def scenario(self, **values) -> None:
        self.scenario_file.write_text(json.dumps(values), encoding="utf-8")

    def command(self, *extra: str, docker: Path | None = None) -> list[str]:
        return [
            _POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
            "-File", str(_WRAPPER),
            "-DeploymentDirectory", str(self.deployment),
            "-LogDirectory", str(self.logs),
            "-DockerExecutable", str(docker or self.docker),
            "-PreflightTimeoutSeconds", "20",
            *extra,
        ]  # fmt: skip

    def environment(self) -> dict[str, str]:
        import os

        return {
            **os.environ,
            "FAKE_DOCKER_STATE": str(self.state),
            "FAKE_DOCKER_SCENARIO": str(self.scenario_file),
            "FAKE_DOCKER_LOG": str(self.calls_file),
        }

    def run(self, *extra: str, docker: Path | None = None, timeout: float = 120):
        return subprocess.run(  # noqa: S603 - fixed test command
            self.command(*extra, docker=docker),
            env=self.environment(),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )

    def start(self, *extra: str) -> subprocess.Popen:
        return subprocess.Popen(  # noqa: S603 - fixed test command
            self.command(*extra),
            env=self.environment(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def calls(self) -> list[list[str]]:
        if not self.calls_file.exists():
            return []
        return [json.loads(line) for line in _read(self.calls_file).splitlines()]

    def run_calls(self) -> list[list[str]]:
        return [call for call in self.calls() if call[:1] == ["compose"] and "run" in call]

    def status(self) -> dict:
        return json.loads(_read(self.logs / "last-run.json"))

    def log_files(self) -> list[Path]:
        directory = self.logs / "logs"
        return sorted(directory.glob("*.log")) if directory.exists() else []


@pytest.fixture
def harness(tmp_path: Path) -> Harness:
    return Harness(tmp_path)


def _alive(pid: int) -> bool:
    listing = subprocess.run(  # noqa: S603
        ["tasklist", "/FI", f"PID eq {pid}", "/NH"],  # noqa: S607
        capture_output=True,
        text=True,
        check=False,
    ).stdout
    return re.search(rf"\b{pid}\b", listing) is not None


_LOG_LINE = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z \[(\d{8}T\d{6}Z-[0-9a-f]{8})\] ")


@_windows
def test_a_completed_run_is_logged_recorded_and_passes_exit_zero(harness: Harness) -> None:
    harness.scenario(stdout=_COMPLETED, stderr="northstar.operations INFO exit SUCCESS (0)\n")
    env_before = harness.env_file.read_bytes()

    result = harness.run()

    assert result.returncode == 0, result.stdout + result.stderr
    (call,) = harness.run_calls()
    name = call[call.index("--name") + 1]
    assert call == [
        "compose", "--project-directory", str(harness.deployment),
        "--file", str(harness.deployment / "compose.yaml"),
        "--project-name", "northstar-india",
        "run", "--rm", "-T", "--no-deps", "--name", name, "india-operations",
    ]  # fmt: skip
    status = harness.status()
    assert status["schema"] == "northstar.india-operations.last-run/1"
    assert re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-f]{8}", status["runId"])
    assert name == f"northstar-india-operations-{status['runId'].lower()}"
    assert status["containerName"] == name
    assert (status["outcome"], status["exitCode"], status["exitClass"]) == (
        "COMPLETED", 0, "SUCCESS"
    )  # fmt: skip
    assert status["northstarExitCode"] == 0
    assert status["statusLine"] == "STATUS: COMPLETED -- 1 session(s) processed"
    assert (status["finalityMode"], status["finalThrough"]) == ("operator-approved", "2026-10-08")
    assert status["dockerServerVersion"] == "27.3.1"
    assert status["startedAtUtc"].endswith("Z") and status["endedAtUtc"].endswith("Z")
    assert status["durationSeconds"] >= 0
    (log,) = harness.log_files()
    assert log.name == f"{status['runId']}.log"
    assert status["logFile"] == str(log)
    text = _read(log)
    stamped = [line for line in text.splitlines() if not line.startswith("    ")]
    assert stamped and all(_LOG_LINE.match(line) for line in stamped)
    assert {_LOG_LINE.match(line).group(1) for line in stamped} == {status["runId"]}
    for expected in (
        "preflight passed",
        "finality mode operator-approved; final-through 2026-10-08 "
        "(read from .env, never changed here)",
        "docker engine 27.3.1 (linux); compose 2.29.7",
        "    stdout | STATUS: COMPLETED -- 1 session(s) processed",
        "    stderr | northstar.operations INFO exit SUCCESS (0)",
        "finished: outcome COMPLETED, exit 0 (SUCCESS)",
    ):
        assert expected in text
    for artifact in (text, _read(harness.logs / "last-run.json"), result.stdout, result.stderr):
        assert harness.secret not in artifact
    assert harness.env_file.read_bytes() == env_before
    assert not list(harness.logs.glob("*.tmp"))


@_windows
@pytest.mark.parametrize(
    ("exit_code", "stdout", "outcome"),
    [
        (0, "STATUS: WAITING -- 2026-10-09 is NOT_YET_FINAL\n", "WAITING"),
        (0, "DAILY OPERATION: SKIPPED\nAnother writer is active.\n", "SKIPPED"),
        (0, "a report without a status line\n", "FAILED"),
        (1, "", "FAILED"),
        (3, "STATUS: ROLLOVER REQUIRED\n", "FAILED"),
        (4, _COMPLETED, "FAILED"),
        (5, "", "FAILED"),
        (6, "", "FAILED"),
    ],
    ids=["waiting", "skipped", "no-status", "internal", "rollover", "data", "state", "provider"],
)
def test_northstar_exit_codes_pass_through_and_outcomes_are_classified(
    harness: Harness, exit_code: int, stdout: str, outcome: str
) -> None:
    stderr = "STATE ERROR: conflict\n" if exit_code == 5 else ""
    harness.scenario(stdout=stdout, stderr=stderr, exit_code=exit_code)

    result = harness.run()

    assert result.returncode == exit_code
    status = harness.status()
    assert (status["outcome"], status["exitCode"], status["northstarExitCode"]) == (
        outcome, exit_code, exit_code
    )  # fmt: skip
    if exit_code == 5:
        assert status["reason"] == "STATE ERROR: conflict"


@_windows
@pytest.mark.parametrize(
    "scenario",
    [{"docker_available": False}, {"engine": "27.3.1 windows"}],
    ids=["engine-down", "windows-containers"],
)
def test_an_unavailable_docker_engine_exits_10_and_runs_nothing(harness: Harness, scenario) -> None:
    harness.scenario(**scenario)

    result = harness.run()

    assert result.returncode == 10
    assert harness.run_calls() == []
    status = harness.status()
    assert (status["outcome"], status["exitClass"]) == ("FAILED", "DOCKER_UNAVAILABLE")


@_windows
def test_a_missing_docker_cli_exits_10(harness: Harness) -> None:
    result = harness.run(docker=harness.bin / "no docker here.exe")

    assert result.returncode == 10
    assert "docker CLI could not be started" in harness.status()["reason"]


@_windows
def test_a_hanging_docker_engine_is_bounded_and_exits_10(harness: Harness) -> None:
    harness.scenario(info_sleep=120)
    command = harness.command()
    command[command.index("-PreflightTimeoutSeconds") + 1] = "3"

    started = time.monotonic()
    result = subprocess.run(  # noqa: S603
        command, env=harness.environment(), capture_output=True, text=True, timeout=90, check=False
    )

    assert result.returncode == 10
    assert time.monotonic() - started < 60
    assert "did not answer within 3 s" in harness.status()["reason"]
    assert not _alive(int(_read(harness.state / "info.pid")))


@_windows
@pytest.mark.parametrize("missing", ["compose.yaml", ".env", "directory"])
def test_missing_deployment_files_exit_11_before_any_docker_call(
    harness: Harness, missing: str
) -> None:
    if missing == "directory":
        shutil.rmtree(harness.deployment)
    else:
        (harness.deployment / missing).unlink()

    result = harness.run()

    assert result.returncode == 11
    assert harness.calls() == []
    status = harness.status()
    assert (status["outcome"], status["exitClass"]) == ("FAILED", "DEPLOYMENT_INVALID")


@_windows
def test_an_invalid_compose_configuration_exits_11_without_logging_its_message(
    harness: Harness,
) -> None:
    harness.scenario(config_ok=False)

    result = harness.run()

    assert result.returncode == 11
    assert harness.run_calls() == []
    assert "does not validate (exit 15)" in harness.status()["reason"]
    for artifact in (_read(harness.log_files()[0]), result.stdout):
        assert "CONFIG-SECRET-SENTINEL" not in artifact


@_windows
def test_an_unbuilt_image_exits_11(harness: Harness) -> None:
    harness.scenario(image_present=False)

    result = harness.run()

    assert result.returncode == 11
    assert harness.run_calls() == []
    assert "northstar-api:india is not built" in harness.status()["reason"]


@_windows
def test_a_timeout_stops_and_removes_only_this_runs_container(harness: Harness) -> None:
    harness.scenario(stdout="CHRONOLOGICAL DAILY OPERATION\n", run_sleep=120, exit_code=0)

    started = time.monotonic()
    result = harness.run("-TimeoutSeconds", "3", timeout=150)

    assert result.returncode == 12
    assert time.monotonic() - started < 90
    (call,) = harness.run_calls()
    name = call[call.index("--name") + 1]
    calls = harness.calls()
    assert ["stop", "--time", "30", name] in calls
    assert ["rm", "--force", name] in calls
    cleanup = [c for c in calls if c[:1] in (["stop"], ["rm"], ["kill"])]
    assert all(c[-1] == name for c in cleanup)
    assert not [c for c in calls if {"india-api", "india-web"} & set(c)]
    status = harness.status()
    assert (status["outcome"], status["exitClass"]) == ("FAILED", "TIMEOUT")
    assert "timed out after 3 s" in status["reason"]
    log = _read(harness.log_files()[0])
    assert "    stdout | CHRONOLOGICAL DAILY OPERATION" in log  # partial output is kept
    assert not _alive(int(_read(harness.state / f"run-{name}.pid")))


@_windows
def test_a_run_that_ignores_docker_stop_is_killed_as_a_process_tree(harness: Harness) -> None:
    harness.scenario(run_sleep=120, ignore_stop=True)

    result = harness.run("-TimeoutSeconds", "3", timeout=150)

    assert result.returncode == 12
    (call,) = harness.run_calls()
    name = call[call.index("--name") + 1]
    assert not _alive(int(_read(harness.state / f"run-{name}.pid")))
    assert ["rm", "--force", name] in harness.calls()


@_windows
def test_concurrent_invocations_never_interleave_and_one_is_skipped(harness: Harness) -> None:
    # The first run holds the (simulated) database lock until the test releases
    # it, so the second run always meets it, however slow the host is.
    harness.scenario(stdout=_COMPLETED, simulate_lock=True, hold_until="release")

    first = harness.start()
    deadline = time.monotonic() + 60
    while not (harness.state / "db.lock").exists():
        assert time.monotonic() < deadline, "the first run never took the database lock"
        assert first.poll() is None, first.communicate()
        time.sleep(0.1)
    second = harness.run()
    (harness.state / "release").write_text("1", encoding="utf-8")
    first_out, _ = first.communicate(timeout=120)

    assert (first.returncode, second.returncode) == (0, 0)
    names = [call[call.index("--name") + 1] for call in harness.run_calls()]
    assert len(names) == 2 and len(set(names)) == 2
    assert "outcome COMPLETED" in first_out
    assert "outcome SKIPPED" in second.stdout
    assert len(harness.log_files()) == 2
    # Last writer wins: the first run finishes after the second.
    assert harness.status()["outcome"] == "COMPLETED"
    assert not list(harness.logs.glob("*.tmp"))


@_windows
def test_the_status_file_is_running_during_the_run_and_always_complete_json(
    harness: Harness,
) -> None:
    # The run is held until the test has seen RUNNING, then released.
    harness.scenario(stdout=_COMPLETED, hold_until="release")
    target = harness.logs / "last-run.json"

    process = harness.start()
    seen: list[str] = []
    deadline = time.monotonic() + 90
    while process.poll() is None:
        assert time.monotonic() < deadline, "the run never reported RUNNING"
        if target.exists():
            try:
                seen.append(json.loads(target.read_text(encoding="utf-8"))["outcome"])
            except PermissionError:
                pass  # a reader can meet the replace itself; it never meets a partial file
        if "RUNNING" in seen:
            (harness.state / "release").write_text("1", encoding="utf-8")
        time.sleep(0.05)
    process.communicate(timeout=60)

    assert "RUNNING" in seen
    assert harness.status()["outcome"] == "COMPLETED"
    assert not list(harness.logs.glob("*.tmp"))


@_windows
def test_preflight_only_runs_nothing_and_leaves_the_status_file_alone(harness: Harness) -> None:
    result = harness.run("-PreflightOnly")

    assert result.returncode == 0
    assert harness.run_calls() == []
    assert not (harness.logs / "last-run.json").exists()
    log = _read(harness.log_files()[0])
    assert "preflight only: the operation was not run" in log


@_windows
def test_expired_logs_are_pruned_and_nothing_else_is(harness: Harness) -> None:
    import os

    logs = harness.logs / "logs"
    logs.mkdir(parents=True)
    old = (datetime.now(UTC) - timedelta(days=30)).timestamp()
    expired = logs / "20200101T000000Z-0123abcd.log"
    recent = logs / "20260101T000000Z-0123abce.log"
    foreign = logs / "operator notes.log"
    stale = harness.logs / "last-run.json.20200101T000000Z-0123abcd.tmp"
    for path in (expired, recent, foreign, stale):
        path.write_text("x", encoding="utf-8")
    for path in (expired, foreign, stale):
        os.utime(path, (old, old))
    harness.scenario(stdout=_COMPLETED)

    assert harness.run("-LogRetentionDays", "7").returncode == 0

    assert not expired.exists() and not stale.exists()
    assert recent.exists() and foreign.exists()
    assert len(harness.log_files()) == 3  # recent, foreign and this run's log


@_windows
def test_the_rendered_task_passes_task_schedulers_parser_without_registering(
    tmp_path: Path,
) -> None:
    rendered = tmp_path / "rendered task (review).xml"
    check = tmp_path / "check.ps1"
    check.write_text(
        "$s = New-Object -ComObject Schedule.Service; $s.Connect()\n"
        "$d = $s.NewTask(0); $d.XmlText = [IO.File]::ReadAllText($args[0])\n"
        "$a = $d.Actions.Item(1)\n"
        "'{0}|{1}|{2}|{3}|{4}|{5}|{6}' -f $d.Settings.Enabled, $d.Settings.MultipleInstances, "
        "$d.Settings.StartWhenAvailable, $d.Principal.LogonType, $d.Principal.RunLevel, "
        "$a.Arguments, $a.WorkingDirectory\n",
        encoding="utf-8",
    )
    task_name = "Northstar Stage1 Test " + uuid.uuid4().hex

    render = subprocess.run(  # noqa: S603
        [_POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File",
         str(_INSTALLER), "-TaskName", task_name, "-RenderPath", str(rendered)],
        capture_output=True, text=True, timeout=60, check=False,
    )  # fmt: skip
    parsed = subprocess.run(  # noqa: S603
        [_POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File",
         str(check), str(rendered)],
        capture_output=True, text=True, timeout=60, check=False,
    )  # fmt: skip
    registered = subprocess.run(  # noqa: S603
        [_POWERSHELL, "-NoProfile", "-Command",
         f"@(Get-ScheduledTask -TaskName '{task_name}' -ErrorAction SilentlyContinue).Count"],
        capture_output=True, text=True, timeout=60, check=False,
    )  # fmt: skip

    assert render.returncode == 0, render.stderr
    assert "Rendered (not registered)" in render.stdout
    enabled, instances, available, logon, level, arguments, directory = parsed.stdout.strip().split(
        "|"
    )
    assert parsed.returncode == 0, parsed.stderr
    assert (enabled, instances, available, logon, level) == ("False", "2", "True", "3", "0")
    assert arguments == (f'-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "{_WRAPPER}"')
    assert directory == str(_WRAPPER.parents[1])
    assert registered.stdout.strip() == "0"
