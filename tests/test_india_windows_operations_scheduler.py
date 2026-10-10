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

import ast
import json
import os
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
_DOCKER_ACCEPTANCE = _API / "tests" / "test_india_windows_docker_acceptance.py"
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
        # Acceptance only: an existing status file is read, never replaced, before refusing.
        ("Exists", "$existingStatus"),
        ("ReadAllText", "$existingStatus"),
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
    if scenario.get("config_ok", True) and "--format" in args:
        out(scenario.get("config_json", "{}"))
        sys.exit(scenario.get("config_json_exit", 0))
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
        (7, "STATUS: EXPIRY EXCEPTION\n", "EXPIRY_EXCEPTION"),
    ],
    ids=[
        "waiting",
        "skipped",
        "no-status",
        "internal",
        "rollover",
        "data",
        "state",
        "provider",
        "expiry-exception",
    ],
)
def test_northstar_exit_codes_pass_through_and_outcomes_are_classified(
    harness: Harness, exit_code: int, stdout: str, outcome: str
) -> None:
    stderr = {
        5: "STATE ERROR: conflict\n",
        7: "EXPIRY_EXCEPTION ERROR: Expiry exception: NIFTY@NSE 2026-10-27\n",
    }.get(exit_code, "")
    harness.scenario(stdout=stdout, stderr=stderr, exit_code=exit_code)

    result = harness.run()

    assert result.returncode == exit_code
    status = harness.status()
    assert (status["outcome"], status["exitCode"], status["northstarExitCode"]) == (
        outcome, exit_code, exit_code
    )  # fmt: skip
    if exit_code == 5:
        assert status["reason"] == "STATE ERROR: conflict"
    if exit_code == 7:
        assert status["exitClass"] == "EXPIRY_EXCEPTION"
        assert status["reason"].startswith("EXPIRY_EXCEPTION ERROR: Expiry exception:")


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


# ---------------------------------------------------------------------------
# -ProjectName: production default, isolated acceptance projects, fail-closed guards
# ---------------------------------------------------------------------------

_ACCEPTANCE = "northstar-india-acceptance-fake0001"
_WAITING = "STATUS: WAITING -- Daily-bar finality is not established.\n"


def _model(project: str = _ACCEPTANCE) -> dict:
    """A rendered Compose model as `docker compose config --format json` prints it."""
    return {
        "name": project,
        "services": {
            "india-operations": {
                "image": "northstar-api:india",
                "pull_policy": "never",
                "command": ["northstar", "operations", "daily"],
                "environment": {"NORTHSTAR_DATABASE": "/data/northstar.sqlite3"},
                "volumes": [
                    {"type": "volume", "source": "acceptance-data", "target": "/data", "volume": {}}
                ],
                "networks": {"acceptance": None},
            }
        },
        "volumes": {"acceptance-data": {"name": f"{project}-data"}},
        "networks": {"acceptance": {"name": f"{project}-net", "internal": True}},
    }


def _acceptance(harness: Harness, project: str = _ACCEPTANCE, *, omit: tuple[str, ...] = (),
                deployment: Path | None = None, logs: Path | None = None) -> list[str]:  # fmt: skip
    """The wrapper command for an acceptance project, with optional omissions."""
    command = [
        _POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
        "-File", str(_WRAPPER), "-DockerExecutable", str(harness.docker),
        "-PreflightTimeoutSeconds", "20", "-ProjectName", project,
    ]  # fmt: skip
    if "DeploymentDirectory" not in omit:
        command += ["-DeploymentDirectory", str(deployment or harness.deployment)]
    if "LogDirectory" not in omit:
        command += ["-LogDirectory", str(logs or harness.logs)]
    return command


def _execute(harness: Harness, command: list[str], env: dict[str, str] | None = None):
    return subprocess.run(  # noqa: S603 - fixed test command
        command,
        env=env or harness.environment(),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def test_the_project_parameter_defaults_to_production(wrapper: str) -> None:
    assert "[string] $ProjectName = 'northstar-india'" in wrapper
    assert "$ProductionProjectName = 'northstar-india'" in wrapper
    assert "$IsAcceptance = -not ($ProjectName -ceq $ProductionProjectName)" in wrapper
    assert "$AcceptanceProjectPattern = '^northstar-india-acceptance-[a-z0-9][a-z0-9-]{3,39}$'" in (
        wrapper
    )


def test_the_scheduled_task_never_passes_a_project_name(installer: str) -> None:
    assert "ProjectName" not in installer
    assert "ProjectName" not in _read(_TEMPLATE)


@_windows
def test_the_default_project_is_production_without_any_acceptance_inspection(
    harness: Harness,
) -> None:
    harness.scenario(stdout=_WAITING)

    result = harness.run()

    assert result.returncode == 0
    (call,) = harness.run_calls()
    assert call[call.index("--project-name") + 1] == "northstar-india"
    assert not [c for c in harness.calls() if "--format" in c and "config" in c]
    status = harness.status()
    assert (status["projectName"], status["outcome"]) == ("northstar-india", "WAITING")
    log = _read(harness.log_files()[0])
    assert "compose project: northstar-india\n" in log
    assert "acceptance" not in log


@_windows
def test_an_omitted_deployment_directory_resolves_to_the_wrappers_parent(
    harness: Harness,
) -> None:
    """The task passes no -DeploymentDirectory; Windows PowerShell 5.1 must still resolve it.

    $PSScriptRoot is empty while an advanced script's parameter defaults are
    evaluated, so the default is resolved in the script body. Preflight only and
    the fake docker: nothing can run, even on a host whose checkout has a .env.
    """
    command = [
        _POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
        "-File", str(_WRAPPER), "-LogDirectory", str(harness.logs),
        "-DockerExecutable", str(harness.docker), "-PreflightOnly",
    ]  # fmt: skip

    result = _execute(harness, command)

    assert "Cannot bind argument" not in result.stdout + result.stderr
    assert result.returncode in (0, 11), result.stdout + result.stderr
    assert harness.run_calls() == []
    log = _read(harness.log_files()[0])
    assert f"deployment directory: {_WRAPPER.parents[1]}\n" in log
    assert "compose project: northstar-india\n" in log


@_windows
def test_an_acceptance_project_runs_under_its_own_name_after_isolation_checks(
    harness: Harness,
) -> None:
    harness.scenario(stdout=_WAITING, config_json=json.dumps(_model()))

    result = _execute(harness, _acceptance(harness))

    assert result.returncode == 0, result.stdout + result.stderr
    (call,) = harness.run_calls()
    assert call[call.index("--project-name") + 1] == _ACCEPTANCE
    inspections = [c for c in harness.calls() if "--format" in c and "config" in c]
    assert len(inspections) == 1
    assert inspections[0][inspections[0].index("--project-name") + 1] == _ACCEPTANCE
    assert harness.calls().index(inspections[0]) < harness.calls().index(call)
    status = harness.status()
    assert (status["projectName"], status["outcome"], status["exitCode"]) == (
        _ACCEPTANCE, "WAITING", 0
    )  # fmt: skip
    log = _read(harness.log_files()[0])
    assert f"compose project: {_ACCEPTANCE} (isolated acceptance" in log
    assert "acceptance isolation verified" in log


@_windows
@pytest.mark.parametrize(
    "project",
    [
        "northstar", "NORTHSTAR-INDIA", "northstar-india ", "other-project",
        "northstar-india-acceptance-", "northstar-india-acceptance-ab",
        "Northstar-India-Acceptance-abcd", "northstar-india-acceptance-ab cd",
        "northstar-india-acceptance-abcd/../x", "northstar-india-acceptance-" + "a" * 41,
    ],
)  # fmt: skip
def test_an_unknown_project_name_is_refused_before_anything(harness: Harness, project) -> None:
    result = _execute(harness, _acceptance(harness, project))

    assert result.returncode == 11
    assert "REFUSED (exit 11)" in result.stderr
    assert harness.calls() == []
    assert not harness.logs.exists()


@_windows
@pytest.mark.parametrize("omitted", ["DeploymentDirectory", "LogDirectory"])
def test_an_acceptance_run_must_name_both_directories(harness: Harness, omitted: str) -> None:
    result = _execute(harness, _acceptance(harness, omit=(omitted,)))

    assert result.returncode == 11
    assert f"must pass -{omitted} explicitly" in result.stderr
    assert harness.calls() == []
    assert not harness.logs.exists()


@_windows
@pytest.mark.parametrize(
    "deployment",
    [_WRAPPER.parents[1], _WRAPPER.parent, _WRAPPER.parents[3]],
    ids=["production", "inside-production", "around-production"],
)
def test_an_acceptance_run_cannot_reach_the_production_deployment(
    harness: Harness, deployment: Path
) -> None:
    result = _execute(harness, _acceptance(harness, deployment=deployment))

    assert result.returncode == 11
    assert "overlaps the production deployment directory" in result.stderr
    assert harness.calls() == []
    assert not harness.logs.exists()


@_windows
@pytest.mark.parametrize("relative", ["", "logs", ".."], ids=["same", "inside", "around"])
def test_an_acceptance_run_cannot_use_the_production_log_directory(
    harness: Harness, tmp_path: Path, relative: str
) -> None:
    appdata = tmp_path / "appdata (local)"
    production_logs = appdata / "Northstar" / "india-operations"
    env = {**harness.environment(), "LOCALAPPDATA": str(appdata)}
    logs = (production_logs / relative) if relative else production_logs

    result = _execute(harness, _acceptance(harness, logs=logs), env=env)

    assert result.returncode == 11
    assert "overlaps the production log directory" in result.stderr
    assert harness.calls() == []
    assert not appdata.exists()


def _unsafe(change) -> dict:
    model = _model()
    change(model)
    return model


_SERVICE = ("services", "india-operations")


def _service(model: dict) -> dict:
    return model["services"]["india-operations"]


# A local volume bound onto production's data directory inside the Docker Desktop VM.
_BIND_ALIAS = {
    "type": "none",
    "o": "bind",
    "device": "/var/lib/docker/volumes/northstar-india-data/_data",
}


_UNSAFE_MODELS = {
    "production-volume": (
        lambda m: m["volumes"]["acceptance-data"].update(name="northstar-india-data"),
        "is a production volume",
    ),
    "unprefixed-volume": (
        lambda m: m["volumes"]["acceptance-data"].update(name="scratch-data"),
        "not northstar-india-acceptance-fake0001-<name>",
    ),
    "external-volume": (
        lambda m: m["volumes"]["acceptance-data"].update(external=True),
        "is external",
    ),
    "network-not-internal": (
        lambda m: m["networks"]["acceptance"].pop("internal"),
        "is not internal",
    ),
    "external-network": (
        lambda m: m["networks"]["acceptance"].update(external=True),
        "is external",
    ),
    "bind-mount": (
        lambda m: _service(m)["volumes"].append(
            {"type": "bind", "source": "C:\\Users", "target": "/host"}
        ),
        "mounts bind 'C:\\Users' at '/host'",
    ),
    "docker-socket": (
        lambda m: _service(m)["volumes"].append(
            {"type": "bind", "source": "/var/run/docker.sock", "target": "/var/run/docker.sock"}
        ),
        "mounts the Docker socket",
    ),
    "ports": (
        lambda m: _service(m).update(ports=[{"target": 8000, "published": "8000"}]),
        "sets ports",
    ),
    "build": (lambda m: _service(m).update(build={"context": "."}), "sets build"),
    "privileged": (lambda m: _service(m).update(privileged=True), "sets privileged"),
    "host-network": (lambda m: _service(m).update(network_mode="host"), "sets network_mode"),
    "pull-policy": (lambda m: _service(m).pop("pull_policy"), "is not pull_policy never"),
    "other-image": (lambda m: _service(m).update(image="northstar-api:local"), "does not use"),
    "provider-token": (
        lambda m: _service(m)["environment"].update(
            {"UPSTOX_ANALYTICS_TOKEN": "SENTINEL-NOT-A-REAL-TOKEN"}
        ),
        "passes provider credential variable UPSTOX_ANALYTICS_TOKEN",
    ),
    "project-name": (lambda m: m.update(name="northstar-india"), "the rendered project is not"),
    "missing-service": (
        lambda m: m["services"].update({"other": m["services"].pop("india-operations")}),
        "service india-operations is not defined",
    ),
    "no-network": (lambda m: _service(m).pop("networks"), "is not attached to a session network"),
    "secrets": (lambda m: m.update(secrets={"x": {"file": "x"}}), "top-level secrets"),
    # H1: a session-prefixed name proves nothing when the backing storage aliases production.
    "volume-bind-alias-of-production": (
        lambda m: m["volumes"]["acceptance-data"].update(driver="local", driver_opts=_BIND_ALIAS),
        "sets driver_opts, which can alias other storage under a session name",
    ),
    "volume-driver-opts-without-driver": (
        lambda m: m["volumes"]["acceptance-data"].update(driver_opts=_BIND_ALIAS),
        "sets driver_opts",
    ),
    "volume-unknown-driver": (
        lambda m: m["volumes"]["acceptance-data"].update(driver="rclone"),
        "uses driver 'rclone'; only local is allowed",
    ),
    "volume-unknown-key": (
        lambda m: m["volumes"]["acceptance-data"].update(labels={"x": "y"}),
        "sets labels, which acceptance does not allow",
    ),
    "network-driver-opts": (
        lambda m: m["networks"]["acceptance"].update(
            driver_opts={"com.docker.network.bridge.name": "docker0"}
        ),
        "sets driver_opts",
    ),
    "network-macvlan-driver": (
        lambda m: m["networks"]["acceptance"].update(driver="macvlan"),
        "uses driver 'macvlan'; only bridge is allowed",
    ),
    "network-ipam-config": (
        lambda m: m["networks"]["acceptance"].update(ipam={"config": [{"subnet": "10.9.0.0/24"}]}),
        "sets ipam",
    ),
    "network-attachment-settings": (
        lambda m: _service(m).update(networks={"acceptance": {"aliases": ["api.upstox.com"]}}),
        "joins network 'acceptance' with attachment settings",
    ),
    "service-extra-hosts": (
        lambda m: _service(m).update(extra_hosts=["api.upstox.com=10.0.0.1"]),
        "sets extra_hosts, which acceptance does not allow",
    ),
    "service-cgroup-parent": (
        lambda m: _service(m).update(cgroup_parent="x"),
        "sets cgroup_parent",
    ),
    "service-runtime": (lambda m: _service(m).update(runtime="runc"), "sets runtime"),
    "service-links": (lambda m: _service(m).update(links=["india-api"]), "sets links"),
    "service-sysctls": (
        lambda m: _service(m).update(sysctls={"net.ipv4.ip_forward": "1"}),
        "sets sysctls",
    ),  # fmt: skip
    "service-level-secrets": (
        lambda m: _service(m).update(secrets=[{"source": "x"}]),
        "sets secrets",
    ),  # fmt: skip
    "service-unknown-future-key": (
        lambda m: _service(m).update(provider={"type": "model"}),
        "sets provider",
    ),
    "top-level-extension": (lambda m: m.update({"x-anything": {"a": 1}}), "top-level x-anything"),
    "top-level-include": (lambda m: m.update(include=["other.yaml"]), "top-level include"),
    "aliased-credential-name": (
        lambda m: _service(m)["environment"].update(
            {"ANALYTICS_TOKEN": "SENTINEL-NOT-A-REAL-TOKEN"}
        ),
        "passes provider credential variable ANALYTICS_TOKEN",
    ),
    "northstar-named-credential": (
        lambda m: _service(m)["environment"].update(
            {"NORTHSTAR_UPSTOX_TOKEN": "SENTINEL-NOT-A-REAL-TOKEN"}
        ),
        "passes provider credential variable NORTHSTAR_UPSTOX_TOKEN",
    ),
    "non-northstar-variable": (
        lambda m: _service(m)["environment"].update({"HTTPS_PROXY": "http://proxy:3128"}),
        "passes variable HTTPS_PROXY, which acceptance does not allow",
    ),
    "mount-bind-options": (
        lambda m: _service(m)["volumes"][0].update(bind={"propagation": "rshared"}),
        "sets bind, which acceptance does not allow",
    ),
    "mount-volume-subpath": (
        lambda m: _service(m)["volumes"][0].update(volume={"subpath": "x"}),
        "sets volume, which acceptance does not allow",
    ),
    "tmpfs-mount": (
        lambda m: _service(m)["volumes"].append({"type": "tmpfs", "target": "/tmp"}),  # noqa: S108
        "mounts tmpfs '' at '/tmp'",
    ),
}


@_windows
@pytest.mark.parametrize("case", sorted(_UNSAFE_MODELS))
def test_an_unsafe_acceptance_model_is_refused_before_running(harness: Harness, case) -> None:
    change, fragment = _UNSAFE_MODELS[case]
    harness.scenario(stdout=_WAITING, config_json=json.dumps(_unsafe(change)))

    result = _execute(harness, _acceptance(harness))

    assert result.returncode == 11
    assert harness.run_calls() == []
    status = harness.status()
    assert (status["outcome"], status["exitClass"], status["projectName"]) == (
        "FAILED", "DEPLOYMENT_INVALID", _ACCEPTANCE
    )  # fmt: skip
    assert status["reason"].startswith("acceptance configuration refused: ")
    assert fragment in status["reason"]
    for artifact in (_read(harness.log_files()[0]), result.stdout, result.stderr):
        assert "SENTINEL-NOT-A-REAL-TOKEN" not in artifact


@_windows
@pytest.mark.parametrize(
    "scenario",
    [{"config_json": "not json"}, {"config_json": "{}", "config_json_exit": 1}],
    ids=["not-json", "inspection-failed"],
)
def test_an_uninspectable_acceptance_model_is_refused(harness: Harness, scenario) -> None:
    harness.scenario(stdout=_WAITING, **scenario)

    result = _execute(harness, _acceptance(harness))

    assert result.returncode == 11
    assert harness.run_calls() == []
    assert harness.status()["exitClass"] == "DEPLOYMENT_INVALID"


@_windows
def test_compose_renderings_of_unset_settings_are_accepted(harness: Harness) -> None:
    """Null, false and empty values are how Compose renders settings that are not in force."""
    model = _model()
    _service(model).update(entrypoint=None, privileged=False, cap_add=[], labels={})
    _service(model)["volumes"][0].update(read_only=False, volume={})
    model["networks"]["acceptance"].update(ipam={}, external=False)
    model["volumes"]["acceptance-data"].update(driver="local", driver_opts={})
    harness.scenario(stdout=_WAITING, config_json=json.dumps(model))

    result = _execute(harness, _acceptance(harness))

    assert result.returncode == 0, result.stdout + result.stderr
    assert len(harness.run_calls()) == 1


# ---------------------------------------------------------------------------
# M1: acceptance paths are resolved, local and never production's, before anything
# ---------------------------------------------------------------------------


def _refused_before_anything(harness: Harness, result, fragment: str) -> None:
    assert result.returncode == 11, result.stdout + result.stderr
    assert "REFUSED (exit 11)" in result.stderr
    assert fragment in result.stderr, result.stderr
    assert harness.calls() == []
    assert not harness.logs.exists()


def _unc(path: Path) -> str:
    text = str(path)
    return f"\\\\localhost\\{text[0]}$" + text[2:]


@_windows
@pytest.mark.parametrize(
    "spelling",
    [_unc, lambda p: "\\\\?\\" + str(p), lambda p: "\\\\.\\" + str(p),
     lambda p: "//localhost/" + str(p)[0] + "$" + str(p)[2:].replace("\\", "/")],
    ids=["unc-admin-share", "device-namespace", "device-path", "forward-slash-unc"],
)  # fmt: skip
@pytest.mark.parametrize("which", ["LogDirectory", "DeploymentDirectory"])
def test_unc_and_device_paths_are_refused(harness: Harness, spelling, which: str) -> None:
    logs = spelling(harness.logs) if which == "LogDirectory" else harness.logs
    deployment = spelling(harness.deployment) if which == "DeploymentDirectory" else None
    command = _acceptance(harness, deployment=deployment, logs=logs)

    _refused_before_anything(harness, _execute(harness, command), "is a UNC or device path")


@_windows
@pytest.mark.parametrize("spelling", ["relative logs", "C:relative", "\\rooted\\no\\drive"])
def test_paths_that_are_not_absolute_drive_paths_are_refused(harness: Harness, spelling) -> None:
    result = _execute(harness, _acceptance(harness, logs=spelling))

    _refused_before_anything(harness, result, "is not an absolute local drive path")


def _junction(link: Path, target: Path) -> None:
    created = subprocess.run(  # noqa: S603 - a junction inside this test's temporary directory
        ["cmd", "/c", "mklink", "/J", str(link), str(target)],  # noqa: S607
        capture_output=True, text=True, check=False,
    )  # fmt: skip
    assert created.returncode == 0, created.stdout + created.stderr


def _symlink_or_skip(link: Path, target: Path) -> None:
    try:
        os.symlink(target, link, target_is_directory=True)
    except OSError as error:  # needs Developer Mode or the symbolic link privilege
        pytest.skip(f"directory symbolic links are not permitted here: {error}")


def _copied_checkout(tmp_path: Path) -> tuple[Path, Path]:
    """A copy of the wrapper whose own 'production' deployment directory is temporary."""
    production = tmp_path / "copied checkout" / "deploy" / "india"
    (production / "windows").mkdir(parents=True)
    shutil.copy2(_WRAPPER, production / "windows" / _WRAPPER.name)
    return production, production / "windows" / _WRAPPER.name


@_windows
@pytest.mark.parametrize("link", ["junction", "symlink"])
@pytest.mark.parametrize("inside", [False, True], ids=["the-directory", "beneath-it"])
def test_a_linked_alias_of_the_production_log_directory_is_refused(
    harness: Harness, tmp_path: Path, link: str, inside: bool
) -> None:
    appdata = tmp_path / "appdata (local)"
    production_logs = appdata / "Northstar" / "india-operations"
    production_logs.mkdir(parents=True)
    alias = tmp_path / "innocent logs"
    (_junction if link == "junction" else _symlink_or_skip)(alias, production_logs)
    try:
        logs = alias / "nested" if inside else alias
        env = {**harness.environment(), "LOCALAPPDATA": str(appdata)}

        result = _execute(harness, _acceptance(harness, logs=logs), env=env)

        assert result.returncode == 11, result.stdout + result.stderr
        assert "overlaps the production log directory" in result.stderr
        assert harness.calls() == []
        assert list(production_logs.iterdir()) == []
    finally:
        os.rmdir(alias)  # removes the link only, never its target


@_windows
@pytest.mark.parametrize("link", ["junction", "symlink"])
def test_a_linked_alias_of_the_production_deployment_is_refused(
    harness: Harness, tmp_path: Path, link: str
) -> None:
    production, wrapper = _copied_checkout(tmp_path)
    alias = tmp_path / "innocent deployment"
    (_junction if link == "junction" else _symlink_or_skip)(alias, production)
    try:
        command = _acceptance(harness, deployment=alias)
        command[command.index(str(_WRAPPER))] = str(wrapper)

        result = _execute(harness, command)

        assert result.returncode == 11, result.stdout + result.stderr
        assert "overlaps the production deployment directory" in result.stderr
        assert harness.calls() == []
    finally:
        os.rmdir(alias)


@_windows
def test_a_dangling_link_is_refused_rather_than_followed(harness: Harness, tmp_path: Path) -> None:
    target = tmp_path / "gone"
    target.mkdir()
    alias = tmp_path / "dangling"
    _junction(alias, target)
    target.rmdir()
    try:
        result = _execute(harness, _acceptance(harness, logs=alias / "logs"))

        _refused_before_anything(harness, result, "could not be resolved safely")
        assert not target.exists()
    finally:
        os.rmdir(alias)


@_windows
def test_a_junctioned_acceptance_path_elsewhere_is_allowed(
    harness: Harness, tmp_path: Path
) -> None:
    """Resolution is not a blanket refusal of links: a link to an unrelated place is fine."""
    real = tmp_path / "real logs"
    real.mkdir()
    alias = tmp_path / "linked logs"
    _junction(alias, real)
    harness.scenario(stdout=_WAITING, config_json=json.dumps(_model()))
    try:
        result = _execute(harness, _acceptance(harness, logs=alias))

        assert result.returncode == 0, result.stdout + result.stderr
        assert json.loads(_read(real / "last-run.json"))["projectName"] == _ACCEPTANCE
    finally:
        os.rmdir(alias)


@_windows
@pytest.mark.parametrize(
    "existing",
    [
        {"schema": "northstar.india-operations.last-run/1", "outcome": "COMPLETED"},
        {"schema": "northstar.india-operations.last-run/1", "projectName": "northstar-india"},
        {"schema": "northstar.india-operations.last-run/1",
         "projectName": "northstar-india-acceptance-other0002"},
        "not json at all",
    ],
    ids=["stage-1-production", "production", "another-session", "unreadable"],
)  # fmt: skip
def test_an_existing_foreign_status_file_is_never_replaced(harness: Harness, existing) -> None:
    harness.logs.mkdir()
    status = harness.logs / "last-run.json"
    status.write_text(existing if isinstance(existing, str) else json.dumps(existing), "utf-8")
    before = status.read_bytes()

    result = _execute(harness, _acceptance(harness))

    assert result.returncode == 11, result.stdout + result.stderr
    assert "already holds a last-run.json that is not from" in result.stderr
    assert harness.calls() == []
    assert status.read_bytes() == before
    assert sorted(p.name for p in harness.logs.iterdir()) == ["last-run.json"]


@_windows
def test_this_sessions_own_status_file_may_be_replaced(harness: Harness) -> None:
    harness.scenario(stdout=_WAITING, config_json=json.dumps(_model()))
    assert _execute(harness, _acceptance(harness)).returncode == 0

    result = _execute(harness, _acceptance(harness))

    assert result.returncode == 0, result.stdout + result.stderr
    assert len(harness.run_calls()) == 2


# ---------------------------------------------------------------------------
# Production default: none of the acceptance guards apply
# ---------------------------------------------------------------------------


@_windows
def test_the_production_default_keeps_working_through_links_and_stage_1_status(
    harness: Harness, tmp_path: Path
) -> None:
    """A junctioned log directory and a Stage 1 last-run.json are fine in production."""
    real = tmp_path / "real production logs"
    real.mkdir()
    (real / "last-run.json").write_text(
        json.dumps({"schema": "northstar.india-operations.last-run/1", "outcome": "COMPLETED"}),
        encoding="utf-8",
    )
    alias = tmp_path / "linked production logs"
    _junction(alias, real)
    harness.scenario(stdout=_WAITING)
    try:
        command = harness.command()
        command[command.index(str(harness.logs))] = str(alias)

        result = _execute(harness, command)

        assert result.returncode == 0, result.stdout + result.stderr
        status = json.loads(_read(real / "last-run.json"))
        assert (status["projectName"], status["outcome"]) == ("northstar-india", "WAITING")
        assert not [c for c in harness.calls() if "--format" in c and "config" in c]
    finally:
        os.rmdir(alias)


def test_acceptance_guards_are_reachable_only_from_acceptance_mode() -> None:
    """Add-Type and every acceptance check are called only under $IsAcceptance."""
    text = _read(_WRAPPER)
    assert text.count("Add-Type") == 1
    resolver = text[text.index("function Resolve-AcceptancePath") :]
    assert resolver.index("Add-Type") < resolver.index("\nfunction ")
    script = _directives(text)
    body = script[script.index("\nfunction Invoke-Operation") :]
    for guard in ("Get-AcceptanceRefusal", "Get-AcceptanceModelProblems"):
        (call,) = [m.start() for m in re.finditer(re.escape(guard), body)]
        assert "if ($IsAcceptance) {" in body[max(0, call - 600) : call], guard
    for helper in ("Resolve-AcceptancePath", "Get-AcceptancePathRefusal"):
        assert helper not in body, helper  # called only from Get-AcceptanceRefusal


# ---------------------------------------------------------------------------
# The harness's own model gate: the same adversarial cases, without Docker
# ---------------------------------------------------------------------------


def _acceptance_module():
    """Import the opt-in module by path; its import is side-effect free (asserted below)."""
    import importlib.util

    name = "_northstar_docker_acceptance_under_test"
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, _DOCKER_ACCEPTANCE)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name]


def _harness_case():
    module = _acceptance_module()
    session = module.Session(
        "abc123def456", "northstar-india-acceptance-abc123def456",
        "northstar-india-acceptance-abc123def456-data",
        "northstar-india-acceptance-abc123def456-net", Path("unused"), {},
    )  # fmt: skip
    service = module._operations_service(session, ["northstar", "operations", "daily"])
    service["networks"] = {"acceptance": None}
    service["volumes"][0]["volume"] = {}
    model = json.loads(module._compose(session, {"india-operations": service}))
    model["networks"]["acceptance"]["ipam"] = {}
    return module, session, model


_HARNESS_UNSAFE = {
    "volume-bind-alias-of-production": lambda m: m["volumes"]["acceptance-data"].update(
        driver="local", driver_opts=_BIND_ALIAS),
    "volume-unknown-driver": lambda m: m["volumes"]["acceptance-data"].update(driver="rclone"),
    "volume-external": lambda m: m["volumes"]["acceptance-data"].update(external=True),
    "network-driver-opts": lambda m: m["networks"]["acceptance"].update(driver_opts={"a": "b"}),
    "network-macvlan": lambda m: m["networks"]["acceptance"].update(driver="macvlan"),
    "network-ipam": lambda m: m["networks"]["acceptance"].update(ipam={"config": [{}, {}]}),
    "network-not-internal": lambda m: m["networks"]["acceptance"].update(internal=False),
    "bind-mount": lambda m: _service(m)["volumes"].append(
        {"type": "bind", "source": "C:\\", "target": "/host"}),
    "extra-hosts": lambda m: _service(m).update(extra_hosts=["a=1.2.3.4"]),
    "top-level-extension": lambda m: m.update({"x-anything": {"a": 1}}),
    "foreign-variable": lambda m: _service(m)["environment"].update(ANALYTICS_TOKEN="x"),
    "attachment-settings": lambda m: _service(m).update(networks={"acceptance": {"x": ["a"]}}),
    "not-a-mapping": lambda m: m.update(volumes=["acceptance-data"]),
}  # fmt: skip


def test_the_harness_gate_accepts_its_own_rendered_model() -> None:
    module, session, model = _harness_case()

    assert module._model_problems(session, model, {"india-operations"}) == []


@pytest.mark.parametrize("case", sorted(_HARNESS_UNSAFE))
def test_the_harness_gate_refuses_unsafe_models(case: str) -> None:
    module, session, model = _harness_case()
    _HARNESS_UNSAFE[case](model)

    assert module._model_problems(session, model, {"india-operations"}) != []
    with pytest.raises(AssertionError, match="rendered model refused"):
        module._verify_model(session, model, {"india-operations"})


# ---------------------------------------------------------------------------
# L4: the opt-in is enforced at setup and by every Docker call, not only by a mark
# ---------------------------------------------------------------------------


def test_the_acceptance_fixture_checks_the_opt_in_before_anything_else() -> None:
    tree = ast.parse(_read(_DOCKER_ACCEPTANCE))
    (fixture,) = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "acceptance"]
    first = fixture.body[0]
    assert isinstance(first, ast.Expr) and first.value.func.id == "_require_opt_in"


def test_the_acceptance_helpers_refuse_without_the_opt_in(monkeypatch) -> None:
    module = _acceptance_module()
    monkeypatch.delenv("NORTHSTAR_DOCKER_ACCEPTANCE", raising=False)
    session = _harness_case()[1]

    with pytest.raises(pytest.skip.Exception, match="NORTHSTAR_DOCKER_ACCEPTANCE=1"):
        module._require_opt_in()
    with pytest.raises(RuntimeError, match="refusing to call Docker"):
        module._docker(session, "version")
    with pytest.raises(RuntimeError, match="refusing to call Docker"):
        module._start_volume_events(session)
    monkeypatch.setenv("NORTHSTAR_DOCKER_ACCEPTANCE", "yes")
    with pytest.raises(RuntimeError, match="refusing to call Docker"):
        module._docker(session, "version")


# ---------------------------------------------------------------------------
# The real-Docker acceptance module is opt-in and inert otherwise
# ---------------------------------------------------------------------------


def test_the_docker_acceptance_module_does_nothing_at_import() -> None:
    text = _read(_DOCKER_ACCEPTANCE)
    tree = ast.parse(text)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.ClassDef):
            continue
        for call in ast.walk(node):
            if isinstance(call, ast.Call):
                name = getattr(call.func, "attr", getattr(call.func, "id", ""))
                assert name not in {"run", "Popen", "_docker", "_gate", "_render", "system"}, name
    assert 'os.environ.get("NORTHSTAR_DOCKER_ACCEPTANCE") == "1"' in text
    assert "pytestmark = pytest.mark.skipif(" in text
    # It never reads, copies or references the production Compose file.
    assert '"india" / "compose.yaml"' not in text
    assert "deploy/india/compose.yaml" not in text


@_windows
def test_the_docker_acceptance_module_is_skipped_without_the_opt_in(tmp_path: Path) -> None:
    """Collected and run without the opt-in, and with no docker on PATH: all skipped."""
    env = {key: value for key, value in os.environ.items() if not key.upper().startswith(
        ("NORTHSTAR_", "DOCKER_", "COMPOSE_", "UPSTOX_"))}  # fmt: skip
    env["PATH"] = os.pathsep.join(
        [str(Path(sys.executable).parent), str(Path(os.environ["SYSTEMROOT"]) / "System32")]
    )

    result = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "pytest", str(_DOCKER_ACCEPTANCE), "-q", "-rs",
         "-p", "no:cacheprovider", "--basetemp", str(tmp_path / "pytest")],
        cwd=_API, env=env, capture_output=True, text=True, timeout=300, check=False,
    )  # fmt: skip

    assert result.returncode == 0, result.stdout + result.stderr
    assert re.search(r"\b\d+ skipped\b", result.stdout), result.stdout
    assert not re.search(r"\b(passed|failed|error|errors)\b", result.stdout), result.stdout
    assert "real-Docker acceptance is opt-in" in result.stdout
