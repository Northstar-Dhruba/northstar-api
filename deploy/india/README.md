# Northstar Indian Futures deployment (NIFTY / NSE / Upstox)

An isolated paper-trading stack beside the CME/ES stack in `../`. The full
operator runbook -- first deployment, bootstrap, go-live, operator-approved
finality, backups, rollback, rollover and the November blocker -- is
`northstar-docs/operations/Indian-Futures-Deployment-Runbook.md`.

## Isolation from the CME stack

|                 | CME stack (unchanged)                   | Indian stack (this directory)                  |
|-----------------|-----------------------------------------|------------------------------------------------|
| Checkout        | `/opt/northstar`                        | `/opt/northstar-india` (separate checkout)     |
| Compose project | `northstar`                             | `northstar-india`                              |
| Services        | `api`, `operations`, `web`              | `india-api`, `india-operations`, `india-web`   |
| Images          | `northstar-api:local`, `northstar-web:local` | `northstar-api:india`, `northstar-web:india` |
| Data volume     | `northstar_northstar-data`              | `northstar-india-data`                         |
| Environment     | `deploy/.env`                           | `deploy/india/.env`                            |
| systemd         | `northstar-daily.{service,timer}`       | `northstar-india-daily.{service,timer}`        |
| Site            | Caddy on 80/443                         | Caddy on host loopback `127.0.0.1:8081`        |

Both stacks keep the database at `/data/northstar.sqlite3` inside their own
container; the volume is what separates them.

| Service            | Image                 | Role                                                         |
|--------------------|-----------------------|--------------------------------------------------------------|
| `india-web`        | `northstar-web:india` | Caddy with `./Caddyfile`: basic auth, static dashboard, and only `GET`/`HEAD` on `/api/health`, `/api/futures/dashboard` and `/api/futures/analysis` proxied |
| `india-api`        | `northstar-api:india` | Read-only dashboard and analysis API over the Indian volume; no provider secret |
| `india-operations` | `northstar-api:india` | `northstar operations daily` with the Upstox provider, run per timer tick, then removed |

## Files

- `compose.yaml` -- project `northstar-india`; reuses `../Dockerfile.api` and
  `../Dockerfile.web` under the Indian image tags.
- `Caddyfile` -- mounted read-only over the one baked into the web image.
- `.env.example` -- copy to `.env` here; secrets and deployment settings only.
- `systemd/northstar-india-daily.service`, `systemd/northstar-india-daily.timer`.
- `windows/Invoke-NorthstarIndiaOperations.ps1` -- the Windows Task Scheduler
  wrapper: preflight, one bounded `docker compose run --rm -T --no-deps
  india-operations`, cleanup of only its own container, UTC per-run logs and an
  atomic `last-run.json`. It never writes `.env` or changes finality.
- `windows/Register-NorthstarIndiaOperationsTask.ps1` and
  `windows/northstar-india-operations.task.xml` -- register the hourly task for
  the Docker Desktop user, **disabled**. Windows hosts follow runbook
  section 18 instead of the systemd commands below.

These files were authored and statically tested on the development machine.
`docker compose config`, Caddy validation, image builds, volume creation,
container startup, timer installation, the real `.env` and the database
bootstrap are still pending on the deployment machine (runbook section 2).

## Everyday commands (DEPLOYMENT MACHINE: Linux, Docker Compose v2, systemd)

```sh
cd /opt/northstar-india/northstar-api/deploy/india
docker compose build
docker compose up -d                                  # india-api + india-web
docker compose ps
sudo systemctl start northstar-india-daily.service    # run the operation now
journalctl -u northstar-india-daily.service -n 200    # its log
docker compose logs india-api                         # dashboard API log
docker compose stop                                   # stop the Indian stack; data kept
```

After editing `.env`, `docker compose up -d india-api` recreates only the
API with the new values; the next operation run reads `.env` afresh.

## Isolated real-Docker acceptance of the Windows wrapper (opt-in)

`tests/test_india_windows_docker_acceptance.py` exercises
`windows/Invoke-NorthstarIndiaOperations.ps1` against the real Docker Desktop
engine **without touching production**. It is skipped in every ordinary pytest
run and in CI; it runs only with the explicit opt-in below, on Windows, with a
reachable Linux engine and the already-built `northstar-api:india` image.

### Isolation boundary

Production's `compose.yaml` pins its database volume by explicit name
(`northstar-india-data`), so no copy of it is isolated under any project name.
The harness therefore never reads or copies that file. Each session:

- writes its own `compose.yaml` from scratch, with literal synthetic values, in
  new directories under the system temp directory (never the checkout, never
  `%LOCALAPPDATA%\Northstar\india-operations`);
- uses one unique project `northstar-india-acceptance-<id>`, one volume
  `<project>-data` and one **internal** network `<project>-net`, so no
  container can reach a provider;
- runs the existing `northstar-api:india` with `pull_policy: never`; it never
  builds, pulls, retags or removes an image;
- passes no provider token, finality `disabled`, go-live `2026-10-26` and
  synthetic strategy and portfolio identities, and scrubs the subprocess
  environment to an allowlist (no `UPSTOX_*`, `NORTHSTAR_*`, `COMPOSE_*` or
  `DOCKER_*` values are inherited);
- calls the wrapper with `-ProjectName`, `-DeploymentDirectory` and
  `-LogDirectory` every time.

The wrapper itself refuses an acceptance run (exit 11) before writing any log
or status file or calling Docker when:

- `-ProjectName` is not `northstar-india-acceptance-<id>`, or either directory
  is not passed explicitly;
- either directory is a UNC or device path (`\\server\share`, `\\?\`, `\\.\`)
  or not an absolute local drive path;
- either directory, resolved through junctions, symbolic links, subst drives
  and 8.3 names (a dangling link is refused), is not on a local fixed drive or
  overlaps the production deployment or log directory;
- the log directory already holds a `last-run.json` that is not this
  project's (production's, another session's or unreadable).

It then checks the rendered model against an allowlist: only session-named
local volumes without driver options (a session name alone proves nothing --
a local volume can bind production's data directory through `driver_opts`),
only internal session bridge networks without driver options or IPAM
settings, session volume mounts only, the `northstar-api:india` image with
`pull_policy: never`, `NORTHSTAR_*` variables whose names carry no credential,
and no other setting in force. The production default (`northstar-india`)
never takes these paths; the scheduled task passes no `-ProjectName`.

### Safety gate and cleanup

Before any operations container runs, the harness checks the Docker context
and Linux engine, requires the image to exist, inspects every rendered project
with `docker compose config --format json` against the same allowlists,
refuses to adopt any session resource that already exists, and records
production's containers (IDs, creation and start times), the
`northstar-india-data` volume identity, the image ID and the production
`last-run.json` hash. It then prints the session's exact project, volume,
network and directory, and writes them to `session.json`.

For the whole session a read-only `docker events` stream watches the
production volume (and, as a canary, the session volume). Production is
compared with its record, and the stream must show no event on the production
volume, after every destructive scenario and at the end. The run also fails
when the stream ends early or never sees the session volume's own mount. These
checks prove that no container attached to the production volume and that
production was not recreated; they are **not** proof that the production
SQLite content is unchanged, which cannot be read without attaching to the
volume or entering a production container, so the harness does neither.

Cleanup removes only session containers, by immutable ID after their Compose
project label is verified, and confirms each is gone; every step is attempted
even when another fails, and only a confirmed "not found" counts as absent. It
never prunes, never runs a production `compose down` and never deletes by
prefix. If a scenario or a cleanup step fails, containers are still removed
but the session volume, network and directories are kept as evidence, and
every error is reported after the production check, which always runs.

### Assumptions only a real run can confirm

The checks fail closed if any of these is wrong, but they are not yet observed
on the deployment machine: the shape of `docker compose config --format json`
(unset settings rendered as absent, null, false or empty; `internal: true`;
`{"<network>": null}` attachments); Compose labelling explicitly named volumes
and networks with the project; an internal network blocking all egress on
Docker Desktop, including `host.docker.internal`; volume `mount` events for
containers started by `compose run`; and `flock` holding across two containers
sharing a volume.

### Scenarios

A preflight only; B first real `operations daily` on the empty session database
(`WAITING`, exit 0, no decisions, orders or fills); C repeat (state
unchanged); D unreachable engine through a nonexistent endpoint or context
(exit 10, Docker Desktop is never stopped); E invalid configuration and
missing `.env` (exit 11); F CRLF `.env`; G bounded timeout of a sleeping
session service (exit 12, its container removed); H interrupted wrapper, both
the wrapper alone and its whole process tree (the stale `RUNNING` status and
the container's fate are recorded, then the container is removed); I the
real database lock held by a session container (`SKIPPED`, then `WAITING`); K
recovery with an intact, unlocked session database; L production unchanged.

### Running it (supervised, on the Docker Desktop host only)

Run it only when no production operation is in progress (the production task
should be disabled), from the `northstar-api` directory of a checkout:

```powershell
cd "<checkout>\northstar-api"
$env:NORTHSTAR_DOCKER_ACCEPTANCE = '1'
.\.venv\Scripts\python.exe -m pytest tests\test_india_windows_docker_acceptance.py -v -rs -p no:cacheprovider
Remove-Item Env:\NORTHSTAR_DOCKER_ACCEPTANCE
```

It takes several minutes (the timeout and interruption scenarios wait on real
containers). Without `NORTHSTAR_DOCKER_ACCEPTANCE=1` every test is skipped and
Docker is never called.
