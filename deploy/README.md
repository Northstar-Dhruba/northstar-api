# Northstar deployment runbook

One host, one persistent local volume, one API process, one scheduled writer,
and a static web build. SQLite stays the database.

| Service      | Image                 | Role                                                                |
|--------------|-----------------------|---------------------------------------------------------------------|
| `web`        | `northstar-web:local` | Caddy: HTTPS, basic auth, static dashboard, `/api/*` proxy          |
| `api`        | `northstar-api:local` | `northstar-api` on `0.0.0.0:8000`, reachable only through `web`      |
| `operations` | `northstar-api:local` | `northstar operations daily`, run once per schedule, then removed   |

Both Python services mount the `northstar-data` volume at `/data`, where
`NORTHSTAR_DATABASE=/data/northstar.sqlite3` lives. The volume must be local
disk: never NFS, SMB or object storage, because SQLite locking is unreliable on
network filesystems.

## One-time setup

The images build from the workspace root, so the host needs all sibling
repositories checked out side by side (for example under `/opt/northstar`).

```sh
cd /opt/northstar/northstar-api/deploy
cp .env.example .env            # fill in every value; never commit .env
docker run --rm caddy:2-alpine caddy hash-password   # value for NORTHSTAR_BASIC_AUTH_HASH
docker compose build
```

Set the product economics once. The point value is yours to supply; nothing
here assumes one.

```sh
docker compose run --rm operations northstar economics set \
  --database /data/northstar.sqlite3 --product ES --exchange CME \
  --point-value <currency per point> --currency USD
```

Bootstrap enough completed history for the strategy warm-up (at least twenty
completed daily sessions). The daily job never chooses this window itself, and
refuses to run against an empty history.

```sh
docker compose run --rm operations northstar market-data sync \
  --database /data/northstar.sqlite3 --product ES --exchange CME \
  --expiration <YYYY-MM-DD> --start <YYYY-MM-DD> --end <last completed session>
```

## Start and verify

```sh
docker compose up -d
docker compose ps                                   # api should be healthy
curl -u <user> https://<site>/api/health            # {"status":"ok"}
curl -u <user> https://<site>/api/futures/dashboard
```

Then open `https://<site>/#futures` in a browser.

## Daily operation

```sh
sudo cp systemd/northstar-daily.service systemd/northstar-daily.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now northstar-daily.timer
journalctl -u northstar-daily.service              # the job's logs
```

Each run reads the clock once, syncs every completed session missing since the
latest persisted daily bar, and paper trades at that bar's instant. A rerun
with no new completed session is an idempotent no-op. Exit codes are the CLI's:
`4 DATA` after a completed paper session means P&L was unavailable (economics
missing); `6 PROVIDER` means the sync stopped, and earlier sessions it already
stored are kept for the next run to resume.

The timer's time is a deployment choice. The job checks each session's
resolved close, including early closes, but a passed close does not prove the
provider has finished publishing the session. Schedule with a buffer after the
product's normal close. Do not run a second writer (another timer, a manual
`paper run`) while the job is running.

## Backup

Stop the writer, then copy the file with SQLite's online backup, which is safe
while the API keeps reading.

```sh
sudo systemctl stop northstar-daily.timer
systemctl is-active northstar-daily.service        # wait until inactive
docker compose exec api python -c "import sqlite3; sqlite3.connect('/data/northstar.sqlite3').backup(sqlite3.connect('/data/backup.sqlite3'))"
docker compose cp api:/data/backup.sqlite3 ./northstar-$(date -u +%F).sqlite3
docker compose exec api rm /data/backup.sqlite3
sudo systemctl start northstar-daily.timer
```
