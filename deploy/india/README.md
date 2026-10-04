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
