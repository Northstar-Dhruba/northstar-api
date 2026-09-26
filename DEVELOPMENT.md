# Development Guide

## Environment Setup

Requirements:

- Python 3.13
- `uv`

Synchronize the API environment and local workspace dependencies:

```powershell
uv sync
```

## Run FastAPI

Start the development API on port 8000:

```powershell
uv run python -m northstar_api
```

## Validation

```powershell
uv run ruff check .
uv run ruff format --check .
uv run python -m pytest
uv lock --check
```

Format when needed:

```powershell
uv run ruff format .
```

The API depends on the local Application, Core, and Infrastructure packages through editable workspace sources. Keep route handlers transport-only and validate behavior through ASGI-level tests.
