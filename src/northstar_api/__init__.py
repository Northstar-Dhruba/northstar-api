"""Northstar API package for the Intelligence MVP."""

from __future__ import annotations

from typing import Any


def main() -> None:
    """Run the API via uvicorn."""
    import uvicorn

    uvicorn.run("northstar_api.app:app", host="0.0.0.0", port=8000, reload=False)


def __getattr__(name: str) -> Any:
    # Lazy, so the CLI never builds the web app or parses dashboard settings.
    if name == "app":
        from northstar_api.app import app

        return app
    raise AttributeError(f"module 'northstar_api' has no attribute {name!r}")


__all__ = ["app", "main"]
