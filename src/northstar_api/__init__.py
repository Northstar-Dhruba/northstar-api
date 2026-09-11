"""Northstar API package for the Intelligence MVP."""

from northstar_api.app import app


def main() -> None:
    """Run the API via uvicorn."""
    import uvicorn

    uvicorn.run("northstar_api.app:app", host="0.0.0.0", port=8000, reload=False)


__all__ = ["app", "main"]
