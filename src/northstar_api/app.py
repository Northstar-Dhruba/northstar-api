"""Create and configure the Northstar FastAPI application."""

from __future__ import annotations

from fastapi import FastAPI

from northstar_api.routers.analysis import router


def create_app() -> FastAPI:
    """Create the application with all route registrations."""
    app = FastAPI(title="Northstar Intelligence MVP", version="0.1.0")
    app.include_router(router)
    return app


app = create_app()

__all__ = ["app", "create_app"]
