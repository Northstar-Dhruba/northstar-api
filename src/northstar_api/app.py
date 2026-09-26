"""Create and configure the Northstar FastAPI application."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from northstar_api.routers.analysis import router
from northstar_api.routers.futures import FuturesDashboard
from northstar_api.routers.futures import router as futures_router
from northstar_api.runtime import DatabaseRuntime, build_database_runtime
from northstar_api.settings import DashboardSettings, load_dashboard_settings


def create_app(
    settings: DashboardSettings | None = None,
    *,
    runtime: DatabaseRuntime | None = None,
    runtime_factory: Callable[[Path], DatabaseRuntime] = build_database_runtime,
) -> FastAPI:
    """Create the application with all route registrations.

    Without settings the futures routes answer 503. With settings, the database
    runtime is the one supplied, or is built once at startup -- never on import.
    """

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        if settings is not None and application.state.futures is None:
            application.state.futures = FuturesDashboard(
                settings, runtime_factory(settings.database)
            )
        yield

    app = FastAPI(title="Northstar Intelligence MVP", version="0.1.0", lifespan=lifespan)
    app.state.futures = (
        FuturesDashboard(settings, runtime)
        if settings is not None and runtime is not None
        else None
    )
    if settings is not None:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=[settings.web_origin],
            allow_methods=["GET"],
            allow_headers=[],
        )
    app.include_router(router)
    app.include_router(futures_router)
    return app


# Only environment parsing happens on import; the database is opened at startup.
app = create_app(load_dashboard_settings(os.environ))

__all__ = ["app", "create_app"]
