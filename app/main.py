"""Application startup.

Run:            uvicorn app.main:app --reload
Swagger UI:     http://localhost:8000/docs      (interactive testing)

create_app wires the layers: one shared httpx client for the upstream
Hospital Directory API, the in-memory batch store, and the engine. Tests
inject a mock transport to stand in for the upstream.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Optional

import httpx
from fastapi import FastAPI

from app.api.error_handlers import register_error_handlers
from app.api.hospital_bulk_route import router
from app.core import config
from app.dao.batch_dao import BatchDao
from app.dao.hospital_api_dao import HospitalApiDao
from app.dto.hospital_bulk_dto import HealthData, HealthResponse
from app.module.bulk_engine import BulkEngine


def create_app(upstream_transport: Optional[httpx.AsyncBaseTransport] = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        client = httpx.AsyncClient(
            base_url=config.HOSPITAL_API_BASE_URL,
            timeout=config.UPSTREAM_TIMEOUT_SECONDS,
            transport=upstream_transport,
        )
        engine = BulkEngine(HospitalApiDao(client), BatchDao())
        app.state.engine = engine
        try:
            yield
        finally:
            await engine.shutdown()  # don't leak background tasks
            await client.aclose()

    app = FastAPI(
        title="Hospital Bulk Processing API",
        version=config.APP_VERSION,
        description=(
            "Accepts CSV uploads of hospitals and processes them against the "
            "deployed Hospital Directory API: each row is created under one "
            "batch ID, then the whole batch is activated atomically. "
            "Processing runs in the background; progress is available via "
            "polling (GET /hospitals/bulk/{batch_id}) and SSE streaming. "
            "Batches can be paused, and failed/paused batches resumed. "
            "Test interactively at /docs."
        ),
        lifespan=lifespan,
    )
    app.include_router(router)
    register_error_handlers(app)

    @app.get("/health", response_model=HealthResponse, tags=["health"])
    def health() -> HealthResponse:
        return HealthResponse(status="ok", data=HealthData(version=config.APP_VERSION))

    return app


app = create_app()
