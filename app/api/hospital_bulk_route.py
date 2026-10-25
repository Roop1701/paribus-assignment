"""Bulk processing routes. Orchestration lives in app.module.bulk_engine;
this layer validates input and shapes responses (including the SSE stream).

Routes are prefixed with /hospitals/bulk.
"""
from __future__ import annotations

import asyncio
import json
import uuid

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse

from ..core import config
from ..dao.batch_dao import BatchRecord
from ..dto.hospital_bulk_dto import (
    BatchStatusResponse,
    BulkAcceptedData,
    BulkAcceptedResponse,
    CsvValidationData,
    CsvValidationResponse,
    PauseAcceptedResponse,
    RowError,
)
from ..module.bulk_engine import BatchConflictError, BulkEngine
from ..module.csv_parser import ParsedCsv, parse_csv_bytes

router = APIRouter(prefix=config.API_PREFIX, tags=["bulk-processing"])

TERMINAL_EVENTS = ("completed", "paused")


def get_engine(request: Request) -> BulkEngine:
    return request.app.state.engine


@router.post("/validate", response_model=CsvValidationResponse)
async def validate_csv(file: UploadFile = File(...)) -> CsvValidationResponse:
    """Dry-run CSV validation; no hospitals are created."""
    parsed = await _read_and_parse(file)
    return CsvValidationResponse(
        status="valid" if parsed.valid else "invalid",
        data=CsvValidationData(
            total_hospitals=len(parsed.rows),
            errors=[RowError(row=e.row, error=e.error) for e in parsed.errors],
        ),
    )


@router.post("", status_code=202, response_model=BulkAcceptedResponse)
async def bulk_create(
    file: UploadFile = File(...), engine: BulkEngine = Depends(get_engine)
) -> BulkAcceptedResponse:
    """Accept a CSV, start background processing, return 202 with batch ID."""
    parsed = await _read_and_parse(file)
    if not parsed.valid:
        raise HTTPException(
            status_code=422,
            detail={
                "message": "CSV validation failed",
                "errors": [{"row": e.row, "error": e.error} for e in parsed.errors],
            },
        )
    batch_id = await engine.start_batch(parsed.rows)
    return _accepted(batch_id, len(parsed.rows))


@router.get("/{batch_id}", response_model=BatchStatusResponse)
async def batch_status(
    batch_id: uuid.UUID, engine: BulkEngine = Depends(get_engine)
) -> BatchStatusResponse:
    """Poll processing progress; terminal states carry the full results."""
    snapshot = await engine.get_status(str(batch_id))
    if snapshot is None:
        raise HTTPException(status_code=404, detail="batch not found")
    return BatchStatusResponse(**snapshot)


@router.get("/{batch_id}/stream")
async def batch_stream(
    batch_id: uuid.UUID, engine: BulkEngine = Depends(get_engine)
) -> StreamingResponse:
    """SSE progress stream: snapshot first, then per-row progress events,
    closing after the terminal 'completed'/'paused' event."""
    snapshot = await engine.get_status(str(batch_id))
    if snapshot is None:
        raise HTTPException(status_code=404, detail="batch not found")
    return StreamingResponse(
        _event_stream(engine, str(batch_id)),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


@router.post("/{batch_id}/pause", status_code=202, response_model=PauseAcceptedResponse)
async def pause_batch(
    batch_id: uuid.UUID, engine: BulkEngine = Depends(get_engine)
) -> PauseAcceptedResponse:
    """Request a cooperative pause: in-flight rows finish, the rest stay
    pending; the batch lands in 'paused' and can be resumed."""
    batch = await _get_batch(engine, batch_id)
    try:
        await engine.pause_batch(batch)
    except BatchConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return PauseAcceptedResponse(
        status="pausing",
        data=BulkAcceptedData(batch_id=batch.batch_id, total_hospitals=len(batch.rows)),
    )


@router.post("/{batch_id}/resume", status_code=202, response_model=BulkAcceptedResponse)
async def resume_batch(
    batch_id: uuid.UUID, engine: BulkEngine = Depends(get_engine)
) -> BulkAcceptedResponse:
    """Retry the failed/pending rows (and/or activation) of a partial,
    paused, or interrupted batch."""
    batch = await _get_batch(engine, batch_id)
    try:
        await engine.resume_batch(batch)
    except BatchConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return _accepted(batch.batch_id, len(batch.rows))


async def _read_and_parse(file: UploadFile) -> ParsedCsv:
    if file.filename and not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=422, detail="expected a .csv file")
    data = await file.read()
    if len(data) > config.MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="CSV file too large")
    return parse_csv_bytes(data)


async def _get_batch(engine: BulkEngine, batch_id: uuid.UUID) -> BatchRecord:
    batch = await engine.batch_dao.get(str(batch_id))
    if batch is None:
        raise HTTPException(status_code=404, detail="batch not found")
    return batch


def _accepted(batch_id: str, total: int) -> BulkAcceptedResponse:
    return BulkAcceptedResponse(
        status="processing",
        data=BulkAcceptedData(batch_id=batch_id, total_hospitals=total),
    )


def _sse(event: str, data: dict) -> str:
    return "event: {}\ndata: {}\n\n".format(event, json.dumps(data))


async def _event_stream(engine: BulkEngine, batch_id: str):
    queue = engine.broker.subscribe(batch_id)  # subscribe before snapshot: no gap
    try:
        snapshot = await engine.get_status(batch_id)
        yield _sse("snapshot", snapshot)
        while snapshot["status"] == "processing":
            try:
                event = await asyncio.wait_for(
                    queue.get(), timeout=config.SSE_HEARTBEAT_SECONDS
                )
            except asyncio.TimeoutError:
                yield ": keep-alive\n\n"
                continue
            yield _sse(event["event"], event["data"])
            if event["event"] in TERMINAL_EVENTS:
                break
    finally:
        engine.broker.unsubscribe(batch_id, queue)
