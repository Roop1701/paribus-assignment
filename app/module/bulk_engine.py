"""Batch orchestration: create every hospital upstream, then activate.

A batch runs as a background asyncio task. Rows are created concurrently
under a single global semaphore (client-side rate limit shared by all
batches); per-call retry policy lives in the hospital DAO. Activation only
happens once every row is created, per the assignment workflow.

State split: batch/row state lives in the store behind BatchDao;
runtime-only state — running tasks, pause flags, run timers, SSE subscriber
queues — lives here, since it is meaningless across restarts. A batch with
failed rows, a failed activation, or a pause stays resumable, and resume
retries only what is missing.
"""
from __future__ import annotations

import asyncio
import time
from typing import Dict, List, Optional, Set

from ..core import config
from ..dao import batch_dao as states
from ..dao.batch_dao import BatchDao, BatchRecord, RowState
from ..dao.hospital_api_dao import HospitalApiDao, UpstreamError
from .csv_parser import CsvRow


class BatchConflictError(Exception):
    """The batch is in a state that does not allow the requested action."""


class ProgressBroker:
    """In-process pub/sub feeding the SSE stream, keyed by batch id."""

    def __init__(self) -> None:
        self._subscribers: Dict[str, List[asyncio.Queue]] = {}

    def subscribe(self, batch_id: str) -> "asyncio.Queue":
        queue: asyncio.Queue = asyncio.Queue()
        self._subscribers.setdefault(batch_id, []).append(queue)
        return queue

    def unsubscribe(self, batch_id: str, queue: "asyncio.Queue") -> None:
        queues = self._subscribers.get(batch_id, [])
        if queue in queues:
            queues.remove(queue)
        if not queues:
            self._subscribers.pop(batch_id, None)

    def publish(self, batch_id: str, event: dict) -> None:
        for queue in list(self._subscribers.get(batch_id, [])):
            queue.put_nowait(event)


class BulkEngine:
    def __init__(self, hospital_dao: HospitalApiDao, batch_dao: BatchDao) -> None:
        self.hospital_dao = hospital_dao
        self.batch_dao = batch_dao
        self.broker = ProgressBroker()
        self._semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_UPSTREAM_CALLS)
        self._tasks: Dict[str, asyncio.Task] = {}
        self._run_started: Dict[str, float] = {}
        self._pause_requested: Set[str] = set()

    # --- public API (called by routes) ---

    async def start_batch(self, csv_rows: List[CsvRow]) -> str:
        batch_id = await self.batch_dao.create(csv_rows)
        self._launch(batch_id)
        return batch_id

    async def resume_batch(self, batch: BatchRecord) -> None:
        if batch.status == states.BATCH_PROCESSING:
            raise BatchConflictError("batch is still processing")
        if batch.status not in states.RESUMABLE_STATUSES:
            raise BatchConflictError("batch already completed; nothing to resume")
        await self.batch_dao.reset_failed_rows(batch.batch_id)
        await self.batch_dao.set_status(batch.batch_id, states.BATCH_PROCESSING)
        self._launch(batch.batch_id)

    async def pause_batch(self, batch: BatchRecord) -> None:
        """Cooperative pause: in-flight upstream calls finish, rows not yet
        started stay 'pending'; the batch lands in 'paused' (resumable)."""
        if batch.status != states.BATCH_PROCESSING:
            raise BatchConflictError("only a processing batch can be paused")
        self._pause_requested.add(batch.batch_id)

    async def get_status(self, batch_id: str) -> Optional[dict]:
        batch = await self.batch_dao.get(batch_id)
        return self._snapshot(batch) if batch else None

    async def shutdown(self) -> None:
        for task in list(self._tasks.values()):
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)

    async def join(self, batch_id: str) -> None:
        """Await the background run, if one is active (tests, graceful stop)."""
        task = self._tasks.get(batch_id)
        if task:
            await asyncio.shield(task)

    # --- background processing ---

    def _launch(self, batch_id: str) -> None:
        self._run_started[batch_id] = time.monotonic()
        task = asyncio.create_task(self._process(batch_id))
        self._tasks[batch_id] = task
        task.add_done_callback(lambda _: self._tasks.pop(batch_id, None))

    async def _process(self, batch_id: str) -> None:
        activated = False
        try:
            batch = await self.batch_dao.get(batch_id)
            pending = [r for r in batch.rows if r.status == states.ROW_PENDING]
            await asyncio.gather(*(self._create_row(batch_id, row) for row in pending))
            batch = await self.batch_dao.get(batch_id)  # re-read row outcomes
            if batch_id in self._pause_requested and any(
                r.status == states.ROW_PENDING for r in batch.rows
            ):
                status = states.BATCH_PAUSED
            elif any(r.status == states.ROW_FAILED for r in batch.rows):
                status = states.BATCH_PARTIAL
            else:
                status = await self._activate(batch_id)
                activated = status == states.BATCH_COMPLETED
        except asyncio.CancelledError:
            raise  # shutdown mid-run; in-memory state dies with the process
        except Exception as exc:  # defensive: a batch must never stay "processing"
            await self._fail_pending_rows(batch_id, exc)
            status = states.BATCH_PARTIAL
        finally:
            self._pause_requested.discard(batch_id)
        elapsed = time.monotonic() - self._run_started.pop(batch_id, time.monotonic())
        await self.batch_dao.finish(
            batch_id, status=status, add_elapsed=elapsed, activated=activated
        )
        snapshot = await self.get_status(batch_id)
        event = "paused" if status == states.BATCH_PAUSED else "completed"
        self.broker.publish(batch_id, {"event": event, "data": snapshot})

    async def _create_row(self, batch_id: str, row: RowState) -> None:
        if batch_id in self._pause_requested:
            return  # stays 'pending'; picked up by resume
        async with self._semaphore:
            if batch_id in self._pause_requested:
                return
            try:
                hospital = await self.hospital_dao.create_hospital(
                    name=row.name,
                    address=row.address,
                    phone=row.phone,
                    batch_id=batch_id,
                )
                row_status, hospital_id, error = states.ROW_CREATED, hospital.get("id"), None
            except UpstreamError as exc:
                row_status, hospital_id, error = states.ROW_FAILED, None, str(exc)
        await self.batch_dao.update_row(
            batch_id, row.row_number, status=row_status, hospital_id=hospital_id, error=error
        )
        processed, failed, total = await self.batch_dao.row_counts(batch_id)
        self.broker.publish(
            batch_id,
            {
                "event": "progress",
                "data": {
                    "status": row_status,
                    "data": {
                        "row": row.row_number,
                        "name": row.name,
                        "hospital_id": hospital_id,
                        "error": error,
                        "processed_hospitals": processed,
                        "failed_hospitals": failed,
                        "total_hospitals": total,
                    },
                },
            },
        )

    async def _activate(self, batch_id: str) -> str:
        try:
            await self.hospital_dao.activate_batch(batch_id)
        except UpstreamError:
            return states.BATCH_ACTIVATION_FAILED  # rows stay "created"
        await self.batch_dao.activate_rows(batch_id)
        return states.BATCH_COMPLETED

    async def _fail_pending_rows(self, batch_id: str, exc: Exception) -> None:
        batch = await self.batch_dao.get(batch_id)
        for row in batch.rows:
            if row.status == states.ROW_PENDING:
                await self.batch_dao.update_row(
                    batch_id,
                    row.row_number,
                    status=states.ROW_FAILED,
                    error="internal error: {}".format(exc),
                )

    def _snapshot(self, batch: BatchRecord) -> dict:
        """{status, data} envelope shared by the status endpoint and SSE."""
        running = self._run_started.get(batch.batch_id)
        elapsed = batch.elapsed_seconds + (time.monotonic() - running if running else 0)
        return {
            "status": batch.status,
            "data": {
                "batch_id": batch.batch_id,
                "total_hospitals": len(batch.rows),
                "processed_hospitals": sum(
                    1
                    for r in batch.rows
                    if r.status in (states.ROW_CREATED, states.ROW_ACTIVATED)
                ),
                "failed_hospitals": sum(
                    1 for r in batch.rows if r.status == states.ROW_FAILED
                ),
                "processing_time_seconds": round(elapsed, 2),
                "batch_activated": batch.batch_activated,
                "hospitals": [
                    {
                        "row": r.row_number,
                        "hospital_id": r.hospital_id,
                        "name": r.name,
                        "status": r.status,
                        "error": r.error,
                    }
                    for r in batch.rows
                ],
            },
        }
