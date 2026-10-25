"""In-memory store for batch processing state.

The dict here stands in for a real database (in-memory persistence is
acceptable per the assignment). The DAO keeps the async method surface a
database-backed implementation would have, so a storage swap touches one
file. Runtime-only concerns (asyncio tasks, pause flags, SSE subscriber
queues, run timers) live in the engine, not here.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

if TYPE_CHECKING:  # type-only: the DAO layer must not import business modules
    from ..module.csv_parser import CsvRow

# Row lifecycle: pending -> created -> created_and_activated, or -> failed.
ROW_PENDING = "pending"
ROW_CREATED = "created"
ROW_ACTIVATED = "created_and_activated"
ROW_FAILED = "failed"

BATCH_PROCESSING = "processing"
BATCH_COMPLETED = "completed"
BATCH_PARTIAL = "completed_with_errors"        # some rows failed; not activated
BATCH_ACTIVATION_FAILED = "activation_failed"  # rows created; activate call failed
BATCH_PAUSED = "paused"                        # pause requested mid-run

RESUMABLE_STATUSES = frozenset(
    {BATCH_PARTIAL, BATCH_ACTIVATION_FAILED, BATCH_PAUSED}
)


@dataclass
class RowState:
    row_number: int  # 1-based CSV data row
    name: str
    address: str
    phone: Optional[str]
    status: str = ROW_PENDING
    hospital_id: Optional[int] = None
    error: Optional[str] = None


@dataclass
class BatchRecord:
    batch_id: str
    rows: List[RowState] = field(default_factory=list)
    status: str = BATCH_PROCESSING
    batch_activated: bool = False
    elapsed_seconds: float = 0.0  # accumulated across runs (resume restarts the clock)


class BatchDao:
    def __init__(self) -> None:
        self._batches: Dict[str, BatchRecord] = {}

    async def create(self, csv_rows: List["CsvRow"]) -> str:
        record = BatchRecord(
            batch_id=str(uuid.uuid4()),
            rows=[
                RowState(row_number=r.row, name=r.name, address=r.address, phone=r.phone)
                for r in csv_rows
            ],
        )
        self._batches[record.batch_id] = record
        return record.batch_id

    async def get(self, batch_id: str) -> Optional[BatchRecord]:
        return self._batches.get(batch_id)

    async def update_row(
        self,
        batch_id: str,
        row_number: int,
        *,
        status: str,
        hospital_id: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        row = self._row(batch_id, row_number)
        row.status = status
        row.hospital_id = hospital_id
        row.error = error

    async def row_counts(self, batch_id: str) -> Tuple[int, int, int]:
        """(processed, failed, total) for progress events."""
        rows = self._batches[batch_id].rows
        processed = sum(1 for r in rows if r.status in (ROW_CREATED, ROW_ACTIVATED))
        failed = sum(1 for r in rows if r.status == ROW_FAILED)
        return processed, failed, len(rows)

    async def reset_failed_rows(self, batch_id: str) -> None:
        for row in self._batches[batch_id].rows:
            if row.status == ROW_FAILED:
                row.status = ROW_PENDING
                row.error = None

    async def activate_rows(self, batch_id: str) -> None:
        for row in self._batches[batch_id].rows:
            if row.status == ROW_CREATED:
                row.status = ROW_ACTIVATED

    async def set_status(self, batch_id: str, status: str) -> None:
        self._batches[batch_id].status = status

    async def finish(
        self, batch_id: str, *, status: str, add_elapsed: float, activated: bool
    ) -> None:
        """Terminal transition: persist status, activation flag, accumulated time."""
        record = self._batches[batch_id]
        record.status = status
        record.batch_activated = activated
        record.elapsed_seconds += add_elapsed

    def _row(self, batch_id: str, row_number: int) -> RowState:
        for row in self._batches[batch_id].rows:
            if row.row_number == row_number:
                return row
        raise KeyError("row {} not in batch {}".format(row_number, batch_id))
