"""Request/response models for the bulk processing API.

Every response uses the same envelope: a top-level `status` (declared first
so it serializes first) followed by the payload under `data`.
"""
from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel

RowStatus = Literal["pending", "created", "created_and_activated", "failed"]
BatchStatus = Literal[
    "processing", "completed", "completed_with_errors", "activation_failed", "paused"
]


class RowError(BaseModel):
    row: int  # 1-based data row; 0 = file-level error
    error: str


class CsvValidationData(BaseModel):
    total_hospitals: int
    errors: List[RowError]


class CsvValidationResponse(BaseModel):
    status: Literal["valid", "invalid"]
    data: CsvValidationData


class BulkAcceptedData(BaseModel):
    batch_id: str
    total_hospitals: int


class BulkAcceptedResponse(BaseModel):
    """202 body: processing continues in the background; poll or stream."""

    status: Literal["processing"]
    data: BulkAcceptedData


class PauseAcceptedResponse(BaseModel):
    """202 body: pause is cooperative — in-flight rows finish first."""

    status: Literal["pausing"]
    data: BulkAcceptedData


class HospitalRowResult(BaseModel):
    row: int
    hospital_id: Optional[int]
    name: str
    status: RowStatus
    error: Optional[str] = None


class BatchStatusData(BaseModel):
    """The assignment's 'comprehensive processing results'."""

    batch_id: str
    total_hospitals: int
    processed_hospitals: int
    failed_hospitals: int
    processing_time_seconds: float
    batch_activated: bool
    hospitals: List[HospitalRowResult]


class BatchStatusResponse(BaseModel):
    status: BatchStatus
    data: BatchStatusData


class HealthData(BaseModel):
    version: str


class HealthResponse(BaseModel):
    status: str
    data: HealthData
