"""MayaCardio Cloud Receiver FastAPI Server (Phase 4A).

Exposes:
- POST /api/v1/ingest/batch: Atomic micro-batch ingestion, duplicate validation,
  bidirectional conflict rejection, and explicit contiguous cursor acknowledgement.
- GET /api/v1/status: Receiver health, active sessions, sample counts, and checkpoints.
- GET /api/v1/export?session=ID: Section 4 official NDJSON export stream.
- GET /api/v1/results?session=ID: Section 3 online window processing results stream.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

import secrets
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from pipeline.cloud.service import CloudProcessingService
from pipeline.cloud.storage import (
    CloudCheckpointRecord,
    CloudContradictoryLossConflictError,
    CloudIntegrityConflictError,
    CloudStorageEngine,
    CloudStorageError,
)
from pipeline.protocol_client import IngestBatch
from shared.protocol import (
    MAX_INT24,
    MIN_INT24,
    LossRecord,
)

logger = logging.getLogger("pipeline.cloud.server")


# ==============================================================================
# Pydantic Request & Response Schemas
# ==============================================================================

class LossRecordInput(BaseModel):
    """Schema for incoming loss interval."""
    model_config = ConfigDict(extra="forbid")

    from_index: int = Field(ge=0, description="Start index of lost range inclusive")
    to_index: Optional[int] = Field(default=None, description="End index of lost range inclusive, or None if open-ended")
    reason: str = Field(min_length=1, description="Reason for data loss")

    @field_validator("to_index")
    @classmethod
    def validate_range(cls, v: Optional[int], info: Any) -> Optional[int]:
        if v is not None:
            from_idx = info.data.get("from_index")
            if from_idx is not None and v < from_idx:
                raise ValueError(f"to_index ({v}) cannot be less than from_index ({from_idx})")
        return v


class IngestBatchInput(BaseModel):
    """Schema for POST /api/v1/ingest/batch payload."""
    model_config = ConfigDict(extra="forbid")

    session_id: str = Field(min_length=1, description="Unique session identifier")
    sensor: int = Field(description="Sensor identifier (1 or 2)")
    first_index: int = Field(ge=0, description="First sample index in this batch")
    count: Optional[int] = Field(default=None, description="Optional sample count; must match len(data) if provided")
    data: list[list[int]] = Field(default_factory=list, description="List of channel sample values")
    lost: list[LossRecordInput] = Field(default_factory=list, description="List of authoritative loss intervals")
    session_end: bool = Field(default=False, description="Flag indicating clean stream completion")

    @field_validator("sensor")
    @classmethod
    def validate_sensor(cls, v: int) -> int:
        if v not in (1, 2):
            raise ValueError(f"sensor must be 1 or 2, got {v}")
        return v

    @field_validator("data")
    @classmethod
    def validate_channel_data(cls, v: list[list[int]], info: Any) -> list[list[int]]:
        sensor = info.data.get("sensor")
        expected_channels = 3 if sensor == 1 else 2
        for sample in v:
            if len(sample) != expected_channels:
                raise ValueError(
                    f"Sensor {sensor} requires exactly {expected_channels} channels per sample, got {len(sample)}"
                )
            for ch in sample:
                if not (MIN_INT24 <= ch <= MAX_INT24):
                    raise ValueError(f"Sample value {ch} exceeds 24-bit integer limits [{MIN_INT24}, {MAX_INT24}]")
        return v

    @model_validator(mode="after")
    def validate_count_matches_data(self) -> "IngestBatchInput":
        if self.count is not None:
            if self.count < 0:
                raise ValueError(f"count ({self.count}) must be non-negative")
            if self.count != len(self.data):
                raise ValueError(f"count ({self.count}) does not match length of data ({len(self.data)})")
        return self


class BatchResponse(BaseModel):
    """Schema for successful batch ingestion acknowledgement."""
    status: str = "ok"
    session_id: str
    sensor: int
    contiguous_persisted_index: int
    contiguous_resolved_index: int
    highest_observed_index: int
    samples_inserted: int
    duplicates_ignored: int


# ==============================================================================
# FastAPI Application Factory
# ==============================================================================

def verify_api_key(
    request: Request,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
    authorization: Optional[str] = Header(None, alias="Authorization"),
) -> None:
    """Optional authentication check.

    If CLOUD_API_KEY environment variable is configured, requests must provide a
    matching key via 'X-API-Key' header or 'Authorization: Bearer <key>'.
    If CLOUD_API_KEY is unset or empty, authentication is disabled (local/test mode).
    Public platform health probes (/healthz, /) are exempt to support Render health checks.
    """
    if request.url.path in ("/healthz", "/"):
        return

    expected = os.environ.get("CLOUD_API_KEY")
    if not expected:
        return

    provided = x_api_key
    if not provided and authorization:
        parts = authorization.split(" ", 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            provided = parts[1]

    if not provided or not secrets.compare_digest(provided, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized: Invalid or missing API key",
            headers={"WWW-Authenticate": "Bearer"},
        )


def create_cloud_app(
    db_path: str | Path = "biosignal_cloud.db",
    storage: Optional[CloudStorageEngine] = None,
    database_url: Optional[str] = None,
) -> FastAPI:
    """Factory function creating configured FastAPI cloud receiver application."""
    app = FastAPI(
        title="MayaCardio Cloud Biosignal Receiver",
        description="Durable cloud receiver and Section 3 online signal processing service.",
        version="0.1.0",
        dependencies=[Depends(verify_api_key)],
    )

    if storage is None:
        db_url = database_url or os.environ.get("DATABASE_URL") or os.environ.get("CLOUD_DATABASE_URL")
        storage = CloudStorageEngine(db_path=db_path, database_url=db_url)
    processing_service = CloudProcessingService(storage=storage)

    # Attach to app state for lifecycle/testing access
    app.state.storage = storage
    app.state.service = processing_service

    @app.get("/healthz", status_code=status.HTTP_200_OK, summary="Liveness probe for cloud platform")
    def healthz() -> dict[str, str]:
        return {"status": "healthy"}

    @app.get("/", status_code=status.HTTP_200_OK, summary="Root health probe")
    def root() -> dict[str, str]:
        return {"status": "healthy", "service": "MayaCardio Cloud Ingestion Receiver"}

    @app.post(
        "/api/v1/ingest/batch",
        response_model=BatchResponse,
        status_code=status.HTTP_200_OK,
        summary="Ingest a validated biosignal batch",
    )
    def ingest_batch(payload: IngestBatchInput) -> BatchResponse:
        """Atomically persist incoming samples and loss records.

        Enforces:
        - Rejection of conflicting duplicates with HTTP 409 Conflict.
        - Rejection of contradictory sample/loss overlaps with HTTP 409 Conflict.
        - Idempotent acknowledgment of identical duplicate batches.
        - Dual contiguous cursor calculation (persisted vs resolved).
        """
        # Convert Pydantic model to protocol IngestBatch
        loss_records = [
            LossRecord(from_index=lr.from_index, to_index=lr.to_index, reason=lr.reason)
            for lr in payload.lost
        ]
        batch = IngestBatch(
            session_id=payload.session_id,
            sensor=payload.sensor,
            first_index=payload.first_index,
            count=len(payload.data),
            data=payload.data,
            lost=loss_records,
            session_end=payload.session_end,
            crc32="",
        )

        prev_inserted = storage.telemetry.samples_inserted
        prev_duplicates = storage.telemetry.duplicate_samples_ignored

        try:
            ckpt: CloudCheckpointRecord = storage.save_batch(batch)
        except CloudIntegrityConflictError as exc:
            logger.warning(f"Integrity conflict: {exc}")
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
        except CloudContradictoryLossConflictError as exc:
            logger.warning(f"Contradictory loss conflict: {exc}")
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
        except Exception as exc:
            logger.error(f"Storage error: {exc}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))

        # Check and process ready windows for online mode
        try:
            processing_service.process_ready_windows(payload.session_id)
        except Exception as exc:
            logger.error(f"Error evaluating ready windows: {exc}", exc_info=True)

        samples_inserted_in_batch = storage.telemetry.samples_inserted - prev_inserted
        duplicates_in_batch = storage.telemetry.duplicate_samples_ignored - prev_duplicates

        return BatchResponse(
            status="ok",
            session_id=ckpt.session_id,
            sensor=ckpt.sensor,
            contiguous_persisted_index=ckpt.contiguous_persisted_index,
            contiguous_resolved_index=ckpt.contiguous_resolved_index,
            highest_observed_index=ckpt.highest_observed_index,
            samples_inserted=samples_inserted_in_batch,
            duplicates_ignored=duplicates_in_batch,
        )

    @app.get(
        "/api/v1/status",
        status_code=status.HTTP_200_OK,
        summary="Get cloud receiver status",
    )
    def get_status(
        session: Optional[str] = Query(None, description="Optional session ID to filter status"),
    ) -> dict[str, Any]:
        """Return system health, active sessions, counts, and stream checkpoints."""
        samp_cnt, loss_cnt = storage.get_counts(session_id=session)
        checkpoints = storage.get_all_checkpoints(session_id=session)
        return {
            "status": "healthy",
            "active_sessions": storage.get_active_sessions(),
            "session_id": session,
            "total_samples": samp_cnt,
            "total_loss_records": loss_cnt,
            "checkpoints": [
                {
                    "session_id": c.session_id,
                    "sensor": c.sensor,
                    "contiguous_persisted_index": c.contiguous_persisted_index,
                    "contiguous_resolved_index": c.contiguous_resolved_index,
                    "highest_observed_index": c.highest_observed_index,
                    "last_updated_at": c.last_updated_at,
                }
                for c in checkpoints
            ],
        }

    @app.get(
        "/api/v1/export",
        status_code=status.HTTP_200_OK,
        summary="Export session data as Section 4 NDJSON",
    )
    def export_session_data(
        session: str = Query(..., min_length=1, description="Target session ID"),
    ) -> Response:
        """Export all samples and loss records for a session in Section 4 NDJSON format."""
        if not storage.session_exists(session):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Session '{session}' not found in cloud database.",
            )

        records = storage.export_ndjson_records(session)
        lines = [json.dumps(r) for r in records]
        content = "\n".join(lines) + ("\n" if lines else "")

        return Response(content=content, media_type="application/x-ndjson")

    @app.get(
        "/api/v1/results",
        status_code=status.HTTP_200_OK,
        summary="Get Section 3 online window processing results",
    )
    def get_session_results(
        session: str = Query(..., min_length=1, description="Target session ID"),
    ) -> Response:
        """Return Section 3 online window processing results in NDJSON format."""
        if not storage.session_exists(session):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Session '{session}' not found in cloud database.",
            )

        results = processing_service.get_results(session)
        lines = [json.dumps(res.to_dict()) for res in results]
        content = "\n".join(lines) + ("\n" if lines else "")

        return Response(content=content, media_type="application/x-ndjson")

    return app


# Default ASGI application instance configured via environment variable
_default_db_url = os.environ.get("DATABASE_URL") or os.environ.get("CLOUD_DATABASE_URL")
app = create_cloud_app(
    db_path=os.environ.get("CLOUD_DB_PATH", "biosignal_cloud.db"),
    database_url=_default_db_url,
)


if __name__ == "__main__":
    import uvicorn
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("pipeline.cloud.server:app", host=host, port=port, reload=False)
