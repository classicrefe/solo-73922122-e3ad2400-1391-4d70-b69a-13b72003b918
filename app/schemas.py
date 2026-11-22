"""Pydantic request/response schemas."""
from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from app.models import fmt_ts


class AwareDatetimeMixin:
    """Reject naive (timezone-less) event/watermark timestamps."""

    @field_validator("*", check_fields=False)
    @classmethod
    def _check_aware(cls, value, info):
        if isinstance(value, datetime) and value.tzinfo is None:
            raise ValueError(f"{info.field_name} must include a timezone (use UTC 'Z')")
        return value


class EventIn(AwareDatetimeMixin, BaseModel):
    user_id: str = Field(min_length=1, max_length=255)
    event_id: str = Field(min_length=1, max_length=255)
    event_time: datetime


class ImportRequest(BaseModel):
    events: list[EventIn] = Field(min_length=1)


class SessionOut(BaseModel):
    session_key: str
    start_time: str
    end_time: str
    event_count: int
    event_ids: list[str]
    status: Literal["published", "pending"]


class ChangeOut(BaseModel):
    change_type: Literal["retract", "add"]
    session_key: str
    start_time: str
    end_time: str
    event_count: int
    event_ids: list[str]


class RevisionOut(BaseModel):
    revision_no: int
    reason: str
    watermark: str
    idle_gap_seconds: int
    created_at: str
    detail: str | None = None
    changes: list[ChangeOut] = Field(default_factory=list)
    sessions: list[SessionOut] = Field(default_factory=list)


class ImportSummary(BaseModel):
    user_id: str
    received: int
    inserted: int
    duplicate: int
    conflicts: list[str] = Field(default_factory=list)


class ImportResponse(BaseModel):
    summary: ImportSummary
    watermark: str
    new_revision_no: int | None
    changes: list[ChangeOut]
    published: list[SessionOut]
    pending: list[SessionOut]


class WatermarkRequest(AwareDatetimeMixin, BaseModel):
    user_id: str = Field(min_length=1, max_length=255)
    watermark: datetime


class WatermarkResponse(BaseModel):
    user_id: str
    watermark: str
    new_revision_no: int | None
    changes: list[ChangeOut]
    published: list[SessionOut]
    pending: list[SessionOut]


class UserConfigIn(BaseModel):
    idle_gap_seconds: int = Field(gt=0)


class UserConfigOut(BaseModel):
    user_id: str
    idle_gap_seconds: int
    watermark: str


class EventOut(BaseModel):
    event_id: str
    event_time: str


def serialize_session(snap) -> SessionOut:
    return SessionOut(
        session_key=snap.session_key,
        start_time=fmt_ts(snap.start_time),
        end_time=fmt_ts(snap.end_time),
        event_count=snap.event_count,
        event_ids=snap.event_ids.split(","),
        status=snap.status,  # type: ignore[arg-type]
    )


def serialize_change(ch) -> ChangeOut:
    return ChangeOut(
        change_type=ch.change_type,  # type: ignore[arg-type]
        session_key=ch.session_key,
        start_time=fmt_ts(ch.start_time),
        end_time=fmt_ts(ch.end_time),
        event_count=ch.event_count,
        event_ids=ch.event_ids.split(","),
    )


def serialize_revision(rev, include_sessions: bool = True) -> RevisionOut:
    return RevisionOut(
        revision_no=rev.revision_no,
        reason=rev.reason,
        watermark=fmt_ts(rev.watermark),
        idle_gap_seconds=rev.idle_gap_seconds,
        created_at=fmt_ts(rev.created_at),
        detail=rev.detail,
        changes=[serialize_change(c) for c in rev.changes],
        sessions=[serialize_session(s) for s in rev.sessions] if include_sessions else [],
    )
