"""Database engine and ORM models.

Timestamps are stored as fixed-length UTC text
(``YYYY-MM-DDTHH:MM:SS.ffffffZ``) so lexical ordering matches chronological
ordering in SQLite.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

from sqlalchemy import (
    CheckConstraint,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.types import TypeDecorator
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class TZDateTime(TypeDecorator):
    """DateTime that survives SQLite's lack of timezone support.

    Values are written as fixed-width UTC text and always read back as
    timezone-aware UTC datetimes, so comparisons never mix naive/aware values.
    """

    impl = String(33)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return fmt_ts(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return parse_ts(value)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_utc(dt: datetime) -> datetime:
    """Normalize an aware/naive datetime to UTC."""
    if dt.tzinfo is None:
        # The API layer rejects naive input, this is only a defensive default.
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def fmt_ts(dt: datetime) -> str:
    """Fixed-width UTC text used both for storage and JSON output."""
    return to_utc(dt).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def parse_ts(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
        tzinfo=timezone.utc
    )


def default_db_url() -> str:
    env_url = os.environ.get("SESSION_DB_URL")
    if env_url:
        return env_url
    # Local development default: a data directory next to the project root.
    # Docker sets SESSION_DB_URL=sqlite:////app/data/sessions.db explicitly.
    local_dir = os.environ.get(
        "SESSION_DATA_DIR",
        os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, "data")),
    )
    return "sqlite:///" + os.path.join(local_dir, "sessions.db")


def make_engine(db_url: str | None = None):
    url = db_url or default_db_url()
    connect_args = {"check_same_thread": False} if url.startswith("sqlite") else {}
    if url.startswith("sqlite:///"):
        # sqlite:////abs/path -> /abs/path ; sqlite:///rel/path -> rel/path
        path_part = url[len("sqlite:///") :]
        if path_part:  # skip in-memory "" / ":memory:"
            parent = os.path.dirname(path_part)
            if parent:
                os.makedirs(parent, exist_ok=True)
    return create_engine(url, connect_args=connect_args, future=True)


class Base(DeclarativeBase):
    pass


class UserConfig(Base):
    __tablename__ = "user_config"

    user_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    idle_gap_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=600)
    watermark: Mapped[datetime] = mapped_column(TZDateTime, nullable=False, default=EPOCH)
    created_at: Mapped[datetime] = mapped_column(TZDateTime, nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        TZDateTime, nullable=False, default=utc_now, onupdate=utc_now
    )

    __table_args__ = (CheckConstraint("idle_gap_seconds > 0", name="ck_idle_gap_positive"),)


class Event(Base):
    __tablename__ = "event"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    event_id: Mapped[str] = mapped_column(String(255), nullable=False)
    event_time: Mapped[datetime] = mapped_column(TZDateTime, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(TZDateTime, nullable=False, default=utc_now)

    __table_args__ = (
        UniqueConstraint("user_id", "event_id", name="uq_user_event"),
    )


class Revision(Base):
    __tablename__ = "revision"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    revision_no: Mapped[int] = mapped_column(Integer, nullable=False)
    reason: Mapped[str] = mapped_column(String(64), nullable=False)
    watermark: Mapped[datetime] = mapped_column(TZDateTime, nullable=False)
    idle_gap_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(TZDateTime, nullable=False, default=utc_now)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)

    sessions: Mapped[list["SessionSnapshot"]] = relationship(
        back_populates="revision",
        cascade="all, delete-orphan",
        order_by="SessionSnapshot.id",
    )
    changes: Mapped[list["RevisionChange"]] = relationship(
        back_populates="revision",
        cascade="all, delete-orphan",
        order_by="RevisionChange.id",
    )

    __table_args__ = (
        UniqueConstraint("user_id", "revision_no", name="uq_user_revision"),
    )


class SessionSnapshot(Base):
    """Immutable snapshot of one session inside one revision.

    Old revisions remain queryable forever, so every revision stores its own
    full session set instead of mutating rows in place.
    """

    __tablename__ = "session_snapshot"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    revision_id: Mapped[int] = mapped_column(
        ForeignKey("revision.id", ondelete="CASCADE"), nullable=False, index=True
    )
    session_key: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    start_time: Mapped[datetime] = mapped_column(TZDateTime, nullable=False)
    end_time: Mapped[datetime] = mapped_column(TZDateTime, nullable=False)
    event_count: Mapped[int] = mapped_column(Integer, nullable=False)
    event_ids: Mapped[str] = mapped_column(Text, nullable=False)  # comma separated
    status: Mapped[str] = mapped_column(String(16), nullable=False)  # published / pending

    revision: Mapped[Revision] = relationship(back_populates="sessions")


class RevisionChange(Base):
    __tablename__ = "revision_change"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    revision_id: Mapped[int] = mapped_column(
        ForeignKey("revision.id", ondelete="CASCADE"), nullable=False, index=True
    )
    change_type: Mapped[str] = mapped_column(String(16), nullable=False)  # retract / add
    session_key: Mapped[str] = mapped_column(String(64), nullable=False)
    start_time: Mapped[datetime] = mapped_column(TZDateTime, nullable=False)
    end_time: Mapped[datetime] = mapped_column(TZDateTime, nullable=False)
    event_count: Mapped[int] = mapped_column(Integer, nullable=False)
    event_ids: Mapped[str] = mapped_column(Text, nullable=False)

    revision: Mapped[Revision] = relationship(back_populates="changes")
