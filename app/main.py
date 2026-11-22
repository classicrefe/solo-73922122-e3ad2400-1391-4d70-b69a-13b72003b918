"""FastAPI application: event ingestion, watermark publishing, revisions."""
from __future__ import annotations

from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session

from app import service
from app.models import Base, UserConfig, fmt_ts, make_engine
from app.schemas import (
    ChangeOut,
    EventOut,
    ImportRequest,
    ImportResponse,
    ImportSummary,
    SessionOut,
    UserConfigIn,
    UserConfigOut,
    WatermarkRequest,
    WatermarkResponse,
    serialize_change,
    serialize_revision,
    serialize_session,
)
from app.service import ApiError

engine = make_engine()
Base.metadata.create_all(engine)

app = FastAPI(
    title="Event Session Publishing Service",
    version="1.0.0",
    description="Sessionize event streams with watermark-based publishing, "
    "late-event revisions and full revision history.",
)

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def get_db():
    db = Session(engine)
    try:
        yield db
    finally:
        db.close()


def _session_from_dict(sess: dict) -> SessionOut:
    return SessionOut(
        session_key=sess["session_key"],
        start_time=fmt_ts(sess["start"]),
        end_time=fmt_ts(sess["end"]),
        event_count=len(sess["event_ids"]),
        event_ids=sess["event_ids"],
        status=sess["status"],
    )


def _change_out(change_type: str, item) -> ChangeOut:
    """Build a change from either a SessionSnapshot (retraction) or a dict."""
    if isinstance(item, dict):
        return ChangeOut(
            change_type=change_type,  # type: ignore[arg-type]
            session_key=item["session_key"],
            start_time=fmt_ts(item["start"]),
            end_time=fmt_ts(item["end"]),
            event_count=len(item["event_ids"]),
            event_ids=item["event_ids"],
        )
    return ChangeOut(
        change_type=change_type,  # type: ignore[arg-type]
        session_key=item.session_key,
        start_time=fmt_ts(item.start_time),
        end_time=fmt_ts(item.end_time),
        event_count=item.event_count,
        event_ids=item.event_ids.split(","),
    )


def _changes_from_result(result: dict) -> list[ChangeOut]:
    changes: list[ChangeOut] = []
    for snap in result.get("retractions", []) or []:
        changes.append(_change_out("retract", snap))
    for d in result.get("additions", []) or []:
        changes.append(_change_out("add", d))
    return changes


def _sessions_from_result(result: dict, key: str):
    out = [_session_from_dict(d) for d in result.get(key, []) or []]
    out.sort(key=lambda s: (s.start_time, s.session_key))
    return out


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/users")
def list_users(db: Session = Depends(get_db)):
    rows = db.query(UserConfig).order_by(UserConfig.user_id).all()
    return [
        {
            "user_id": r.user_id,
            "idle_gap_seconds": r.idle_gap_seconds,
            "watermark": fmt_ts(r.watermark),
        }
        for r in rows
    ]


@app.get("/api/users/{user_id}/config", response_model=UserConfigOut)
def get_user_config(user_id: str, db: Session = Depends(get_db)):
    cfg = service.get_or_create_user(db, user_id)
    db.commit()
    return UserConfigOut(
        user_id=cfg.user_id,
        idle_gap_seconds=cfg.idle_gap_seconds,
        watermark=fmt_ts(cfg.watermark),
    )


@app.put("/api/users/{user_id}/config", response_model=UserConfigOut)
def update_user_config(
    user_id: str, body: UserConfigIn, db: Session = Depends(get_db)
):
    with service.user_lock(user_id):
        try:
            cfg = service.set_idle_gap(db, user_id, body.idle_gap_seconds)
            db.commit()
        except ApiError as exc:
            db.rollback()
            raise HTTPException(exc.status_code, exc.detail)
        except Exception:
            db.rollback()
            raise
    return UserConfigOut(
        user_id=cfg.user_id,
        idle_gap_seconds=cfg.idle_gap_seconds,
        watermark=fmt_ts(cfg.watermark),
    )


@app.post("/api/events/import", response_model=ImportResponse)
def import_events(body: ImportRequest, db: Session = Depends(get_db)):
    user_ids = {e.user_id for e in body.events}
    if len(user_ids) > 1:
        raise HTTPException(422, "all events in one batch must share the same user_id")
    user_id = next(iter(user_ids))

    payload = [
        {"event_id": e.event_id, "event_time": e.event_time} for e in body.events
    ]

    with service.user_lock(user_id):
        try:
            result = service.import_events(db, user_id, payload)
            db.commit()
        except ApiError as exc:
            db.rollback()
            raise HTTPException(exc.status_code, exc.message)
        except Exception as exc:
            db.rollback()
            raise HTTPException(500, f"import failed, previous revision kept: {exc}")

    cfg = db.get(UserConfig, user_id)
    return ImportResponse(
        summary=ImportSummary(**result["summary"]),
        watermark=fmt_ts(cfg.watermark),
        new_revision_no=result["new_revision_no"],
        changes=_changes_from_result(result),
        published=_sessions_from_result(result, "published_dicts"),
        pending=_sessions_from_result(result, "pending_dicts"),
    )


@app.post("/api/watermark/advance", response_model=WatermarkResponse)
def advance_watermark(body: WatermarkRequest, db: Session = Depends(get_db)):
    with service.user_lock(body.user_id):
        try:
            result = service.advance_watermark(db, body.user_id, body.watermark)
            db.commit()
        except ApiError as exc:
            db.rollback()
            raise HTTPException(exc.status_code, exc.message)
        except Exception as exc:
            db.rollback()
            raise HTTPException(500, f"publish failed, previous revision kept: {exc}")

    return WatermarkResponse(
        user_id=body.user_id,
        watermark=fmt_ts(result["watermark"]),
        new_revision_no=result["new_revision_no"],
        changes=_changes_from_result(result),
        published=_sessions_from_result(result, "published_dicts"),
        pending=_sessions_from_result(result, "pending_dicts"),
    )


@app.get("/api/users/{user_id}/events", response_model=list[EventOut])
def list_events(user_id: str, db: Session = Depends(get_db)):
    from app.models import Event

    rows = (
        db.query(Event)
        .filter(Event.user_id == user_id)
        .order_by(Event.event_time, Event.event_id)
        .all()
    )
    return [
        EventOut(event_id=r.event_id, event_time=fmt_ts(r.event_time)) for r in rows
    ]


@app.get("/api/users/{user_id}/sessions", response_model=list[SessionOut])
def current_sessions(user_id: str, db: Session = Depends(get_db)):
    rev = service.latest_revision(db, user_id)
    if rev is not None:
        snaps = sorted(rev.sessions, key=lambda s: (s.start_time, s.session_key))
        return [serialize_session(s) for s in snaps]
    # No revision yet: derive pending sessions on the fly from raw events.
    cfg = service.get_or_create_user(db, user_id)
    db.commit()
    from app.models import Event
    from sqlalchemy import select

    events = list(
        db.scalars(
            select(Event).where(Event.user_id == user_id)
        ).all()
    )
    sessions = service.classify_sessions(
        service.sessionize(events, cfg.idle_gap_seconds),
        cfg.watermark,
        cfg.idle_gap_seconds,
    )
    return [_session_from_dict(s) for s in sessions]


@app.get("/api/users/{user_id}/revisions")
def get_revisions(user_id: str, db: Session = Depends(get_db)):
    revs = service.list_revisions(db, user_id)
    return [
        {
            "revision_no": r.revision_no,
            "reason": r.reason,
            "watermark": fmt_ts(r.watermark),
            "idle_gap_seconds": r.idle_gap_seconds,
            "created_at": fmt_ts(r.created_at),
            "detail": r.detail,
            "changes": [serialize_change(c).model_dump() for c in r.changes],
        }
        for r in revs
    ]


@app.get("/api/users/{user_id}/revisions/{revision_no}")
def get_revision(user_id: str, revision_no: int, db: Session = Depends(get_db)):
    rev = service.get_revision(db, user_id, revision_no)
    if rev is None:
        raise HTTPException(404, f"revision {revision_no} not found for user {user_id}")
    return serialize_revision(rev).model_dump()
