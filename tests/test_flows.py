"""Key end-to-end flows of the session publishing service."""
from datetime import datetime, timezone

from app import service


def _iso(y, mo, d, h, mi=0, s=0):
    return datetime(y, mo, d, h, mi, s, tzinfo=timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def import_events(client, uid, pairs):
    payload = {
        "events": [
            {"user_id": uid, "event_id": eid, "event_time": ts}
            for eid, ts in pairs
        ]
    }
    return client.post("/api/events/import", json=payload)


def sessions_of(client, uid):
    return client.get(f"/api/users/{uid}/sessions").json()


def revisions_of(client, uid):
    return client.get(f"/api/users/{uid}/revisions").json()


# ---------------------------------------------------------------- sessionize
def test_sessionization_gap_and_tie_break(client):
    uid = "u1"
    client.put(f"/api/users/{uid}/config", json={"idle_gap_seconds": 600})
    r = import_events(
        client,
        uid,
        [
            ("e2", _iso(2026, 11, 22, 10, 5)),
            ("e1", _iso(2026, 11, 22, 10, 5)),  # same time -> event_id order
            ("e3", _iso(2026, 11, 22, 10, 16)),  # 11m gap -> new session
        ],
    )
    assert r.status_code == 200, r.text
    pending = sessions_of(client, uid)
    assert len(pending) == 2
    assert pending[0]["event_ids"] == ["e1", "e2"]
    assert pending[1]["event_ids"] == ["e3"]


# ----------------------------------------------------------- publish + watermark
def test_publish_only_closed_sessions_and_watermark_monotonic(client):
    uid = "u2"
    client.put(f"/api/users/{uid}/config", json={"idle_gap_seconds": 600})
    import_events(
        client,
        uid,
        [
            ("a1", _iso(2026, 11, 22, 10, 0)),
            ("a2", _iso(2026, 11, 22, 10, 8)),
            ("b1", _iso(2026, 11, 22, 10, 20)),
            ("b2", _iso(2026, 11, 22, 10, 26)),
        ],
    )
    # watermark 10:36 -> threshold 10:26; end == threshold stays pending
    r = client.post(
        "/api/watermark/advance",
        json={"user_id": uid, "watermark": _iso(2026, 11, 22, 10, 36)},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["new_revision_no"] == 1
    assert len(body["changes"]) == 1 and body["changes"][0]["change_type"] == "add"
    assert body["changes"][0]["event_ids"] == ["a1", "a2"]
    assert len(body["published"]) == 1
    assert len(body["pending"]) == 1

    # watermark cannot move backwards
    bad = client.post(
        "/api/watermark/advance",
        json={"user_id": uid, "watermark": _iso(2026, 11, 22, 10, 30)},
    )
    assert bad.status_code == 409
    assert "only move forward" in bad.json()["detail"]

    # equal watermark is a no-op: no new revision
    same = client.post(
        "/api/watermark/advance",
        json={"user_id": uid, "watermark": _iso(2026, 11, 22, 10, 36)},
    )
    assert same.status_code == 200
    assert same.json()["new_revision_no"] == 1

    # advance to 10:40 -> threshold 10:30, second session publishes
    r2 = client.post(
        "/api/watermark/advance",
        json={"user_id": uid, "watermark": _iso(2026, 11, 22, 10, 40)},
    )
    assert r2.json()["new_revision_no"] == 2
    assert [c["change_type"] for c in r2.json()["changes"]] == ["add"]
    assert r2.json()["changes"][0]["event_ids"] == ["b1", "b2"]


# ------------------------------------------------------------- late -> inside
def test_late_event_inside_published_session_emits_retract_and_add(client):
    uid = "u3"
    client.put(f"/api/users/{uid}/config", json={"idle_gap_seconds": 600})
    import_events(
        client,
        uid,
        [("a1", _iso(2026, 11, 22, 10, 0)), ("a2", _iso(2026, 11, 22, 10, 8))],
    )
    client.post(
        "/api/watermark/advance",
        json={"user_id": uid, "watermark": _iso(2026, 11, 22, 10, 20)},
    )
    before = revisions_of(client, uid)
    assert len(before) == 1

    r = import_events(client, uid, [("L", _iso(2026, 11, 22, 10, 5))])
    body = r.json()
    assert body["new_revision_no"] == 2
    kinds = {c["change_type"]: c for c in body["changes"]}
    assert set(kinds) == {"retract", "add"}
    assert kinds["retract"]["event_ids"] == ["a1", "a2"]
    # chronological order (10:00, 10:05, 10:08), same-time tie-break by event_id
    assert kinds["add"]["event_ids"] == ["a1", "L", "a2"]

    # old revision is still fully queryable
    rev1 = client.get(f"/api/users/{uid}/revisions/1").json()
    assert rev1["revision_no"] == 1
    assert len(rev1["sessions"]) == 1
    assert rev1["sessions"][0]["event_ids"] == ["a1", "a2"]
    rev2 = client.get(f"/api/users/{uid}/revisions/2").json()
    assert rev2["sessions"][0]["event_ids"] == ["a1", "L", "a2"]


# ------------------------------------------------------------- late -> bridge
def test_late_event_bridges_two_sessions_then_republish(client):
    uid = "u4"
    client.put(f"/api/users/{uid}/config", json={"idle_gap_seconds": 600})
    import_events(
        client,
        uid,
        [
            ("a1", _iso(2026, 11, 22, 10, 0)),
            ("a2", _iso(2026, 11, 22, 10, 8)),
            ("b1", _iso(2026, 11, 22, 10, 20)),
            ("b2", _iso(2026, 11, 22, 10, 26)),
        ],
    )
    client.post(
        "/api/watermark/advance",
        json={"user_id": uid, "watermark": _iso(2026, 11, 22, 10, 36)},
    )
    # late event at 10:14: both gaps are exactly 6 minutes <= idle gap
    r = import_events(client, uid, [("L1", _iso(2026, 11, 22, 10, 14))])
    body = r.json()
    assert body["new_revision_no"] == 2
    assert [c["change_type"] for c in body["changes"]] == ["retract"]
    assert body["changes"][0]["event_ids"] == ["a1", "a2"]
    # merged session exists but is pending (end 10:26 is not before 10:26 threshold)
    assert len(body["published"]) == 0
    assert body["pending"][0]["event_ids"] == ["a1", "a2", "L1", "b1", "b2"]

    r2 = client.post(
        "/api/watermark/advance",
        json={"user_id": uid, "watermark": _iso(2026, 11, 22, 10, 40)},
    )
    b2 = r2.json()
    assert b2["new_revision_no"] == 3
    assert b2["changes"][0]["change_type"] == "add"
    assert b2["changes"][0]["event_ids"] == ["a1", "a2", "L1", "b1", "b2"]

    # revision history: 3 revisions, each independently stored
    revs = revisions_of(client, uid)
    assert [r["revision_no"] for r in revs] == [3, 2, 1]
    assert [r["reason"] for r in revs[::-1]] == ["publish", "late_import", "publish"]


# ------------------------------------------------------------- idempotency
def test_duplicate_import_is_idempotent(client):
    uid = "u5"
    client.put(f"/api/users/{uid}/config", json={"idle_gap_seconds": 600})
    events = [("x1", _iso(2026, 11, 22, 10, 0)), ("x2", _iso(2026, 11, 22, 10, 3))]
    first = import_events(client, uid, events)
    assert first.json()["summary"]["inserted"] == 2
    # the import itself creates no revision while nothing is published
    assert len(revisions_of(client, uid)) == 0
    client.post(
        "/api/watermark/advance",
        json={"user_id": uid, "watermark": _iso(2026, 11, 22, 10, 20)},
    )
    assert len(revisions_of(client, uid)) == 1

    second = import_events(client, uid, events)
    s = second.json()["summary"]
    assert s["inserted"] == 0 and s["duplicate"] == 2
    assert second.json()["new_revision_no"] is None
    # replay does not add a revision or alter stored events
    assert len(revisions_of(client, uid)) == 1
    assert len(client.get(f"/api/users/{uid}/events").json()) == 2


def test_conflicting_event_id_rejected_without_state_change(client):
    uid = "u6"
    client.put(f"/api/users/{uid}/config", json={"idle_gap_seconds": 600})
    import_events(client, uid, [("x1", _iso(2026, 11, 22, 10, 0))])
    bad = import_events(client, uid, [("x1", _iso(2026, 11, 22, 11, 0))])
    assert bad.status_code == 409
    events = client.get(f"/api/users/{uid}/events").json()
    assert len(events) == 1
    assert events[0]["event_time"].startswith("2026-11-22T10:00:00")


def test_duplicate_event_id_in_same_batch_rejected(client):
    uid = "u7"
    r = import_events(
        client,
        uid,
        [("x1", _iso(2026, 11, 22, 10, 0)), ("x1", _iso(2026, 11, 22, 10, 1))],
    )
    assert r.status_code == 422


# ---------------------------------------------------- failure keeps old revision
def test_publish_failure_keeps_previous_revision(client):
    uid = "u8"
    client.put(f"/api/users/{uid}/config", json={"idle_gap_seconds": 600})
    import_events(
        client,
        uid,
        [
            ("a1", _iso(2026, 11, 22, 10, 0)),
            ("a2", _iso(2026, 11, 22, 10, 8)),
            ("b1", _iso(2026, 11, 22, 10, 25)),
        ],
    )
    client.post(
        "/api/watermark/advance",
        json={"user_id": uid, "watermark": _iso(2026, 11, 22, 10, 20)},
    )
    revs_before = revisions_of(client, uid)
    assert len(revs_before) == 1

    # Advancing to 10:40 would publish b1 (end 10:25 < threshold 10:30),
    # so a revision is attempted and the injected failure fires.
    service.FAIL_BEFORE_COMMIT = True
    try:
        failed = client.post(
            "/api/watermark/advance",
            json={"user_id": uid, "watermark": _iso(2026, 11, 22, 10, 40)},
        )
        assert failed.status_code == 500
    finally:
        service.FAIL_BEFORE_COMMIT = False

    # watermark and revision set are untouched
    cfg = client.get(f"/api/users/{uid}/config").json()
    assert cfg["watermark"].startswith("2026-11-22T10:20:00")
    assert len(revisions_of(client, uid)) == 1

    # a normal retry after the failure succeeds
    ok = client.post(
        "/api/watermark/advance",
        json={"user_id": uid, "watermark": _iso(2026, 11, 22, 10, 40)},
    )
    assert ok.status_code == 200
    assert ok.json()["new_revision_no"] == 2
    assert ok.json()["changes"][0]["event_ids"] == ["b1"]


def test_late_import_failure_rolls_back_inserted_event(client):
    uid = "u9"
    client.put(f"/api/users/{uid}/config", json={"idle_gap_seconds": 600})
    import_events(client, uid, [("a1", _iso(2026, 11, 22, 10, 0))])
    client.post(
        "/api/watermark/advance",
        json={"user_id": uid, "watermark": _iso(2026, 11, 22, 10, 20)},
    )
    service.FAIL_BEFORE_COMMIT = True
    try:
        failed = import_events(client, uid, [("L", _iso(2026, 11, 22, 10, 5))])
        assert failed.status_code == 500
    finally:
        service.FAIL_BEFORE_COMMIT = False

    events = client.get(f"/api/users/{uid}/events").json()
    assert [e["event_id"] for e in events] == ["a1"]
    assert len(revisions_of(client, uid)) == 1


# ------------------------------------------------------------------- misc rules
def test_naive_timestamp_rejected_and_offset_normalized_to_utc(client):
    uid = "u10"
    # pydantic rejects naive datetimes
    r = client.post(
        "/api/events/import",
        json={
            "events": [
                {"user_id": uid, "event_id": "z1", "event_time": "2026-11-22T10:00:00"}
            ]
        },
    )
    assert r.status_code == 422
    # +02:00 offset is accepted and normalized to 08:00Z
    r2 = client.post(
        "/api/events/import",
        json={
            "events": [
                {"user_id": uid, "event_id": "z1", "event_time": "2026-11-22T10:00:00+02:00"}
            ]
        },
    )
    assert r2.status_code == 200
    events = client.get(f"/api/users/{uid}/events").json()
    assert events[0]["event_time"].startswith("2026-11-22T08:00:00")


def test_revision_not_found_returns_404(client):
    r = client.get("/api/users/nobody/revisions/99")
    assert r.status_code == 404


def test_idle_gap_must_be_positive(client):
    r = client.put("/api/users/u11/config", json={"idle_gap_seconds": 0})
    assert r.status_code == 422
