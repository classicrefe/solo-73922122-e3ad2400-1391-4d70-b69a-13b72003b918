"""End-to-end smoke test against a running server (not pytest)."""
import json
import urllib.request

BASE = "http://127.0.0.1:8000"


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def imp(uid, events):
    return call("POST", "/api/events/import", {
        "events": [{"user_id": uid, "event_id": e, "event_time": t} for e, t in events]
    })


def wm(uid, t):
    return call("POST", "/api/watermark/advance", {"user_id": uid, "watermark": t})


U = "smoke-bridge"
print("set gap:", call("PUT", f"/api/users/{U}/config", {"idle_gap_seconds": 600})[0])

batch = [
    ("e1", "2026-11-22T10:00:00Z"), ("e2", "2026-11-22T10:08:00Z"),
    ("e3", "2026-11-22T10:20:00Z"), ("e4", "2026-11-22T10:26:00Z"),
]
s, r = imp(U, batch)
print("import1:", s, r["summary"], "rev:", r["new_revision_no"], "pending:", len(r["pending"]))
assert s == 200 and r["new_revision_no"] is None and len(r["pending"]) == 2

# idempotent replay
s, r = imp(U, batch)
print("replay:", s, r["summary"], "rev:", r["new_revision_no"])
assert r["summary"]["duplicate"] == 4 and r["new_revision_no"] is None

# watermark 10:36 -> publish first session
s, r = wm(U, "2026-11-22T10:36:00Z")
print("wm1036:", s, "rev:", r["new_revision_no"], "changes:",
      [(c["change_type"], c["event_ids"]) for c in r["changes"]])
assert r["new_revision_no"] == 1 and r["changes"][0]["event_ids"] == ["e1", "e2"]

# backward rejected
s, r = wm(U, "2026-11-22T10:00:00Z")
print("backward:", s, r["detail"])
assert s == 409

# late bridge event
s, r = imp(U, [("L1", "2026-11-22T10:14:00Z")])
print("late:", s, "rev:", r["new_revision_no"], "changes:",
      [(c["change_type"], c["event_ids"]) for c in r["changes"]],
      "pending:", [p["event_ids"] for p in r["pending"]])
assert r["new_revision_no"] == 2
assert r["changes"][0] == {"change_type": "retract", **{k: r["changes"][0][k] for k in r["changes"][0]}}
assert r["changes"][0]["change_type"] == "retract"
assert r["pending"][0]["event_ids"] == ["e1", "e2", "L1", "e3", "e4"]

# re-publish after watermark advance
s, r = wm(U, "2026-11-22T10:40:00Z")
print("wm1040:", s, "rev:", r["new_revision_no"], "changes:",
      [(c["change_type"], c["event_ids"]) for c in r["changes"]])
assert r["new_revision_no"] == 3
assert r["changes"][0]["change_type"] == "add"
assert r["changes"][0]["event_ids"] == ["e1", "e2", "L1", "e3", "e4"]

# revision history + old revision still queryable
s, revs = call("GET", f"/api/users/{U}/revisions")
print("revisions:", [(x["revision_no"], x["reason"]) for x in revs])
assert [x["revision_no"] for x in revs] == [3, 2, 1]
s, rev1 = call("GET", f"/api/users/{U}/revisions/1")
print("rev1 snapshot:", [(x["status"], x["event_ids"]) for x in rev1["sessions"]])
assert rev1["sessions"][0]["status"] == "published"
assert rev1["sessions"][0]["event_ids"] == ["e1", "e2"]

# conflicting event id rejected
s, r = imp(U, [("e1", "2026-11-22T11:00:00Z")])
print("conflict:", s, r["detail"])
assert s == 409

# naive timestamp rejected
s, r = call("POST", "/api/events/import",
            {"events": [{"user_id": U, "event_id": "n1", "event_time": "2026-11-22T10:00:00"}]})
print("naive:", s)
assert s == 422

# current sessions and index page
s, cur = call("GET", f"/api/users/{U}/sessions")
print("current sessions:", [(x["status"], x["event_ids"]) for x in cur])
assert len(cur) == 1 and cur[0]["status"] == "published"

with urllib.request.urlopen(BASE + "/", timeout=10) as r:
    html = r.read().decode()
assert "事件会话发布" in html
print("index page bytes:", len(html))
print("\nALL SMOKE CHECKS PASSED")
