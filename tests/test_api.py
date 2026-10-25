"""API-level tests through the full stack with a faked upstream."""
from __future__ import annotations

import json
import uuid

from app.core.config import API_PREFIX

from .conftest import make_csv, upload, wait_for_terminal

CSV_THREE = make_csv(
    [
        "General Hospital,123 Main St,555-1234",
        "City Clinic,45 Oak Ave,",
        '"St. Mary, West Wing","1 Elm St, Suite 2",555-9999',
    ]
)


def test_health(client):
    body = client.get("/health").json()
    assert body["status"] == "ok"
    assert "version" in body["data"]


def test_startup_fails_fast_without_upstream_url(monkeypatch):
    import pytest
    from fastapi.testclient import TestClient

    from app.core import config
    from app.main import create_app

    monkeypatch.setattr(config, "HOSPITAL_API_BASE_URL", "")
    with pytest.raises(RuntimeError, match="HOSPITAL_API_BASE_URL is not set"):
        with TestClient(create_app()):
            pass


def test_bulk_upload_returns_202_envelope(client):
    response = upload(client, CSV_THREE)
    assert response.status_code == 202
    body = response.json()
    assert list(body) == ["status", "data"]  # status comes before data
    assert body["status"] == "processing"
    assert body["data"]["total_hospitals"] == 3
    uuid.UUID(body["data"]["batch_id"])  # a real UUID


def test_bulk_full_lifecycle_matches_expected_response(client, fake_upstream):
    batch_id = upload(client, CSV_THREE).json()["data"]["batch_id"]
    result = wait_for_terminal(client, batch_id)
    assert list(result) == ["status", "data"]  # enveloped, status first
    assert result["status"] == "completed"
    data = result["data"]
    assert data["batch_id"] == batch_id
    assert data["total_hospitals"] == 3
    assert data["processed_hospitals"] == 3
    assert data["failed_hospitals"] == 0
    assert data["batch_activated"] is True
    assert data["processing_time_seconds"] >= 0
    rows = data["hospitals"]
    assert [r["row"] for r in rows] == [1, 2, 3]
    assert all(r["status"] == "created_and_activated" for r in rows)
    assert all(isinstance(r["hospital_id"], int) for r in rows)
    # upstream really received the batch id and activated it
    created = list(fake_upstream.hospitals.values())
    assert all(h["creation_batch_id"] == batch_id for h in created)
    assert all(h["active"] for h in created)


def test_invalid_csv_rejected_with_row_errors(client, fake_upstream):
    response = upload(client, make_csv(["A,Addr,1", ",Addr,1"]))
    assert response.status_code == 422
    body = response.json()
    assert list(body) == ["status", "data"]  # errors use the envelope too
    assert body["status"] == "error"
    assert body["data"]["message"] == "CSV validation failed"
    assert body["data"]["errors"] == [{"row": 2, "error": "name is required"}]
    assert fake_upstream.create_calls == 0  # nothing reached the upstream


def test_over_20_rows_rejected(client):
    rows = ["Hospital {},Addr,".format(i) for i in range(21)]
    response = upload(client, make_csv(rows))
    assert response.status_code == 422
    assert "maximum is 20" in json.dumps(response.json())


def test_non_csv_filename_rejected(client):
    response = upload(client, CSV_THREE, filename="hospitals.txt")
    assert response.status_code == 422
    assert response.json() == {
        "status": "error",
        "data": {"message": "expected a .csv file"},
    }


def test_missing_file_field_rejected(client):
    response = client.post(API_PREFIX)
    assert response.status_code == 422
    body = response.json()
    assert body["status"] == "error"
    assert body["data"]["message"] == "request validation failed"
    assert body["data"]["errors"][0]["loc"] == ["body", "file"]


def test_validate_endpoint_reports_without_side_effects(client, fake_upstream):
    ok = upload(client, CSV_THREE, path=API_PREFIX + "/validate").json()
    assert ok == {"status": "valid", "data": {"total_hospitals": 3, "errors": []}}
    bad = upload(client, make_csv(["A,1"], header="name,phone"), path=API_PREFIX + "/validate").json()
    assert bad["status"] == "invalid"
    assert bad["data"]["errors"][0]["error"].startswith("missing required column")
    assert fake_upstream.create_calls == 0


def test_unknown_batch_returns_404(client):
    missing = str(uuid.uuid4())
    response = client.get("{}/{}".format(API_PREFIX, missing))
    assert response.status_code == 404
    assert response.json() == {"status": "error", "data": {"message": "batch not found"}}
    assert client.post("{}/{}/resume".format(API_PREFIX, missing)).status_code == 404
    assert client.post("{}/{}/pause".format(API_PREFIX, missing)).status_code == 404
    assert client.get("{}/{}/stream".format(API_PREFIX, missing)).status_code == 404


def test_malformed_batch_id_returns_422(client):
    response = client.get(API_PREFIX + "/not-a-uuid")
    assert response.status_code == 422
    body = response.json()
    assert body["status"] == "error"
    assert body["data"]["errors"][0]["loc"] == ["path", "batch_id"]


def test_partial_failure_then_resume_completes(client, fake_upstream):
    fake_upstream.fail_status = 400  # permanent -> no retries, fast test
    fake_upstream.fail_always = {"City Clinic"}
    batch_id = upload(client, CSV_THREE).json()["data"]["batch_id"]
    result = wait_for_terminal(client, batch_id)
    assert result["status"] == "completed_with_errors"
    assert result["data"]["processed_hospitals"] == 2
    assert result["data"]["failed_hospitals"] == 1
    assert result["data"]["batch_activated"] is False
    assert fake_upstream.activate_calls == 0
    failed = result["data"]["hospitals"][1]
    assert failed["status"] == "failed"
    assert "400" in failed["error"]

    fake_upstream.fail_always = set()
    response = client.post("{}/{}/resume".format(API_PREFIX, batch_id))
    assert response.status_code == 202
    result = wait_for_terminal(client, batch_id)
    assert result["status"] == "completed"
    assert result["data"]["failed_hospitals"] == 0
    assert result["data"]["batch_activated"] is True
    assert fake_upstream.create_calls == 4  # only the failed row was retried


def test_pause_then_resume_via_api(client, fake_upstream):
    fake_upstream.response_delay = 0.05  # keep the batch running while we pause
    csv = make_csv(["Hospital {},Addr {},".format(i, i) for i in range(8)])
    batch_id = upload(client, csv).json()["data"]["batch_id"]
    response = client.post("{}/{}/pause".format(API_PREFIX, batch_id))
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "pausing"
    assert body["data"]["batch_id"] == batch_id

    result = wait_for_terminal(client, batch_id)
    assert result["status"] == "paused"
    assert result["data"]["batch_activated"] is False
    statuses = {r["status"] for r in result["data"]["hospitals"]}
    assert "pending" in statuses  # rows not yet started stayed pending

    assert client.post("{}/{}/resume".format(API_PREFIX, batch_id)).status_code == 202
    result = wait_for_terminal(client, batch_id)
    assert result["status"] == "completed"
    assert result["data"]["processed_hospitals"] == 8
    assert fake_upstream.create_calls == 8  # every row created exactly once


def test_pause_terminal_batch_returns_409(client):
    batch_id = upload(client, CSV_THREE).json()["data"]["batch_id"]
    wait_for_terminal(client, batch_id)
    response = client.post("{}/{}/pause".format(API_PREFIX, batch_id))
    assert response.status_code == 409
    assert "only a processing batch" in response.json()["data"]["message"]


def test_resume_completed_batch_returns_409(client):
    batch_id = upload(client, CSV_THREE).json()["data"]["batch_id"]
    wait_for_terminal(client, batch_id)
    response = client.post("{}/{}/resume".format(API_PREFIX, batch_id))
    assert response.status_code == 409
    body = response.json()
    assert body["status"] == "error"
    assert "nothing to resume" in body["data"]["message"]


def test_sse_stream_emits_snapshot_progress_and_completed(client, fake_upstream):
    fake_upstream.response_delay = 0.05  # keep the batch alive while we connect
    batch_id = upload(client, CSV_THREE).json()["data"]["batch_id"]
    events = []
    with client.stream("GET", "{}/{}/stream".format(API_PREFIX, batch_id)) as stream:
        assert stream.headers["content-type"].startswith("text/event-stream")
        event_name = None
        for line in stream.iter_lines():
            if line.startswith("event: "):
                event_name = line[len("event: "):]
            elif line.startswith("data: "):
                events.append((event_name, json.loads(line[len("data: "):])))
                if event_name == "completed":
                    break
    names = [name for name, _ in events]
    assert names[0] == "snapshot"
    assert names.count("progress") >= 1
    assert names[-1] == "completed"
    final = events[-1][1]
    assert final["status"] == "completed"
    assert final["data"]["processed_hospitals"] == 3
    # progress events carry the row status in the envelope too
    first_progress = next(body for name, body in events if name == "progress")
    assert first_progress["status"] == "created"
    assert "row" in first_progress["data"]


def test_sse_stream_on_finished_batch_sends_snapshot_and_closes(client):
    batch_id = upload(client, CSV_THREE).json()["data"]["batch_id"]
    wait_for_terminal(client, batch_id)
    with client.stream("GET", "{}/{}/stream".format(API_PREFIX, batch_id)) as stream:
        lines = list(stream.iter_lines())  # closes by itself: no hang
    assert lines[0] == "event: snapshot"
    snapshot = json.loads(lines[1][len("data: "):])
    assert snapshot["status"] == "completed"
