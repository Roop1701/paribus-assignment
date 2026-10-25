"""Engine tests: orchestration, rate limiting, retries, pause/resume."""
from __future__ import annotations

import asyncio

import httpx
import pytest

from app.core import config
from app.dao.batch_dao import BatchDao
from app.dao.hospital_api_dao import HospitalApiDao
from app.module.bulk_engine import BatchConflictError, BulkEngine
from app.module.csv_parser import CsvRow

pytestmark = pytest.mark.anyio


def rows(*names):
    return [CsvRow(row=i, name=n, address="Addr {}".format(i), phone=None) for i, n in enumerate(names, 1)]


@pytest.fixture
async def engine(fake_upstream):
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(fake_upstream.handler), base_url="http://upstream"
    )
    bulk_engine = BulkEngine(HospitalApiDao(client), BatchDao())
    yield bulk_engine
    await bulk_engine.shutdown()
    await client.aclose()


async def run_batch(engine, *names) -> str:
    batch_id = await engine.start_batch(rows(*names))
    await engine.join(batch_id)
    return batch_id


async def test_happy_path_creates_then_activates(engine, fake_upstream):
    batch_id = await run_batch(engine, "A", "B", "C")
    snapshot = await engine.get_status(batch_id)
    assert snapshot["status"] == "completed"
    data = snapshot["data"]
    assert data["batch_activated"] is True
    assert [r["status"] for r in data["hospitals"]] == ["created_and_activated"] * 3
    assert all(r["hospital_id"] for r in data["hospitals"])
    # activation happened once, after all creates, with our batch id
    assert fake_upstream.activated_batches == [batch_id]
    assert all(h["active"] for h in fake_upstream.hospitals.values())


async def test_concurrency_is_bounded_by_semaphore(engine, fake_upstream):
    fake_upstream.response_delay = 0.03
    batch_id = await run_batch(engine, *["H{}".format(i) for i in range(12)])
    assert (await engine.get_status(batch_id))["status"] == "completed"
    assert fake_upstream.max_in_flight <= config.MAX_CONCURRENT_UPSTREAM_CALLS


async def test_retry_on_429_honours_retry_after_then_succeeds(engine, fake_upstream):
    fake_upstream.fail_status = 429
    fake_upstream.retry_after = "0"
    fake_upstream.fail_once = {"A": 2}
    batch_id = await run_batch(engine, "A")
    assert (await engine.get_status(batch_id))["status"] == "completed"
    assert fake_upstream.create_calls == 3  # 2 failures + 1 success


async def test_retries_exhausted_marks_row_failed(engine, fake_upstream, monkeypatch):
    monkeypatch.setattr(config, "MAX_RETRIES", 2)
    fake_upstream.fail_always = {"B"}
    batch_id = await run_batch(engine, "A", "B")
    snapshot = await engine.get_status(batch_id)
    assert snapshot["status"] == "completed_with_errors"
    assert snapshot["data"]["batch_activated"] is False
    assert fake_upstream.activate_calls == 0  # no activation on partial failure
    failed = snapshot["data"]["hospitals"][1]
    assert failed["status"] == "failed"
    assert "upstream returned 500" in failed["error"]
    assert fake_upstream.create_calls == 1 + 3  # A once, B tried 1+2 times


async def test_non_retryable_4xx_fails_immediately(engine, fake_upstream):
    fake_upstream.fail_status = 422
    fake_upstream.fail_always = {"A"}
    batch_id = await run_batch(engine, "A")
    snapshot = await engine.get_status(batch_id)
    assert snapshot["data"]["hospitals"][0]["status"] == "failed"
    assert fake_upstream.create_calls == 1  # no retries on permanent errors


async def resume(engine, batch_id):
    batch = await engine.batch_dao.get(batch_id)
    await engine.resume_batch(batch)
    await engine.join(batch_id)


async def test_resume_retries_only_failed_rows(engine, fake_upstream):
    fake_upstream.fail_status = 400
    fake_upstream.fail_always = {"B"}
    batch_id = await run_batch(engine, "A", "B", "C")
    snapshot = await engine.get_status(batch_id)
    assert snapshot["status"] == "completed_with_errors"
    assert snapshot["data"]["processed_hospitals"] == 2

    fake_upstream.fail_always = set()
    await resume(engine, batch_id)
    snapshot = await engine.get_status(batch_id)
    assert snapshot["status"] == "completed"
    assert snapshot["data"]["batch_activated"] is True
    assert fake_upstream.create_calls == 4  # 3 first run + only B on resume


async def test_resume_after_activation_failure_retries_activation_only(
    engine, fake_upstream, monkeypatch
):
    monkeypatch.setattr(config, "MAX_RETRIES", 0)
    fake_upstream.activate_failures = 1
    batch_id = await run_batch(engine, "A", "B")
    snapshot = await engine.get_status(batch_id)
    assert snapshot["status"] == "activation_failed"
    assert [r["status"] for r in snapshot["data"]["hospitals"]] == ["created", "created"]

    await resume(engine, batch_id)
    assert (await engine.get_status(batch_id))["status"] == "completed"
    assert fake_upstream.create_calls == 2  # rows were not recreated
    assert fake_upstream.activate_calls == 2


async def test_resume_while_processing_conflicts(engine, fake_upstream):
    fake_upstream.response_delay = 0.05
    batch_id = await engine.start_batch(rows("A"))
    batch = await engine.batch_dao.get(batch_id)
    with pytest.raises(BatchConflictError, match="still processing"):
        await engine.resume_batch(batch)
    await engine.join(batch_id)


async def test_resume_completed_batch_conflicts(engine, fake_upstream):
    batch_id = await run_batch(engine, "A")
    batch = await engine.batch_dao.get(batch_id)
    with pytest.raises(BatchConflictError, match="nothing to resume"):
        await engine.resume_batch(batch)


async def test_pause_keeps_pending_rows_and_resume_finishes(engine, fake_upstream):
    fake_upstream.response_delay = 0.05
    names = ["H{}".format(i) for i in range(8)]
    batch_id = await engine.start_batch(rows(*names))
    await asyncio.sleep(0.01)  # let the first wave take the semaphore
    await engine.pause_batch(await engine.batch_dao.get(batch_id))
    await engine.join(batch_id)
    snapshot = await engine.get_status(batch_id)
    assert snapshot["status"] == "paused"
    statuses = {r["status"] for r in snapshot["data"]["hospitals"]}
    assert "pending" in statuses  # some rows never started
    assert fake_upstream.activate_calls == 0

    await resume(engine, batch_id)
    snapshot = await engine.get_status(batch_id)
    assert snapshot["status"] == "completed"
    assert snapshot["data"]["processed_hospitals"] == 8
    assert fake_upstream.create_calls == 8  # every row created exactly once


async def test_pause_non_processing_batch_conflicts(engine, fake_upstream):
    batch_id = await run_batch(engine, "A")
    batch = await engine.batch_dao.get(batch_id)
    with pytest.raises(BatchConflictError, match="only a processing batch"):
        await engine.pause_batch(batch)


async def test_progress_events_published_to_subscribers(engine, fake_upstream):
    fake_upstream.response_delay = 0.03
    batch_id = await engine.start_batch(rows("A", "B"))
    queue = engine.broker.subscribe(batch_id)
    await engine.join(batch_id)
    events = []
    while not queue.empty():
        events.append(queue.get_nowait())
    kinds = [e["event"] for e in events]
    assert kinds.count("progress") == 2
    assert kinds[-1] == "completed"
    final = events[-1]["data"]  # {status, data} envelope
    assert final["status"] == "completed"
    assert final["data"]["batch_activated"] is True


async def test_processing_time_accumulates_across_resume(engine, fake_upstream):
    fake_upstream.fail_status = 400
    fake_upstream.fail_always = {"A"}
    batch_id = await run_batch(engine, "A")
    first = (await engine.get_status(batch_id))["data"]["processing_time_seconds"]
    fake_upstream.fail_always = set()
    await resume(engine, batch_id)
    second = (await engine.get_status(batch_id))["data"]["processing_time_seconds"]
    assert second >= first
