"""Shared fixtures: a fake Hospital Directory upstream behind an httpx
MockTransport, with failure injection for retry/resume scenarios, plus a
TestClient wired to it. Backoff is tightened so retry tests run in ms."""
from __future__ import annotations

import asyncio
import itertools
import json
import os
import re
import time
from typing import Optional

import httpx
import pytest
from fastapi.testclient import TestClient

# must be set before app.core.config is imported; the mock transport
# intercepts every request, so the host itself never resolves
os.environ.setdefault("HOSPITAL_API_BASE_URL", "http://hospital-directory.test")

from app.core import config  # noqa: E402
from app.main import create_app  # noqa: E402

ACTIVATE_RE = re.compile(r"^/hospitals/batch/([0-9a-f-]+)/activate$")


class FakeHospitalDirectory:
    """Stands in for the deployed Hospital Directory API."""

    def __init__(self) -> None:
        self.hospitals: dict = {}
        self._ids = itertools.count(101)
        self.activated_batches: list = []
        self.create_calls = 0
        self.activate_calls = 0
        self.in_flight = 0
        self.max_in_flight = 0
        # failure injection
        self.fail_always: set = set()        # names that always fail
        self.fail_once: dict = {}            # name -> remaining failures
        self.fail_status = 500
        self.retry_after: Optional[str] = None
        self.activate_failures = 0           # fail the next N activate calls
        self.response_delay = 0.0

    async def handler(self, request: httpx.Request) -> httpx.Response:
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            if self.response_delay:
                await asyncio.sleep(self.response_delay)
            if request.method == "POST" and request.url.path == "/hospitals/":
                return self._create(json.loads(request.content))
            match = ACTIVATE_RE.match(request.url.path)
            if request.method == "PATCH" and match:
                return self._activate(match.group(1))
            return httpx.Response(404, json={"detail": "not found"})
        finally:
            self.in_flight -= 1

    def _create(self, body: dict) -> httpx.Response:
        self.create_calls += 1
        name = body["name"]
        if name in self.fail_always:
            return self._failure()
        if self.fail_once.get(name, 0) > 0:
            self.fail_once[name] -= 1
            return self._failure()
        hospital = {
            "id": next(self._ids),
            "name": name,
            "address": body["address"],
            "phone": body.get("phone"),
            "creation_batch_id": body.get("creation_batch_id"),
            "active": False,
            "created_at": "2026-01-01T00:00:00Z",
        }
        self.hospitals[hospital["id"]] = hospital
        return httpx.Response(200, json=hospital)

    def _activate(self, batch_id: str) -> httpx.Response:
        self.activate_calls += 1
        if self.activate_failures > 0:
            self.activate_failures -= 1
            return self._failure()
        self.activated_batches.append(batch_id)
        for hospital in self.hospitals.values():
            if hospital["creation_batch_id"] == batch_id:
                hospital["active"] = True
        return httpx.Response(200, json={"message": "activated"})

    def _failure(self) -> httpx.Response:
        headers = {"Retry-After": self.retry_after} if self.retry_after else {}
        return httpx.Response(self.fail_status, json={"detail": "boom"}, headers=headers)


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def fake_upstream(monkeypatch) -> FakeHospitalDirectory:
    monkeypatch.setattr(config, "BACKOFF_BASE_SECONDS", 0.01)
    monkeypatch.setattr(config, "BACKOFF_CAP_SECONDS", 0.05)
    return FakeHospitalDirectory()


@pytest.fixture
def client(fake_upstream):
    app = create_app(upstream_transport=httpx.MockTransport(fake_upstream.handler))
    with TestClient(app) as test_client:
        yield test_client


def make_csv(rows, header="name,address,phone") -> bytes:
    lines = [header] + list(rows)
    return ("\n".join(lines) + "\n").encode("utf-8")


def upload(client, content: bytes, path: str = config.API_PREFIX, filename="hospitals.csv"):
    return client.post(path, files={"file": (filename, content, "text/csv")})


def wait_for_terminal(client, batch_id: str, timeout: float = 5.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get("{}/{}".format(config.API_PREFIX, batch_id)).json()
        if body["status"] != "processing":
            return body
        time.sleep(0.02)
    raise AssertionError("batch {} did not finish within {}s".format(batch_id, timeout))
