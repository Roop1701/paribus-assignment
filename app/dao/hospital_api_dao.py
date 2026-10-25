"""HTTP data access for the remote Hospital Directory API.

All upstream calls go through _request, which applies the retry policy:
exponential backoff with jitter on transient statuses (429/5xx) and network
errors, honouring a Retry-After header when the upstream sends one. Any
other 4xx is permanent and raised immediately. Config values are read at
call time so tests can tighten them.
"""
from __future__ import annotations

import asyncio
import random
from typing import Optional

import httpx

from ..core import config


class UpstreamError(Exception):
    """A single upstream call failed permanently (retries exhausted or 4xx)."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class HospitalApiDao:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def create_hospital(
        self, *, name: str, address: str, phone: Optional[str], batch_id: str
    ) -> dict:
        response = await self._request(
            "POST",
            "/hospitals/",
            json={
                "name": name,
                "address": address,
                "phone": phone,
                "creation_batch_id": batch_id,
            },
        )
        return response.json()

    async def activate_batch(self, batch_id: str) -> None:
        await self._request("PATCH", "/hospitals/batch/{}/activate".format(batch_id))

    async def _request(self, method: str, url: str, json: Optional[dict] = None) -> httpx.Response:
        attempts = config.MAX_RETRIES + 1
        last_error = "upstream request failed"
        retry_after: Optional[float] = None
        for attempt in range(attempts):
            if attempt:
                await asyncio.sleep(self._delay(attempt - 1, retry_after))
            try:
                response = await self._client.request(method, url, json=json)
            except httpx.HTTPError as exc:
                last_error = "network error calling {} {}: {}".format(method, url, exc)
                retry_after = None
                continue
            if response.status_code < 400:
                return response
            last_error = "upstream returned {} for {} {}: {}".format(
                response.status_code, method, url, response.text[:200]
            )
            if response.status_code not in config.RETRYABLE_STATUS_CODES:
                raise UpstreamError(last_error, response.status_code)
            retry_after = _parse_retry_after(response)
        raise UpstreamError(last_error)

    @staticmethod
    def _delay(backoff_step: int, retry_after: Optional[float]) -> float:
        delay = min(
            config.BACKOFF_CAP_SECONDS, config.BACKOFF_BASE_SECONDS * (2 ** backoff_step)
        )
        delay += random.uniform(0, config.BACKOFF_BASE_SECONDS)  # jitter de-syncs workers
        if retry_after is not None:
            delay = max(delay, retry_after)
        return delay


def _parse_retry_after(response: httpx.Response) -> Optional[float]:
    value = response.headers.get("Retry-After")
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None
