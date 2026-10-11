"""Single-worker, counted, bounded public GET transport."""

from __future__ import annotations

import ipaddress
import os
import socket
import time
from urllib.parse import urlsplit

import httpx

from .foundation import BudgetExceeded, now

API_URL = "https://external-api.kalshi.com/trade-api/v2"


class ClosedStream(httpx.SyncByteStream):
    def __init__(self, stream, closed=False):
        self.stream, self.closed = stream, closed

    def __iter__(self):
        yield from self.stream

    def close(self):
        self.stream.close()
        self.closed = True


def api_origin(value=None):
    value = value or os.environ.get("KALSHI_MARKET_DATA_API_URL", API_URL)
    parsed = urlsplit(value)
    loopback = (
        os.environ.get("KALSHI_MARKET_DATA_ALLOW_LOOPBACK") == "1"
        and parsed.hostname in {"127.0.0.1", "::1"}
        and parsed.scheme == "http"
    )
    if (
        parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path.rstrip("/") != "/trade-api/v2"
        or not loopback
        and (
            parsed.scheme != "https"
            or parsed.hostname != "external-api.kalshi.com"
            or parsed.port not in (None, 443)
        )
    ):
        raise ValueError("source must be the registered public Kalshi origin")
    return value.rstrip("/")


class Client:
    def __init__(self, store, *, duration=120, transport=None, sleep=time.sleep):
        self.store, self.meter = store, store.run_meter
        self.origin = api_origin()
        self.deadline = time.monotonic() + duration
        self.sleep, self.next_request = sleep, 0
        self.client = httpx.Client(
            transport=transport,
            follow_redirects=False,
            trust_env=False,
            timeout=httpx.Timeout(30, connect=10),
            headers={"Accept-Encoding": "identity"},
        )
        if transport is None and self.origin.startswith("https:"):
            addresses = socket.getaddrinfo("external-api.kalshi.com", 443, type=socket.SOCK_STREAM)
            if not addresses or any(
                not ipaddress.ip_address(row[4][0]).is_global for row in addresses
            ):
                raise ValueError("source resolves to a non-public address")

    def close(self):
        self.client.close()

    def get(self, endpoint, params):
        if (
            not endpoint.startswith("/")
            or any(part in {".", ".."} for part in endpoint.split("/"))
            or "?" in endpoint
        ):
            raise ValueError("invalid public endpoint")
        for attempt in range(3):
            remaining_duration = self.deadline - time.monotonic()
            if remaining_duration <= 0:
                raise BudgetExceeded("invocation deadline exhausted")
            delay = max(0, self.next_request - time.monotonic())
            if delay >= remaining_duration:
                raise BudgetExceeded("invocation deadline exhausted")
            self.sleep(delay)
            maximum = min(
                self.store.limits.max_response_bytes,
                self.store.limits.max_temp_bytes,
                self.meter.remaining[1],
                self.store.available_bytes(),
            )
            if maximum <= 0:
                raise BudgetExceeded("response allowance exhausted")
            token = self.meter.reserve(1, maximum, "HTTP GET")
            requested_at, downloaded, guard = now(), 0, None
            self.next_request = time.monotonic() + 0.5
            try:
                with self.client.stream(
                    "GET",
                    self.origin + endpoint,
                    params=params,
                    timeout=min(30, remaining_duration),
                ) as response:
                    guard = ClosedStream(response.stream, response.is_closed)
                    response.stream = guard
                    if response.headers.get("content-encoding", "identity") != "identity":
                        raise ValueError("encoded response refused")
                    chunks = []
                    ingress = (
                        (response.content,) if response.is_stream_consumed else response.iter_raw()
                    )
                    for chunk in ingress:
                        downloaded += len(chunk)
                        if downloaded > maximum:
                            raise BudgetExceeded("response limit exhausted")
                        if time.monotonic() >= self.deadline:
                            raise BudgetExceeded("invocation deadline exhausted")
                        chunks.append(chunk)
                    raw, status = b"".join(chunks), response.status_code
                if 300 <= status < 400:
                    raise ValueError("source redirect refused")
                if status not in (429, 500, 502, 503, 504):
                    return {
                        "status": status,
                        "raw": raw,
                        "requested_at": requested_at,
                        "received_at": now(),
                        "endpoint": endpoint,
                        "params": params,
                    }
            except httpx.HTTPError:
                if attempt == 2:
                    raise ValueError("public source transport failed") from None
            finally:
                if guard is not None and guard.closed:
                    self.meter.settle(token, 1, downloaded)
            if attempt == 2:
                raise ValueError("public source retries exhausted")
            if self.deadline - time.monotonic() <= 2**attempt:
                raise BudgetExceeded("invocation deadline exhausted")
            self.sleep(2**attempt)
        raise ValueError("public source failed")
