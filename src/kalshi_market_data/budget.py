"""Durable request reservations remain charged until proven settled."""

import uuid

from .foundation import BudgetExceeded, now


def nonnegative(value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("invalid resource accounting")


class RunMeter:
    """A reservation remains charged until its owner proves its measured consumption."""

    def __init__(self, limits, data, save=lambda: None):
        self.limits, self.data, self.save = limits, data, save
        if not data:
            data.update(
                measured_requests=0,
                measured_downloaded_bytes=0,
                charged_requests=0,
                charged_downloaded_bytes=0,
                reservations={},
            )
        for key in (
            "measured_requests",
            "measured_downloaded_bytes",
            "charged_requests",
            "charged_downloaded_bytes",
        ):
            self._nonnegative(data.get(key))
        if not isinstance(data.get("reservations"), dict):
            raise ValueError("invalid run accounting journal")
        for value in data["reservations"].values():
            self._nonnegative(value["requests"])
            self._nonnegative(value["downloaded_bytes"])

    _nonnegative = staticmethod(nonnegative)

    @property
    def charged(self):
        reservations = self.data["reservations"].values()
        return (
            self.data["charged_requests"] + sum(value["requests"] for value in reservations),
            self.data["charged_downloaded_bytes"]
            + sum(value["downloaded_bytes"] for value in reservations),
        )

    @property
    def remaining(self):
        requests, downloaded = self.charged
        return (
            max(0, self.limits.max_requests - requests),
            max(0, self.limits.max_download_bytes - downloaded),
        )

    def reserve(self, requests, downloaded, label):
        self._nonnegative(requests)
        self._nonnegative(downloaded)
        remaining = self.remaining
        if requests > remaining[0] or downloaded > remaining[1]:
            raise BudgetExceeded("run request/download allowance exhausted")
        token = uuid.uuid4().hex
        self.data["reservations"][token] = {
            "requests": requests,
            "downloaded_bytes": downloaded,
            "label": label,
            "reserved_at": now(),
        }
        self.save()
        return token

    def settle(self, token, requests, downloaded, *, measured=None):
        self._nonnegative(requests)
        self._nonnegative(downloaded)
        measurement = measured or (requests, downloaded)
        for value in measurement:
            self._nonnegative(value)
        reservation = self.data["reservations"].pop(token)
        self.data["charged_requests"] += requests
        self.data["charged_downloaded_bytes"] += downloaded
        self.data["measured_requests"] += measurement[0]
        self.data["measured_downloaded_bytes"] += measurement[1]
        self.save()
        # Preserve actual crossing bytes before surfacing the exceeded limit.
        if (
            requests > reservation["requests"]
            or downloaded > reservation["downloaded_bytes"]
            or self.charged[0] > self.limits.max_requests
            or self.charged[1] > self.limits.max_download_bytes
        ):
            raise BudgetExceeded("source exceeded reserved run allowance")

    def charge_bytes(self, size):
        self._nonnegative(size)
        self.data["charged_downloaded_bytes"] += size
        self.data["measured_downloaded_bytes"] += size
        self.save()
        if self.charged[1] > self.limits.max_download_bytes:
            raise BudgetExceeded("run downloaded-byte limit exhausted")
