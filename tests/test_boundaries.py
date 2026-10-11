import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import httpx
import pytest
from conftest import metadata_at

from kalshi_market_data import CONTRACT, producer
from kalshi_market_data.budget import RunMeter
from kalshi_market_data.foundation import BudgetExceeded, Store, digest, json_bytes, read_json
from kalshi_market_data.metadata import load_metadata, pin_metadata
from kalshi_market_data.normalize import timestamp
from kalshi_market_data.transport import Client, api_origin
from kalshi_market_data.workflow import DEFAULT_LIMITS


def metered(tmp_path, **changes):
    store = Store(tmp_path / "root", CONTRACT, replace(DEFAULT_LIMITS, **changes))
    store.run_meter = RunMeter(store.limits, {})
    return store


def test_short_partial_read_counts_every_received_byte(tmp_path):
    class Partial(httpx.SyncByteStream):
        def __iter__(self):
            yield b"x" * 100
            raise httpx.ReadError("source terminated")

    store = metered(tmp_path)
    client = Client(
        store,
        transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=Partial())),
        sleep=lambda _: None,
    )
    with pytest.raises(ValueError, match="transport"):
        client.get("/historical/cutoff", {})
    assert store.run_meter.data["measured_requests"] == 3
    assert store.run_meter.data["measured_downloaded_bytes"] == 300
    assert store.run_meter.charged == (3, 300)
    client.close()


def test_response_limit_counts_crossing_bytes_and_redirect_refused(tmp_path):
    store = metered(tmp_path, max_response_bytes=10)
    client = Client(
        store, transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b"x" * 20))
    )
    with pytest.raises(BudgetExceeded):
        client.get("/historical/cutoff", {})
    assert store.run_meter.charged == (1, 20)
    assert store.run_meter.data["measured_downloaded_bytes"] == 20
    assert store.run_meter.data["reservations"] == {}
    client.close()
    store = metered(tmp_path / "other")
    client = Client(
        store,
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                302, headers={"Location": "https://evil.invalid/"}, content=b"redirect"
            )
        ),
    )
    with pytest.raises(ValueError, match="redirect"):
        client.get("/historical/cutoff", {})
    assert store.run_meter.charged == (1, 8)
    client.close()


def test_registered_origin_and_explicit_loopback_only(monkeypatch):
    for origin in (
        "http://external-api.kalshi.com/trade-api/v2",
        "https://evil.invalid/trade-api/v2",
        "https://user@external-api.kalshi.com/trade-api/v2",
        "http://127.0.0.1:1234/trade-api/v2",
    ):
        with pytest.raises(ValueError):
            api_origin(origin)
    monkeypatch.setenv("KALSHI_MARKET_DATA_ALLOW_LOOPBACK", "1")
    assert api_origin("http://127.0.0.1:1234/trade-api/v2").startswith("http:")
    with pytest.raises(ValueError):
        api_origin("http://localhost:1234/trade-api/v2")


def test_pin_copy_outlives_producer_tamper_inventory_and_symlink(tmp_path):
    directory, pin = metadata_at(tmp_path / "metadata")
    store = metered(tmp_path)
    bundle = pin_metadata(store.root, directory, pin, store=store)
    assert bundle.path != directory and load_metadata(bundle.path, pin).markets
    (directory / "markets.json").write_bytes(b"[]")
    assert load_metadata(bundle.path, pin).markets
    with pytest.raises(ValueError, match="digest"):
        load_metadata(directory, pin)
    directory, pin = metadata_at(tmp_path / "another")
    (directory / "extra.json").write_bytes(b"[]")
    with pytest.raises(ValueError, match="unexpected"):
        load_metadata(directory, pin)
    (directory / "extra.json").unlink()
    target = directory / "series.json"
    original = target.read_bytes()
    target.unlink()
    (tmp_path / "source.json").write_bytes(original)
    target.symlink_to(tmp_path / "source.json")
    with pytest.raises(OSError):
        load_metadata(directory, pin)


def test_usable_parent_identity_and_file_aggregate_caps(tmp_path, monkeypatch):
    directory, pin = metadata_at(tmp_path / "metadata")
    manifest = read_json(directory / "manifest.json")
    rows = read_json(directory / "markets.json")
    rows[0]["series_ticker"] = "UNRELATED"
    raw = json_bytes(rows)
    (directory / "markets.json").write_bytes(raw)
    manifest["files"]["markets.json"] = {"size": len(raw), "sha256": digest(raw)}
    manifest["source_release_id"] = digest(json_bytes(manifest["files"]))
    raw = json_bytes(manifest)
    (directory / "manifest.json").write_bytes(raw)
    with pytest.raises(ValueError, match="parent"):
        load_metadata(directory, digest(raw))
    from kalshi_market_data import metadata

    directory, pin = metadata_at(tmp_path / "large")
    monkeypatch.setattr(metadata, "MAX_BUNDLE", 1000)
    with pytest.raises(ValueError, match="bundle limit"):
        load_metadata(directory, pin)
    monkeypatch.setattr(metadata, "MAX_FILE", 20)
    with pytest.raises(ValueError, match="bounded regular"):
        load_metadata(directory, pin)


def test_delegated_environment_and_disjoint_roots(tmp_path, monkeypatch):
    monkeypatch.setenv("KALSHI_EVENTS_DATA_DIR", "/tmp/evil")
    monkeypatch.setenv("KALSHI_EVENTS_API_URL", "https://evil.invalid")
    env = producer.environment(tmp_path / "producer")
    assert env["KALSHI_EVENTS_DATA_DIR"] == str(tmp_path / "producer" / "data")
    assert "KALSHI_EVENTS_API_URL" not in env
    store = metered(tmp_path)
    for root in (store.root, store.root / "producer", store.root.parent):
        with pytest.raises(ValueError, match="disjoint"):
            producer.refresh_metadata(store, ["KXTEST-E1-A"], sys.executable, root)


def test_producer_timeout_diagnostics_and_persistent_state_survive(tmp_path):
    store = metered(tmp_path)
    persistent = tmp_path / "producer"
    persistent.mkdir()
    marker = persistent / "raw"
    marker.write_bytes(b"retain")
    temporary = store.root / ".state" / "producer"
    temporary.mkdir()
    with store.lock:
        with pytest.raises(ValueError, match="deadline"):
            producer.bounded_run(
                [sys.executable, "-c", "import time;time.sleep(10)"],
                env=dict(os.environ),
                directory=temporary,
                store=store,
                timeout=0.1,
            )
        with pytest.raises(ValueError, match="diagnostics"):
            producer.bounded_run(
                [sys.executable, "-c", "print('x'*10000)"],
                env=dict(os.environ),
                directory=temporary,
                store=store,
                max_diagnostics=10,
            )
    assert marker.read_bytes() == b"retain"


def test_unknown_reservation_survives_restart_and_cannot_be_refunded(tmp_path):
    store = metered(tmp_path, max_requests=3, max_download_bytes=300)
    state = {}
    meter = RunMeter(store.limits, state)
    token = meter.reserve(3, 300, "producer")
    restarted = RunMeter(store.limits, state)
    assert restarted.remaining == (0, 0) and token in state["reservations"]
    with pytest.raises(BudgetExceeded):
        restarted.reserve(1, 1, "unproven retry")


def test_metadata_fifo_is_rejected_without_waiting_for_writer(tmp_path):
    directory, pin = metadata_at(tmp_path / "metadata")
    (directory / "series.json").unlink()
    os.mkfifo(directory / "series.json")
    with pytest.raises(ValueError, match="bounded regular"):
        load_metadata(directory, pin)


@pytest.mark.parametrize("download_cap", [1024**2, 32 * 1024**2])
def test_delegated_response_cap_fits_remaining_download_grant(tmp_path, monkeypatch, download_cap):
    store = metered(tmp_path, max_download_bytes=download_cap)
    consumed = store.run_meter.reserve(1, 17, "prior HTTP")
    store.run_meter.settle(consumed, 1, 17)
    grants = []

    def child(argv, **kwargs):
        download = int(argv[argv.index("--max-download-bytes") + 1])
        response = int(argv[argv.index("--max-response-bytes") + 1])
        assert 0 < response <= download
        assert response == min(store.limits.max_response_bytes, download)
        grants.append((download, response))
        directory = Path(argv[argv.index("--output") + 1])
        _, pin = metadata_at(directory)
        manifest = read_json(directory / "manifest.json")
        return subprocess.CompletedProcess(
            argv,
            0,
            json_bytes(
                {
                    "output": str(directory),
                    "manifest_sha256": pin,
                    "source_release_id": manifest["source_release_id"],
                    "requests": 1,
                    "downloaded_bytes": 17,
                    "accounting_complete": True,
                }
            ),
        )

    monkeypatch.setattr(producer, "bounded_run", child)
    with store.lock:
        for _ in range(2):
            assert producer.refresh_metadata(
                store, ["KXTEST-E1-A"], sys.executable, tmp_path / "producer"
            ).markets
    assert grants == [
        (download_cap - 17, min(16 * 1024**2, download_cap - 17)),
        (download_cap - 34, min(16 * 1024**2, download_cap - 34)),
    ]
    assert store.run_meter.charged == (3, 51)
    assert store.run_meter.data["reservations"] == {}


@pytest.mark.parametrize(
    "value", ["2020-01-01T00:00:00.000000000Z", "2020-01-01T00:00:00.000000000+00:00"]
)
def test_cli_windows_accept_exact_whole_second_with_zero_nanosecond_fraction(value):
    assert timestamp(value) == 1577836800


@pytest.mark.parametrize(
    "value",
    [
        "2020-01-01T00:00:00.000000001Z",
        "2020-01-01T00:00:00.000000001+00:00",
        "2020-01-01T00:00:00+01:00",
        "2020-01-01T00:00:00",
        "2020-01-01T00:00:00+00:00:00.000000001",
        "20200101T000000.000000001Z",
        "not-a-date",
    ],
)
def test_cli_windows_reject_nonintegral_nanoseconds_or_non_utc_shapes(value):
    with pytest.raises(ValueError):
        timestamp(value)
