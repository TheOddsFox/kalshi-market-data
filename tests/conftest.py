from pathlib import Path

import pytest

from kalshi_market_data.cli import parser
from kalshi_market_data.foundation import digest, json_bytes
from kalshi_market_data.workflow import settings

START = 1577836800
MARKET = "KXTEST-E1-A"


def metadata_at(directory, *, market=None):
    directory = Path(directory)
    directory.mkdir()
    rows = {
        "series.json": [{"series_ticker": "KXTEST", "ticker": "KXTEST"}],
        "events.json": [{"event_ticker": "KXTEST-E1", "series_ticker": "KXTEST"}],
        "markets.json": [
            market
            or {
                "market_ticker": MARKET,
                "ticker": MARKET,
                "event_ticker": "KXTEST-E1",
                "series_ticker": "KXTEST",
                "usable": True,
                "status": "open",
                "settlement_ts": None,
                "observation_id": "fixture",
                "received_at": "2020-01-01T00:00:00+00:00",
                "provider_updated_at": None,
            }
        ],
        "coverage.json": [{"market_ticker": MARKET, "status": "found", "error": None}],
    }
    files = {}
    for name, value in rows.items():
        raw = json_bytes(value)
        (directory / name).write_bytes(raw)
        files[name] = {"sha256": digest(raw), "size": len(raw)}
    manifest = {
        "contract": "oddsfox.kalshi.metadata.v1",
        "files": files,
        "source_release_id": digest(json_bytes(files)),
        "coverage_scope": "targeted",
        "received_at": "2020-01-01T00:00:00+00:00",
    }
    raw = json_bytes(manifest)
    (directory / "manifest.json").write_bytes(raw)
    return directory, digest(raw)


@pytest.fixture
def specification(tmp_path):
    directory, pin = metadata_at(tmp_path / "metadata")
    args = parser().parse_args(
        [
            "--root",
            str(tmp_path / "root"),
            "run",
            "--market-ticker",
            MARKET,
            "--feed",
            "candles",
            "--feed",
            "trades",
            "--feed",
            "books",
            "--start",
            "2020-01-01T00:00:00Z",
            "--end",
            "2020-01-01T00:03:00Z",
            "--metadata",
            str(directory),
            "--metadata-sha256",
            pin,
        ]
    )
    return settings(args)
