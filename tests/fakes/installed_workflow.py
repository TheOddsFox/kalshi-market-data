"""Installed-wheel pinned workflow: no checkout imports or producer process."""

import hashlib
import json
import sys
from decimal import Decimal
from pathlib import Path

import duckdb
import httpx
import pyarrow.parquet as pq

import kalshi_market_data
from kalshi_market_data.cli import parser
from kalshi_market_data.foundation import digest, json_bytes
from kalshi_market_data.workflow import replay, run, settings

root = Path.cwd()
installed = Path(kalshi_market_data.__file__).resolve()
assert installed.is_relative_to(Path(sys.argv[1]).resolve()), installed
metadata = root / "metadata"
metadata.mkdir()
market = "KXTEST-E1-A"
start = 1577836800
relations = {
    "series.json": [{"series_ticker": "KXTEST"}],
    "events.json": [{"event_ticker": "KXTEST-E1", "series_ticker": "KXTEST"}],
    "markets.json": [
        {
            "market_ticker": market,
            "event_ticker": "KXTEST-E1",
            "series_ticker": "KXTEST",
            "usable": True,
            "status": "open",
            "settlement_ts": None,
            "observation_id": "installed",
            "received_at": "2020-01-01T00:00:00Z",
        }
    ],
    "coverage.json": [{"market_ticker": market, "status": "found", "error": None}],
}
inventory = {}
for name, rows in relations.items():
    raw = json_bytes(rows)
    (metadata / name).write_bytes(raw)
    inventory[name] = {"sha256": hashlib.sha256(raw).hexdigest(), "size": len(raw)}
manifest = {
    "contract": "oddsfox.kalshi.metadata.v1",
    "coverage_scope": "targeted",
    "source_release_id": digest(json_bytes(inventory)),
    "files": inventory,
    "received_at": "2020-01-01T00:00:00Z",
}
raw = json_bytes(manifest)
(metadata / "manifest.json").write_bytes(raw)
args = parser().parse_args(
    [
        "--root",
        str(root / "operator"),
        "run",
        "--market-ticker",
        market,
        "--feed",
        "candles",
        "--feed",
        "trades",
        "--feed",
        "books",
        "--start",
        "2020-01-01T00:00:00Z",
        "--end",
        "2020-01-01T01:00:00Z",
        "--metadata",
        str(metadata),
        "--metadata-sha256",
        digest(raw),
    ]
)


def exchange(request):
    path = request.url.path
    if path.endswith("cutoff"):
        return httpx.Response(200, json={"market_settled_ts": start, "trades_created_ts": start})
    if path.endswith("candlesticks"):
        if "/historical/" in path:
            return httpx.Response(404, json={"error": "not archived"})
        return httpx.Response(
            200,
            json={
                "ticker": market,
                "candlesticks": [
                    {
                        "end_period_ts": start + 60,
                        "price": {"open_dollars": "0.1234"},
                        "volume_fp": "0.25",
                        "open_interest_fp": "1.50",
                    }
                ],
            },
        )
    if path.endswith("trades"):
        return httpx.Response(
            200,
            json={
                "cursor": "",
                "trades": [
                    {
                        "trade_id": "1",
                        "ticker": market,
                        "created_time": "2020-01-01T00:01:00Z",
                        "count_fp": "0.25",
                        "taker_side": "yes",
                        "yes_price_dollars": "0.1234",
                        "no_price_dollars": "0.8766",
                        "is_block_trade": True,
                    }
                ],
            },
        )
    if path.endswith("orderbook"):
        return httpx.Response(200, json={"orderbook_fp": {"yes_dollars": [], "no_dollars": []}})
    raise AssertionError(path)


result = run(
    args.root, settings(args), transport=httpx.MockTransport(exchange), sleep=lambda _: None
)
release = Path(args.root) / "published" / result["release_id"]
assert pq.read_table(release / "trades.parquet")["count"].to_pylist() == [Decimal("0.25")]
assert len(pq.read_table(release / "book_snapshots.parquet")) == 1
assert len(pq.read_table(release / "book_levels.parquet")) == 0
with duckdb.connect() as db:
    assert db.execute(
        "SELECT yes_price FROM read_parquet(?)", [str(release / "trades.parquet")]
    ).fetchone()[0] == Decimal("0.1234")
assert replay(args.root, result["acquisition_id"])["release_id"] == result["release_id"]
assert replay(args.root, result["acquisition_id"])["release_id"] == result["release_id"]
assert (
    run(
        args.root,
        run_id=result["run_id"],
        transport=httpx.MockTransport(lambda _: (_ for _ in ()).throw(AssertionError("IO"))),
    )["release_id"]
    == result["release_id"]
)
print(json.dumps({"installed_pinned_workflow": "passed", "release_id": result["release_id"]}))
