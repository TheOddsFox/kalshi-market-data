#!/usr/bin/env python3
"""Exercise real installed Kalshi wheels against a synthetic loopback exchange."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

if not __debug__:
    raise SystemExit("installed acceptance requires Python assertions; disable optimization")

LIVE = "KXTEST-E1-A"
OLD = "KXTEST-E0-H"
COMBO = "KXTEST-C1-C"
START = "2026-04-14T00:00:00Z"
END = "2026-04-14T02:00:00Z"
CANDLE_END = int(datetime(2026, 4, 14, 1, tzinfo=timezone.utc).timestamp())
CUTOFF = {
    "market_settled_ts": "2026-05-01T00:00:00Z",
    "trades_created_ts": "2026-05-01T00:00:00Z",
    "orders_updated_ts": "2026-05-01T00:00:00Z",
    "market_positions_last_updated_ts": "2026-05-01T00:00:00Z",
}
SERIES = {
    "ticker": "KXTEST",
    "title": "Synthetic series",
    "frequency": "daily",
    "category": "Sports",
    "tags": ["Synthetic"],
    "settlement_sources": [],
}


def market(ticker, event, *, archived=False, combo=False):
    value = {
        "ticker": ticker,
        "event_ticker": event,
        "market_type": "binary",
        "title": "Synthetic contract",
        "yes_sub_title": "Yes",
        "no_sub_title": "No",
        "created_time": "2025-01-01T00:00:00Z",
        "updated_time": "2026-04-14T00:00:00Z",
        "open_time": "2025-01-01T00:00:00Z",
        "close_time": "2027-01-01T00:00:00Z",
        "status": "finalized" if archived else "active",
        "result": "yes" if archived else "",
        "settlement_ts": "2026-01-01T00:00:00Z" if archived else None,
        "settlement_value_dollars": "1.0000" if archived else None,
        "last_price_dollars": "0.5123",
        "volume_fp": "1.55",
        "open_interest_fp": "0.01",
        "rules_primary": "Synthetic rule.",
        "rules_secondary": "",
        "price_level_structure": "deci_cent",
        "price_ranges": [{"start": "0.0000", "end": "1.0000", "step": "0.0001"}],
    }
    if combo:
        value["mve_collection_ticker"] = "KXTEST-COLLECTION"
        value["mve_selected_legs"] = [
            {"event_ticker": "KXTEST-E1", "market_ticker": LIVE, "side": "yes"}
        ]
    return value


MARKETS = {
    LIVE: market(LIVE, "KXTEST-E1"),
    OLD: market(OLD, "KXTEST-E0", archived=True),
    COMBO: market(COMBO, "KXTEST-C1", combo=True),
}
EVENTS = {
    key: {
        "event_ticker": key,
        "series_ticker": "KXTEST",
        "title": "Synthetic event",
        "sub_title": "Synthetic",
        "category": "Sports",
        "mutually_exclusive": True,
        "markets": [value],
    }
    for key, value in [
        ("KXTEST-E1", MARKETS[LIVE]),
        ("KXTEST-E0", MARKETS[OLD]),
        ("KXTEST-C1", MARKETS[COMBO]),
    ]
}


def candle(archived):
    suffix = "" if archived else "_dollars"
    result = {"end_period_ts": CANDLE_END}
    for section in ("price", "yes_bid", "yes_ask"):
        result[section] = {name + suffix: "0.5123" for name in ("open", "high", "low", "close")}
    result["price"].update({"mean" + suffix: "0.5123", "previous" + suffix: "0.5000"})
    result["volume" if archived else "volume_fp"] = "1.55"
    result["open_interest" if archived else "open_interest_fp"] = "0.01"
    return result


class Exchange(BaseHTTPRequestHandler):
    calls = []

    def log_message(self, *_):
        pass

    def do_GET(self):
        request = urlsplit(self.path)
        path = request.path.removeprefix("/trade-api/v2")
        query = parse_qs(request.query)
        self.calls.append((path, query))
        assert "Authorization" not in self.headers
        cursor = query.get("cursor", [""])[0]
        status = 200
        if path == "/historical/cutoff":
            value = CUTOFF
        elif path == "/series":
            value = {"series": [SERIES]}
        elif path == "/series/KXTEST":
            value = {"series": SERIES}
        elif path == "/events":
            value = {
                "events": [EVENTS["KXTEST-E0"] if cursor else EVENTS["KXTEST-E1"]],
                "cursor": "" if cursor else "events-next",
            }
        elif path == "/events/multivariate":
            value = {"events": [EVENTS["KXTEST-C1"]], "cursor": ""}
        elif path.startswith("/events/") and path.split("/")[-1] in EVENTS:
            value = {
                "event": EVENTS[path.split("/")[-1]],
                "markets": EVENTS[path.split("/")[-1]]["markets"],
            }
        elif path == "/markets":
            value = {
                "markets": [MARKETS[COMBO] if cursor else MARKETS[LIVE]],
                "cursor": "" if cursor else "markets-next",
            }
        elif path == "/historical/markets":
            value = {"markets": [MARKETS[OLD]], "cursor": ""}
        elif path in ("/markets/trades", "/historical/trades"):
            ticker = query.get("ticker", [LIVE])[0]
            rows = (
                []
                if path == "/markets/trades"
                else [
                    {
                        "trade_id": "synthetic-trade-" + ticker,
                        "ticker": ticker,
                        "count_fp": "1.55",
                        "yes_price_dollars": "0.5123",
                        "no_price_dollars": "0.4877",
                        "created_time": "2026-04-14T00:10:00Z",
                        "taker_outcome_side": "yes",
                        "taker_book_side": "bid",
                        "taker_side": "yes",
                        "is_block_trade": True,
                    }
                ]
            )
            value = {"trades": rows, "cursor": ""}
        elif path.endswith("/candlesticks"):
            ticker = path.split("/")[-2]
            archived = path.startswith("/historical/")
            if archived == (ticker == OLD):
                value = {"ticker": ticker, "candlesticks": [candle(archived)]}
            else:
                status, value = 404, {"code": "not_found"}
        elif path.endswith("/orderbook"):
            ticker = path.split("/")[-2]
            value = {
                "orderbook_fp": {
                    "yes_dollars": [] if ticker == OLD else [["0.5123", "1.55"]],
                    "no_dollars": [] if ticker == OLD else [["0.4800", "0.01"]],
                }
            }
        elif path.startswith(("/markets/", "/historical/markets/")):
            ticker = path.split("/")[-1]
            archived = path.startswith("/historical/")
            if ticker in MARKETS and archived == (ticker == OLD):
                value = {"market": MARKETS[ticker]}
            else:
                status, value = 404, {"code": "not_found"}
        else:
            status, value = 404, {"code": "not_found"}
        raw = json.dumps(value, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


def command(executable, arguments, *, cwd, env):
    result = subprocess.run(
        [str(executable), *map(str, arguments)],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )
    if result.returncode:
        raise RuntimeError(f"{Path(executable).name} {arguments}: {result.stdout}\n{result.stderr}")
    return json.loads(result.stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events-executable", type=Path, required=True)
    parser.add_argument("--market-data-executable", type=Path, required=True)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    executables = [p.absolute() for p in (args.events_executable, args.market_data_executable)]
    with tempfile.TemporaryDirectory(
        prefix="kalshi-installed-handoff-", dir=Path(tempfile.gettempdir()).resolve()
    ) as scratch:
        work = Path(scratch)
        env = {
            key: value
            for key, value in os.environ.items()
            if key != "PYTHONPATH" and not key.startswith(("KALSHI_EVENTS_", "KALSHI_MARKET_DATA_"))
        }
        env["PYTHONNOUSERSITE"] = "1"
        origins = command(
            args.python.absolute(),
            [
                "-c",
                "import importlib.util,json;print(json.dumps("
                "[importlib.util.find_spec('kalshi_market_data').origin]))",
            ],
            cwd=work,
            env=env,
        )
        assert all("site-packages" in p for p in origins), (
            "Use installed wheels, not editable checkouts"
        )
        Exchange.calls = []
        server = ThreadingHTTPServer(("127.0.0.1", 0), Exchange)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        origin = f"http://127.0.0.1:{server.server_port}/trade-api/v2"
        for prefix in ("KALSHI_EVENTS", "KALSHI_MARKET_DATA"):
            env[prefix + "_API_URL"] = origin
            env[prefix + "_ALLOW_LOOPBACK"] = "1"
        try:
            report = workflow(executables[0], executables[1], args.python.absolute(), work, env)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
        report.update(
            installed_origins=origins, source="synthetic-loopback", requests=len(Exchange.calls)
        )
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, sort_keys=True))


def workflow(events, data, python, work, env):
    producer = work / "events-root"
    consumer = work / "market-data-root"
    bundle = work / "metadata"

    def run_events(*arguments):
        return command(events, ["--root", producer, *arguments], cwd=work, env=env)

    def run_data(*arguments):
        return command(data, ["--root", consumer, *arguments], cwd=work, env=env)

    capture = run_events("capture")
    assert capture["complete"]
    run_events("load")
    run_events("build")
    catalogue = run_events("publish")
    run_events("verify", "--release", catalogue["output"])
    run_events("replay", "--capture-id", capture["capture_id"])
    replayed_catalogue = run_events("publish")
    assert replayed_catalogue["source_release_id"] == catalogue["source_release_id"]
    backup = work / "backup"
    run_events("backup", "--output", backup)
    restored = work / "restored-events"
    command(events, ["--root", restored, "restore", "--backup", backup], cwd=work, env=env)
    restored_catalogue = command(events, ["--root", restored, "current"], cwd=work, env=env)
    assert restored_catalogue["source_release_id"] == catalogue["source_release_id"]
    run_events(
        "metadata", "export", "--market-ticker", LIVE, "--market-ticker", OLD, "--output", bundle
    )
    pin = hashlib.sha256((bundle / "manifest.json").read_bytes()).hexdigest()
    selection = [
        "--market-ticker",
        LIVE,
        "--market-ticker",
        OLD,
        "--feed",
        "candles",
        "--feed",
        "trades",
        "--feed",
        "books",
        "--start",
        START,
        "--end",
        END,
        "--period-interval",
        "60",
        "--period-interval",
        "1",
        "--period-interval",
        "1440",
    ]
    result = run_data("run", "--metadata", bundle, "--metadata-sha256", pin, *selection)
    for ticker in (LIVE, OLD):
        candle_paths = [
            path for path, _ in Exchange.calls if path.endswith(f"/{ticker}/candlesticks")
        ]
        assert candle_paths and candle_paths[0].startswith("/historical/") == (ticker == OLD)
    run_id = result["run_id"]
    before = len(Exchange.calls)
    resumed = run_data("resume", "--run-id", run_id)
    assert len(Exchange.calls) == before, "Completed resume must make zero requests"
    assert resumed["release_id"] == result["release_id"]
    replay = run_data("replay", "--acquisition-id", result["acquisition_id"])
    assert replay["release_id"] == result["release_id"]
    run_data("verify")
    checks = command(
        python,
        ["-c", FACT_CHECK, catalogue["output"], consumer / "published" / result["release_id"]],
        cwd=work,
        env=env,
    )
    source_pointer = (producer / "current.json").read_bytes()
    initial_raw = set((producer / "raw").rglob("body.bin"))
    refreshed = []
    for number in range(2):
        before = len(Exchange.calls)
        delegated = command(
            data,
            [
                "--root",
                work / f"delegated-{number}",
                "run",
                "--events-executable",
                events,
                "--events-root",
                producer,
                *(["--max-download-bytes", "1048576"] if number else []),
                *selection,
            ],
            cwd=work,
            env=env,
        )
        assert delegated["usage"]["measured_requests"] == len(Exchange.calls) - before
        assert delegated["usage"]["measured_requests"] == delegated["usage"]["charged_requests"]
        assert (producer / "current.json").read_bytes() == source_pointer
        assert set((producer / "raw").rglob("body.bin")) > initial_raw
        refreshed.append({"run_id": delegated["run_id"], "release_id": delegated["release_id"]})
    # Prove pinned consumption survives removal of the producer and original bundle.
    producer.rename(work / "unavailable-producer")
    bundle.rename(work / "unavailable-bundle")
    before = len(Exchange.calls)
    run_data("replay", "--acquisition-id", result["acquisition_id"])
    run_data("verify")
    assert len(Exchange.calls) == before
    return {
        "status": "pass",
        "pinned_run": {
            key: result[key] for key in ("run_id", "acquisition_id", "release_id", "usage")
        },
        "completed_resume": {"release_id": resumed["release_id"], "acquisition_requests": 0},
        "catalogue_release": catalogue["source_release_id"],
        "fact_checks": checks,
        "persistent_refreshes": refreshed,
        "independent_replay": True,
    }


FACT_CHECK = """
import json, sys
import importlib.util
from pathlib import Path
from decimal import Decimal
import pyarrow.parquet as pq
assert importlib.util.find_spec('kalshi_events') is None
catalogue, release = map(Path, sys.argv[1:])
def rows(root, name):
    return pq.read_table(root / (name + '.parquet')).to_pylist()
markets = rows(catalogue, 'markets')
assert len(markets) == 3
assert {r['status'] for r in markets} == {'active', 'finalized'}
assert all(r['last_price_dollars'] == Decimal('0.5123') for r in markets)
assert len(rows(catalogue, 'combo_legs')) == 1
instruments = rows(release, 'instruments')
assert len(instruments) == 4
assert {r['outcome_side'] for r in instruments} == {'YES', 'NO'}
candles = rows(release, 'candlesticks')
assert len(candles) == 6
assert {r['period_interval'] for r in candles} == {1, 60, 1440}
assert all(r['price_close'] == Decimal('0.5123') and r['volume'] == Decimal('1.55') for r in candles)
assert len(rows(release, 'latest_candlesticks')) == 6
trades = rows(release, 'trades')
assert len(trades) == 2
assert all(r['count'] == Decimal('1.55') and r['yes_price'] == Decimal('0.5123') for r in trades)
assert all(r['is_block_trade'] and r['taker_book_side'] == 'bid' for r in trades)
books = rows(release, 'book_snapshots')
assert len(books) == 2 and any(r['yes_levels'] == r['no_levels'] == 0 for r in books)
levels = rows(release, 'book_levels')
assert len(levels) == 2 and {r['book_side'] for r in levels} == {'bid'}
assert len(rows(release, 'coverage')) == 6
print(json.dumps({'exact_decimals': True, 'periods': [1, 60, 1440], 'trades': 2, 'empty_book': True}))
"""


if __name__ == "__main__":
    main()
