import os
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest
from conftest import MARKET, START, metadata_at
from test_boundaries import metered
from test_workflow import Exchange, candle, execute, trade

from kalshi_market_data import producer, workflow
from kalshi_market_data.foundation import BudgetExceeded, Store, json_bytes, read_json
from kalshi_market_data.normalize import candles, trades
from kalshi_market_data.transport import Client


def test_trade_window_is_half_open_at_nanosecond_precision():
    times = [
        "2019-12-31T23:59:59.999999999Z",
        "2020-01-01T00:00:00Z",
        "2020-01-01T00:00:00.000000001Z",
        "2020-01-01T00:02:59.999999999Z",
        "2020-01-01T00:03:00Z",
    ]
    rows = trades(
        {
            "trades": [
                trade(trade_id=str(index), created_time=value) for index, value in enumerate(times)
            ]
        },
        MARKET,
        START,
        START + 180,
    )
    assert [row["trade_id"] for row in rows] == ["1", "2", "3"]
    assert rows[-1]["created_ts"] == Decimal(str(START + 179) + ".999999999")


def test_invalidated_trade_cursor_restarts_without_loss_or_duplicate_facts(tmp_path, specification):
    specification["feeds"] = ["trades"]
    exchange = Exchange()
    requests = []

    def fixture(request):
        if request.url.path.endswith("/historical/trades"):
            return httpx.Response(200, json={"trades": [], "cursor": ""})
        if request.url.path.endswith("/markets/trades"):
            cursor = request.url.params.get("cursor")
            requests.append(cursor)
            if cursor == "expired":
                return httpx.Response(410, json={"error": "invalid cursor"})
            if cursor == "next":
                return httpx.Response(200, json={"trades": [trade()], "cursor": ""})
            if len(requests) == 1:
                return httpx.Response(200, json={"trades": [trade()], "cursor": "expired"})
            return httpx.Response(
                200, json={"trades": [trade(trade_id="native-2")], "cursor": "next"}
            )
        return exchange(request)

    result = workflow.run(
        tmp_path / "root",
        specification,
        transport=httpx.MockTransport(fixture),
        sleep=lambda _: None,
    )
    release = tmp_path / "root" / "published" / result["release_id"]
    assert requests == [None, "expired", None, "next"]
    assert {row["trade_id"] for row in pq.read_table(release / "trades.parquet").to_pylist()} == {
        "native-1",
        "native-2",
    }
    assert (
        workflow.replay(tmp_path / "root", result["acquisition_id"])["release_id"]
        == result["release_id"]
    )


def test_repeated_trade_cursor_never_certifies_partial_rows(tmp_path, specification):
    specification["feeds"] = ["trades"]
    exchange = Exchange()

    def fixture(request):
        if request.url.path.endswith("/trades"):
            return httpx.Response(200, json={"trades": [trade()], "cursor": "same"})
        return exchange(request)

    with pytest.raises(ValueError, match="repeated source cursor"):
        workflow.run(
            tmp_path / "root",
            specification,
            transport=httpx.MockTransport(fixture),
            sleep=lambda _: None,
        )
    journal = read_json(next((tmp_path / "root" / ".state" / "runs").glob("*.json")))
    assert not list((tmp_path / "root" / "published").glob("*.current.json"))
    with pytest.raises(FileNotFoundError):
        workflow.replay(tmp_path / "root", journal["acquisition_id"])


def test_trade_archive_cutoff_movement_is_reconciled_before_publication(tmp_path, specification):
    specification["feeds"] = ["trades"]
    cutoffs = [START + 90, START + 150]
    archive_ends = []

    def fixture(request):
        path = request.url.path
        if path.endswith("/cutoff"):
            return httpx.Response(
                200, json={"market_settled_ts": START, "trades_created_ts": cutoffs.pop(0)}
            )
        if path.endswith("/historical/trades"):
            end = int(request.url.params["max_ts"])
            archive_ends.append(end)
            rows = [trade(created_time="2020-01-01T00:02:00Z")] if end > START + 120 else []
            return httpx.Response(200, json={"trades": rows, "cursor": ""})
        if path.endswith("/markets/trades"):
            return httpx.Response(200, json={"trades": [], "cursor": ""})
        raise AssertionError(path)

    result = workflow.run(
        tmp_path / "root",
        specification,
        transport=httpx.MockTransport(fixture),
        sleep=lambda _: None,
    )
    rows = pq.read_table(
        tmp_path / "root" / "published" / result["release_id"] / "trades.parquet"
    ).to_pylist()
    assert archive_ends == [START + 91, START + 151]
    assert len(rows) == 1 and rows[0]["created_ts"] == Decimal(START + 120)


def test_candle_chunk_seam_and_exclusive_end_have_no_loss_or_multiplication(
    tmp_path, specification
):
    specification["feeds"], specification["periods"] = ["candles"], [1]
    seam, end = START + 499 * 60, START + 501 * 60
    specification["end"] = "2020-01-01T08:21:00+00:00"
    boundaries = [START, seam - 60, seam, end - 60, end]
    requests = []
    exchange = Exchange()

    def fixture(request):
        if request.url.path.endswith("/candlesticks"):
            if "/historical/" in request.url.path:
                return httpx.Response(404, json={"error": "not archived"})
            lower, upper = int(request.url.params["start_ts"]), int(request.url.params["end_ts"])
            requests.append((lower, upper))
            rows = [candle(end_period_ts=value) for value in boundaries if lower <= value <= upper]
            return httpx.Response(200, json={"ticker": MARKET, "candlesticks": rows})
        return exchange(request)

    result = workflow.run(
        tmp_path / "root",
        specification,
        transport=httpx.MockTransport(fixture),
        sleep=lambda _: None,
    )
    rows = pq.read_table(
        tmp_path / "root" / "published" / result["release_id"] / "candlesticks.parquet"
    ).to_pylist()
    assert requests == [(START, seam - 1), (seam, end - 1)]
    assert sorted(row["end_period_ts"] for row in rows) == boundaries[:-1]


@pytest.mark.parametrize("conflict", [False, True])
def test_duplicate_candle_within_capture_has_one_fact_or_fails(conflict):
    repeated = candle(volume_fp="0.50") if conflict else candle()
    payload = {"candlesticks": [candle(), repeated]}
    if conflict:
        with pytest.raises(ValueError, match="conflicting candle"):
            candles(payload, MARKET, 1, START, START + 180, "capture", "now")
    else:
        assert len(candles(payload, MARKET, 1, START, START + 180, "capture", "now")) == 1


def test_empty_book_publishes_header_coverage_and_native_lifecycle(tmp_path, specification):
    specification["feeds"] = ["books"]
    market = read_json(Path(specification["metadata"]) / "markets.json")[0] | {"status": "disputed"}
    directory, pin = metadata_at(tmp_path / "native", market=market)
    specification.update(metadata=str(directory), metadata_sha256=pin)
    exchange = Exchange()

    def fixture(request):
        if request.url.path.endswith("/orderbook"):
            return httpx.Response(200, json={"orderbook_fp": {"yes_dollars": [], "no_dollars": []}})
        return exchange(request)

    result = workflow.run(
        tmp_path / "root",
        specification,
        transport=httpx.MockTransport(fixture),
        sleep=lambda _: None,
    )
    release = tmp_path / "root" / "published" / result["release_id"]
    headers = pq.read_table(release / "book_snapshots.parquet").to_pylist()
    assert len(headers) == 1 and headers[0]["yes_levels"] == headers[0]["no_levels"] == 0
    assert len(pq.read_table(release / "book_levels.parquet")) == 0
    assert pq.read_table(release / "coverage.parquet").to_pylist()[0]["status"] == "complete"
    assert {
        row["status"] for row in pq.read_table(release / "instruments.parquet").to_pylist()
    } == {"disputed"}


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"orderbook_fp": None},
        {"orderbook_fp": {"yes_dollars": [], "no_dollars": None}},
        {"orderbook_fp": {"yes_dollars": [["0.1", "1"], ["0.1", "2"]], "no_dollars": []}},
    ],
)
def test_malformed_book_cannot_publish_empty_success(tmp_path, specification, body):
    specification["feeds"] = ["books"]
    exchange = Exchange()

    def fixture(request):
        return (
            httpx.Response(200, json=body)
            if request.url.path.endswith("/orderbook")
            else exchange(request)
        )

    with pytest.raises(ValueError, match="book"):
        workflow.run(
            tmp_path / "root",
            specification,
            transport=httpx.MockTransport(fixture),
            sleep=lambda _: None,
        )
    assert not list((tmp_path / "root" / "published").glob("*.current.json"))


def test_delegated_refresh_and_http_use_one_remaining_allowance(tmp_path, monkeypatch):
    store = metered(tmp_path, max_requests=3)
    root = tmp_path / "producer"
    root.mkdir()
    marker = root / "retained-raw"
    marker.write_bytes(b"evidence")
    grants, outputs = [], []

    def child(argv, **kwargs):
        assert kwargs["env"]["KALSHI_EVENTS_ROOT"] == str(root)
        assert Path(argv[argv.index("--root") + 1]) == root
        grants.append(int(argv[argv.index("--max-requests") + 1]))
        directory = Path(argv[argv.index("--output") + 1])
        outputs.append(directory)
        _, pin = metadata_at(directory)
        manifest = read_json(directory / "manifest.json")
        return subprocess.CompletedProcess(
            argv,
            0,
            json_bytes(
                {
                    "output": str(directory),
                    "root": str(root),
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
            producer.refresh_metadata(store, [MARKET], sys.executable, root)
        client = Client(
            store,
            transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b"{}")),
            sleep=lambda _: None,
        )
        try:
            client.get("/historical/cutoff", {})
        finally:
            client.close()
        with pytest.raises(BudgetExceeded, match="allowance"):
            producer.refresh_metadata(store, [MARKET], sys.executable, root)
    assert grants == [3, 2]
    assert store.run_meter.charged == (3, 36)
    assert len(set(outputs)) == 2 and not any(path.exists() for path in outputs)
    assert marker.read_bytes() == b"evidence"


@pytest.mark.parametrize("accounted", [False, True])
def test_failed_or_busy_producer_never_falls_back_or_refunds_unproven_usage(
    tmp_path, monkeypatch, accounted
):
    store = metered(tmp_path, max_requests=3)
    reply = {"requests": 1, "downloaded_bytes": 17, "accounting_complete": accounted}
    monkeypatch.setattr(
        producer,
        "bounded_run",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 1, json_bytes(reply)),
    )
    with (
        store.lock,
        pytest.raises(ValueError, match="failed or busy" if accounted else "accounting"),
    ):
        producer.refresh_metadata(store, [MARKET], sys.executable, tmp_path / "producer")
    if accounted:
        assert store.run_meter.charged == (1, 17)
    else:
        assert store.run_meter.remaining == (0, 0)
    assert list((store.root / "inputs").iterdir()) == []
    assert not list((store.root / ".state").glob(".metadata-*"))


def test_producer_timeout_reaps_child_before_return(tmp_path, monkeypatch):
    store = metered(tmp_path)
    directory = store.root / ".state" / "child-work"
    directory.mkdir()
    original, children = subprocess.Popen, []

    def launch(*args, **kwargs):
        child = original(*args, **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(producer.subprocess, "Popen", launch)
    with store.lock, pytest.raises(ValueError, match="deadline"):
        producer.bounded_run(
            [sys.executable, "-c", "import time;time.sleep(10)"],
            env=dict(os.environ),
            directory=directory,
            store=store,
            timeout=0.05,
        )
    assert len(children) == 1 and children[0].poll() is not None


def test_failed_publication_keeps_prior_pointer_and_resumes_without_acquisition(
    tmp_path, specification, monkeypatch
):
    first = execute(tmp_path, specification, Exchange())
    pointer = tmp_path / "root" / "published" / (first["plan_id"] + ".current.json")
    before = read_json(pointer)
    original = Store.publish

    def unavailable(*args, **kwargs):
        raise OSError("synthetic publication failure")

    monkeypatch.setattr(Store, "publish", unavailable)
    with pytest.raises(OSError, match="publication failure"):
        execute(tmp_path, specification, Exchange())
    assert read_json(pointer) == before
    journals = [read_json(path) for path in (tmp_path / "root" / ".state" / "runs").glob("*.json")]
    interrupted = next(row for row in journals if row["run_id"] != first["run_id"])
    assert interrupted["release_id"] != first["release_id"]
    monkeypatch.setattr(Store, "publish", original)
    resumed = workflow.run(
        tmp_path / "root",
        run_id=interrupted["run_id"],
        transport=httpx.MockTransport(lambda _: pytest.fail("publication recovery did HTTP")),
    )
    assert read_json(pointer)["release_id"] == resumed["release_id"] == interrupted["release_id"]
