from decimal import Decimal

import httpx
import pyarrow.parquet as pq
import pytest
from conftest import MARKET, START
from filelock import FileLock, Timeout

from kalshi_market_data import CONTRACT, workflow
from kalshi_market_data.foundation import BudgetExceeded, Store, read_json
from kalshi_market_data.normalize import book, candles, exact, trades


def trade(**changes):
    return {
        "trade_id": "native-1",
        "ticker": MARKET,
        "created_time": "2020-01-01T00:01:00.123456Z",
        "count_fp": "0.25",
        "yes_price_dollars": "0.1234",
        "no_price_dollars": "0.8766",
        "taker_side": "yes",
        "taker_outcome_side": "yes",
        "taker_book_side": "ask",
        "is_block_trade": True,
        **changes,
    }


def candle(**changes):
    return {
        "end_period_ts": START + 60,
        "price": {
            "open_dollars": "0.1234",
            "close_dollars": None,
            "mean_dollars": "0.12005",
            "previous_dollars": "0.1001",
        },
        "yes_bid": {"low_dollars": "0.1001"},
        "yes_ask": None,
        "volume_fp": "0.25",
        "open_interest_fp": "1.50",
        **changes,
    }


class Exchange:
    def __init__(self, *, conflict=False, both_candles=False, malformed_book=False):
        self.calls = []
        self.conflict, self.both_candles, self.malformed_book = (
            conflict,
            both_candles,
            malformed_book,
        )

    def __call__(self, request):
        path = request.url.path
        self.calls.append((path, dict(request.url.params)))
        if path.endswith("/cutoff"):
            return httpx.Response(
                200, json={"market_settled_ts": START, "trades_created_ts": START + 90}
            )
        if path.endswith("/candlesticks"):
            if "/historical/" in path and not self.both_candles:
                return httpx.Response(404, json={"error": "not archived"})
            row = candle()
            if "/historical/" in path:
                row = candle(volume_fp="0.50")
            return httpx.Response(
                200,
                json={"ticker": MARKET, "candlesticks": [row, candle(end_period_ts=START + 180)]},
            )
        if path.endswith("/trades"):
            if request.url.params.get("cursor"):
                return httpx.Response(200, json={"trades": [trade()], "cursor": ""})
            row = trade(count_fp="0.50") if self.conflict and "/historical/" in path else trade()
            return httpx.Response(200, json={"trades": [row], "cursor": "next"})
        if path.endswith("/orderbook"):
            body = {"yes_dollars": [["0.1234", "0.25"]], "no_dollars": []}
            if self.malformed_book:
                body.pop("no_dollars")
            return httpx.Response(200, json={"orderbook_fp": body})
        raise AssertionError(path)


def execute(tmp_path, specification, exchange):
    return workflow.run(
        tmp_path / "root",
        specification,
        transport=httpx.MockTransport(exchange),
        sleep=lambda _: None,
    )


def test_capture_publish_precision_boundaries_native_grain_and_replay(tmp_path, specification):
    exchange = Exchange(both_candles=True)
    result = execute(tmp_path, specification, exchange)
    release = tmp_path / "root" / "published" / result["release_id"]
    rows = pq.read_table(release / "trades.parquet").to_pylist()
    assert len(rows) == 1 and rows[0]["count"] == Decimal("0.25")
    assert rows[0]["yes_price"] == Decimal("0.1234")
    assert rows[0]["created_ts"] == Decimal(str(START + 60) + ".123456")
    assert rows[0]["taker_book_side"] == "ask" and rows[0]["is_block_trade"] is True
    candle_rows = pq.read_table(release / "candlesticks.parquet").to_pylist()
    assert len(candle_rows) == 2
    assert {row["volume"] for row in candle_rows} == {Decimal("0.25"), Decimal("0.50")}
    assert all(row["price_close"] is None and row["yes_ask_open"] is None for row in candle_rows)
    latest = pq.read_table(release / "latest_candlesticks.parquet").to_pylist()
    assert len(latest) == 1 and latest[0]["volume"] == Decimal("0.50")
    assert len(pq.read_table(release / "instruments.parquet")) == 2
    assert len(pq.read_table(release / "book_snapshots.parquet")) == 1
    assert len(pq.read_table(release / "book_levels.parquet")) == 1
    assert len(pq.read_table(release / "coverage.parquet")) == 3
    assert (
        workflow.replay(tmp_path / "root", result["acquisition_id"])["release_id"]
        == result["release_id"]
    )
    assert (
        workflow.replay(tmp_path / "root", result["acquisition_id"])["release_id"]
        == result["release_id"]
    )
    assert (
        workflow.run(
            tmp_path / "root",
            run_id=result["run_id"],
            transport=httpx.MockTransport(lambda _: pytest.fail("complete resume did IO")),
        )["release_id"]
        == result["release_id"]
    )
    assert all(
        params["include_latest_before_start"] == "false"
        for path, params in exchange.calls
        if path.endswith("/candlesticks")
    )


def test_conflicting_trades_and_malformed_books_refuse_promotion(tmp_path, specification):
    with pytest.raises(ValueError, match="conflicting duplicate"):
        execute(tmp_path, specification, Exchange(conflict=True))
    assert not list((tmp_path / "root" / "published").glob("*.current.json"))
    other = tmp_path / "other"
    with pytest.raises(ValueError, match="missing source book side"):
        workflow.run(
            other,
            specification,
            transport=httpx.MockTransport(Exchange(malformed_book=True)),
            sleep=lambda _: None,
        )
    assert not list((other / "published").glob("*.current.json"))


def test_crash_after_raw_commit_resume_skips_acquisition_request(
    tmp_path, specification, monkeypatch
):
    original = Store.atomic_write
    failed = []

    def interrupted(self, path, data):
        if path.name == "checkpoint.json" and not failed:
            failed.append(True)
            raise OSError("crash after raw commit")
        return original(self, path, data)

    exchange = Exchange()
    monkeypatch.setattr(Store, "atomic_write", interrupted)
    with pytest.raises(OSError, match="raw commit"):
        execute(tmp_path, specification, exchange)
    journal = read_json(next((tmp_path / "root" / ".state" / "runs").glob("*.json")))
    assert len(exchange.calls) == 1
    assert len(Store(tmp_path / "root", CONTRACT).committed_captures()) == 1
    result = workflow.run(
        tmp_path / "root",
        run_id=journal["run_id"],
        transport=httpx.MockTransport(exchange),
        sleep=lambda _: None,
    )
    assert result["stage"] == "complete"
    assert len([path for path, _ in exchange.calls if path.endswith("cutoff")]) == 2


def test_unfinished_budget_cannot_build_or_reset_run_caps(tmp_path, specification):
    specification["limits"]["max_requests"] = 1
    with pytest.raises(BudgetExceeded):
        execute(tmp_path, specification, Exchange())
    journal = read_json(next((tmp_path / "root" / ".state" / "runs").glob("*.json")))
    with pytest.raises(FileNotFoundError):
        workflow.replay(tmp_path / "root", journal["acquisition_id"])
    with pytest.raises(BudgetExceeded):
        workflow.run(
            tmp_path / "root",
            run_id=journal["run_id"],
            transport=httpx.MockTransport(Exchange()),
            sleep=lambda _: None,
        )


def test_root_busy_and_settings_integrity(tmp_path, specification):
    result = execute(tmp_path, specification, Exchange())
    with FileLock(tmp_path / "root" / ".writer.lock", timeout=0):
        with pytest.raises(Timeout):
            workflow.run(tmp_path / "root", run_id=result["run_id"])
    path = tmp_path / "root" / ".state" / "runs" / (result["run_id"] + ".json")
    data = read_json(path)
    data["settings"]["depth"] = 1
    from kalshi_market_data.foundation import json_bytes

    path.write_bytes(json_bytes(data))
    with pytest.raises(ValueError, match="settings"):
        workflow.run(tmp_path / "root", run_id=result["run_id"])


@pytest.mark.parametrize("period", [1, 60, 1440])
def test_candles_legacy_precision_and_inclusive_boundary(period):
    rows = candles(
        {
            "candlesticks": [
                candle(price={"open": 12, "mean": Decimal("12.345")}, volume="0.125"),
                candle(end_period_ts=START + 180),
            ]
        },
        MARKET,
        period,
        START,
        START + 180,
        "capture",
        "now",
    )
    assert len(rows) == 1 and rows[0]["price_mean"] == Decimal("0.12345")
    assert rows[0]["volume"] == Decimal("0.25")  # preferred fixed-point source wins


def test_empty_book_header_malformed_distinct_and_decimal_domain():
    header, levels = book(
        {"orderbook_fp": {"yes_dollars": [], "no_dollars": []}},
        MARKET,
        "capture",
        "requested",
        "received",
        0,
    )
    assert len(header) == 1 and header[0]["yes_levels"] == 0 and levels == []
    with pytest.raises(ValueError):
        book(
            {"orderbook_fp": {"yes_dollars": None, "no_dollars": []}},
            MARKET,
            "capture",
            "requested",
            "received",
            0,
        )
    for value in ("NaN", "-1", "1.0000000000000000001", True, 0.1234):
        with pytest.raises(ValueError):
            exact(value, price=True)
    with pytest.raises(ValueError, match="missing"):
        trades({}, MARKET, START, START + 180)


def test_archive_dollar_strings_and_large_quantity_are_exact():
    rows = candles(
        {
            "candlesticks": [
                candle(
                    price={"open": "0.1234", "close": "0.5123", "mean": "0.50005", "previous": None}
                )
            ]
        },
        MARKET,
        60,
        START,
        START + 180,
        "capture",
        "now",
    )
    assert rows[0]["price_close"] == Decimal("0.5123")
    assert rows[0]["price_mean"] == Decimal("0.50005")
    value = "1000000000000.123456789012345678"
    assert exact(value) == Decimal(value)


def test_fractional_source_routing_dates_and_native_trade_nanoseconds(tmp_path, specification):
    from kalshi_market_data.foundation import digest, json_bytes, read_json
    from kalshi_market_data.normalize import source_timestamp

    metadata = tmp_path / "metadata"
    rows = read_json(metadata / "markets.json")
    rows[0]["settlement_ts"] = "2019-12-31T23:59:59.123456Z"
    raw = json_bytes(rows)
    (metadata / "markets.json").write_bytes(raw)
    manifest = read_json(metadata / "manifest.json")
    manifest["files"]["markets.json"] = {"sha256": digest(raw), "size": len(raw)}
    manifest["source_release_id"] = digest(json_bytes(manifest["files"]))
    raw = json_bytes(manifest)
    (metadata / "manifest.json").write_bytes(raw)
    specification["metadata_sha256"] = digest(raw)
    exchange = Exchange(both_candles=True)
    original = exchange.__call__

    def fixture(request):
        if request.url.path.endswith("cutoff"):
            return httpx.Response(
                200,
                json={
                    "market_settled_ts": "2020-01-01T00:00:00.123456Z",
                    "trades_created_ts": "2020-01-01T00:01:30.123456Z",
                },
            )
        return original(request)

    result = workflow.run(
        tmp_path / "root",
        specification,
        transport=httpx.MockTransport(fixture),
        sleep=lambda _: None,
    )
    assert result["stage"] == "complete"
    assert exchange.calls[0][0].endswith("/historical/markets/" + MARKET + "/candlesticks")
    assert source_timestamp("2020-01-01T01:00:00.123456789+01:00") == Decimal(
        str(START) + ".123456789"
    )
    rows = trades(
        {"trades": [trade(created_time="2020-01-01T00:01:00.123456789Z")]},
        MARKET,
        START,
        START + 180,
    )
    assert rows[0]["created_ts"] == Decimal(str(START + 60) + ".123456789")


@pytest.mark.parametrize("shifted", [False, True])
def test_duplicate_trade_uses_exact_instant_and_known_native_facts(
    tmp_path, specification, shifted
):
    exchange = Exchange()

    def fixture(request):
        response = exchange(request)
        if "/historical/trades" in request.url.path:
            payload = response.json()
            for row in payload["trades"]:
                row["created_time"] = (
                    "2020-01-01T00:01:01.123456000+00:00"
                    if shifted
                    else "2020-01-01T00:01:00.123456000+00:00"
                )
                row.pop("taker_side")
                row.pop("is_block_trade")
            return httpx.Response(200, json=payload)
        return response

    if shifted:
        with pytest.raises(ValueError, match="conflicting duplicate"):
            workflow.run(
                tmp_path / "root",
                specification,
                transport=httpx.MockTransport(fixture),
                sleep=lambda _: None,
            )
    else:
        result = workflow.run(
            tmp_path / "root",
            specification,
            transport=httpx.MockTransport(fixture),
            sleep=lambda _: None,
        )
        rows = pq.read_table(
            tmp_path / "root" / "published" / result["release_id"] / "trades.parquet"
        ).to_pylist()
        assert len(rows) == 1 and rows[0]["is_block_trade"] is True
