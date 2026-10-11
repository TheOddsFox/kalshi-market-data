from decimal import Decimal

import pytest
from conftest import MARKET, START
from test_workflow import trade

from kalshi_market_data.normalize import source_timestamp, timestamp, trades


@pytest.mark.parametrize(
    "value,expected",
    [
        ("2020-01-01T00:00:00Z", Decimal(START)),
        ("2020-01-01T00:00:00.000000001Z", Decimal("1577836800.000000001")),
        ("2020-01-01T00:00:00.999999999Z", Decimal("1577836800.999999999")),
        ("2020-01-01T00:00:00.123456789000Z", Decimal("1577836800.123456789")),
        ("2020-01-01T01:30:00.123456789+01:30", Decimal("1577836800.123456789")),
        ("2019-12-31T23:00:00.123456789-01:00", Decimal("1577836800.123456789")),
        (START, Decimal(START)),
        (0, Decimal(0)),
    ],
)
def test_source_timestamp_preserves_exact_instant_and_integer_cutoffs(value, expected):
    assert source_timestamp(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "20200101T000000.000000001Z",
        "20200101T00:00:00.000000001Z",
        "2020-01-01T000000.000000001Z",
        "2020-01-01T00:00Z",
        "2020-01-01T00:00:00+00:00:00.000000001",
        "2020-01-01T00:00:00+00:00:01",
        "2020-01-01T00:00:00+0000",
        "2020-01-01T00:00:00+00:60",
        "2020-01-01T00:00:00+01:99",
        "2020-01-01T00:00:00.1234567891Z",
        "2020-01-01T00:00:00.123456789",
        "2020-02-30T00:00:00Z",
        "",
        None,
        True,
        1577836800.0,
    ],
)
def test_malformed_source_timestamp_is_rejected_instead_of_normalized(value):
    with pytest.raises(ValueError):
        source_timestamp(value)


@pytest.mark.parametrize(
    "created_time",
    [
        "20200101T000100.000000001Z",
        "2020-01-01T00:01Z",
        "2020-01-01T00:01:00+00:00:00.000000001",
        "2020-01-01T00:01:00+00:60",
    ],
)
def test_trade_with_malformed_creation_time_cannot_emit_a_fact(created_time):
    with pytest.raises(ValueError):
        trades({"trades": [trade(created_time=created_time)]}, MARKET, START, START + 180)


def test_trade_creation_time_keeps_native_string_and_exact_offset_equivalent_nanoseconds():
    created = "2019-12-31T23:01:00.000000001-01:00"
    rows = trades({"trades": [trade(created_time=created)]}, MARKET, START, START + 180)
    assert len(rows) == 1
    assert rows[0]["created_time"] == created
    assert rows[0]["created_ts"] == Decimal("1577836860.000000001")


@pytest.mark.parametrize(
    "value",
    ["2020-01-01 00:00:00+00:00", "2020-01-01 00:00:00.000000000Z"],
)
def test_cli_whole_second_utc_window_keeps_space_separator_compatibility(value):
    assert timestamp(value) == START
