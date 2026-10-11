"""Native facts, exact decimals and explicit empty-versus-invalid semantics."""

import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

import pyarrow as pa

from .foundation import json_bytes

DECIMAL = pa.decimal128(38, 18)
STRING = pa.string()
PRICE_FIELDS = ("open", "high", "low", "close", "mean", "previous", "min", "max")


def timestamp(value):
    if isinstance(value, bool):
        raise ValueError("invalid timestamp")
    if isinstance(value, int):
        return value
    if not isinstance(value, str):
        raise ValueError("UTC timestamp required")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]00:00)", value):
        raise ValueError("whole-second UTC timestamp required")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    exact = source_timestamp(value.replace(" ", "T", 1))
    if (
        parsed.tzinfo is None
        or parsed.utcoffset().total_seconds() != 0
        or exact != exact.to_integral_value()
    ):
        raise ValueError("whole-second UTC timestamp required")
    return int(exact)


def utc(value):
    return datetime.fromtimestamp(timestamp(value), UTC).isoformat()


def source_timestamp(value):
    if isinstance(value, int) and not isinstance(value, bool):
        return Decimal(value)
    if not isinstance(value, str):
        raise ValueError("source date-time required")
    match = re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.(\d+))?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)",
        value,
    )
    if match is None:
        raise ValueError("source RFC3339 date-time required")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("source timezone required")
    fraction = match.group(1) or ""
    if len(fraction.rstrip("0")) > 9:
        raise ValueError("source time exceeds nanosecond precision")
    base = parsed.astimezone(UTC).replace(microsecond=0) - datetime(1970, 1, 1, tzinfo=UTC)
    return Decimal(base.days * 86400 + base.seconds) + Decimal("0." + (fraction or "0"))


def exact(value, *, cents=False, price=False):
    if value is None:
        return None
    if isinstance(value, (bool, float)) or not isinstance(value, (str, int, Decimal)):
        raise ValueError("exact source decimal required")
    try:
        result = Decimal(value)
        if cents:
            result = result.scaleb(-2)
    except InvalidOperation:
        raise ValueError("invalid source decimal") from None
    if not result.is_finite() or result < 0 or (price and result > 1):
        raise ValueError("source decimal outside financial domain")
    if result.as_tuple().exponent < -18 or result.adjusted() > 19:
        raise ValueError("source decimal exceeds exact contract precision")
    return result


def field(row, name, *, price=False):
    modern = name + ("_dollars" if price else "_fp")
    if modern in row:
        return exact(row[modern], price=price)
    value = row.get(name)
    return exact(value, cents=price and not isinstance(value, str), price=price)


def objects(payload, name):
    if not isinstance(payload, dict) or not isinstance(payload.get(name), list):
        raise ValueError("missing source array: " + name)
    rows = payload[name]
    if any(not isinstance(row, dict) for row in rows):
        raise ValueError("malformed source array: " + name)
    return rows


def candles(payload, market, period, start, end, capture_id, received_at):
    if payload.get("ticker", market) != market:
        raise ValueError("candle market mismatch")
    results, seen = [], {}
    for row in objects(payload, "candlesticks"):
        stamp = row.get("end_period_ts")
        if isinstance(stamp, bool) or not isinstance(stamp, int):
            raise ValueError("candle end timestamp required")
        if not start <= stamp < end:
            continue
        result = {
            "capture_id": capture_id,
            "market_ticker": market,
            "period_interval": period,
            "end_period_ts": stamp,
            "received_at": received_at,
            "native_payload": json_bytes(row).decode(),
            "volume": field(row, "volume"),
            "open_interest": field(row, "open_interest"),
        }
        for kind in ("yes_bid", "yes_ask", "price"):
            source = row.get(kind)
            if source is not None and not isinstance(source, dict):
                raise ValueError("invalid candle price object")
            for component in PRICE_FIELDS:
                result[kind + "_" + component] = field(source or {}, component, price=True)
        if stamp in seen and seen[stamp] != result:
            raise ValueError("conflicting candle within capture")
        if stamp not in seen:
            results.append(result)
        seen[stamp] = result
    return results


def trades(payload, market, start, end):
    results = []
    for row in objects(payload, "trades"):
        if row.get("ticker") != market:
            raise ValueError("trade market mismatch")
        native = row.get("trade_id")
        if not isinstance(native, str) or not 1 <= len(native) <= 256:
            raise ValueError("trade identity required")
        created = row.get("created_time")
        if not isinstance(created, str):
            raise ValueError("trade created_time required")
        stamp = source_timestamp(created)
        if not start <= stamp < end:
            continue
        count = field(row, "count")
        yes, no = field(row, "yes_price", price=True), field(row, "no_price", price=True)
        if count is None or count <= 0 or yes is None or no is None or yes + no != 1:
            raise ValueError("invalid trade financial facts")
        side = row.get("taker_outcome_side", row.get("taker_side"))
        if side not in {"yes", "no"} or row.get("taker_book_side") not in {None, "bid", "ask"}:
            raise ValueError("invalid trade direction")
        block = row.get("is_block_trade")
        if block is not None and not isinstance(block, bool):
            raise ValueError("invalid block-trade flag")
        results.append(
            {
                "trade_id": native,
                "market_ticker": market,
                "created_ts": stamp,
                "created_time": created,
                "count": count,
                "yes_price": yes,
                "no_price": no,
                "taker_side": row.get("taker_side"),
                "taker_outcome_side": side,
                "taker_book_side": row.get("taker_book_side"),
                "is_block_trade": block,
                "native_payload": json_bytes(row).decode(),
            }
        )
    return results


def book(payload, market, capture_id, request_at, received_at, depth):
    modern = "orderbook_fp" in payload
    body = payload.get("orderbook_fp" if modern else "orderbook")
    if not isinstance(body, dict):
        raise ValueError("missing source book object")
    levels, counts = [], {}
    for side in ("yes", "no"):
        name = side + "_dollars" if modern else side
        rows = body.get(name)
        if not isinstance(rows, list):
            raise ValueError("missing source book side")
        seen = set()
        for ordinal, row in enumerate(rows):
            if not isinstance(row, list) or len(row) != 2:
                raise ValueError("malformed source book level")
            price, quantity = exact(row[0], cents=not modern, price=True), exact(row[1])
            if price is None or quantity is None or quantity <= 0 or price in seen:
                raise ValueError("invalid/duplicate book level")
            seen.add(price)
            levels.append(
                {
                    "capture_id": capture_id,
                    "market_ticker": market,
                    "outcome_side": side.upper(),
                    "book_side": "bid",
                    "level_index": ordinal,
                    "price": price,
                    "quantity": quantity,
                }
            )
        counts[side + "_levels"] = len(rows)
    header = {
        "capture_id": capture_id,
        "market_ticker": market,
        "requested_at": request_at,
        "received_at": received_at,
        "depth": depth,
        "native_payload": json_bytes(payload).decode(),
        **counts,
    }
    return [header], levels


SCHEMAS = {
    "instruments": pa.schema(
        [
            (name, STRING)
            for name in (
                "market_ticker",
                "event_ticker",
                "series_ticker",
                "outcome_side",
                "status",
                "settlement_ts",
                "observation_id",
                "received_at",
                "metadata_sha256",
                "source_release_id",
                "native_payload",
            )
        ]
    ),
    "candlesticks": pa.schema(
        [
            (name, STRING)
            for name in ("capture_id", "market_ticker", "received_at", "native_payload")
        ]
        + [("period_interval", pa.int32()), ("end_period_ts", pa.int64())]
        + [("volume", DECIMAL), ("open_interest", DECIMAL)]
        + [
            (kind + "_" + component, DECIMAL)
            for kind in ("yes_bid", "yes_ask", "price")
            for component in PRICE_FIELDS
        ]
    ),
    "trades": pa.schema(
        [
            (name, STRING)
            for name in (
                "trade_id",
                "market_ticker",
                "created_time",
                "taker_side",
                "taker_outcome_side",
                "taker_book_side",
                "native_payload",
            )
        ]
        + [
            ("created_ts", pa.decimal128(29, 9)),
            ("count", DECIMAL),
            ("yes_price", DECIMAL),
            ("no_price", DECIMAL),
            ("is_block_trade", pa.bool_()),
        ]
    ),
    "book_snapshots": pa.schema(
        [
            (name, STRING)
            for name in (
                "capture_id",
                "market_ticker",
                "requested_at",
                "received_at",
                "native_payload",
            )
        ]
        + [(name, pa.int32()) for name in ("depth", "yes_levels", "no_levels")]
    ),
    "book_levels": pa.schema(
        [(name, STRING) for name in ("capture_id", "market_ticker", "outcome_side", "book_side")]
        + [("level_index", pa.int32()), ("price", DECIMAL), ("quantity", DECIMAL)]
    ),
    "coverage": pa.schema(
        [
            (name, STRING)
            for name in (
                "market_ticker",
                "feed",
                "status",
                "detail",
                "observed_start",
                "observed_end",
            )
        ]
    ),
}
SCHEMAS["latest_candlesticks"] = SCHEMAS["candlesticks"]
