"""Finite acquisition, resumable raw pages, verified deterministic replay."""

from __future__ import annotations

import base64
import math
import time
import uuid
from pathlib import Path

from . import CONTRACT
from .budget import RunMeter
from .foundation import (
    BudgetExceeded,
    Limits,
    Store,
    digest,
    json_bytes,
    json_loads,
    now,
    read_json,
)
from .metadata import load_metadata, pin_metadata, select_markets, ticker
from .normalize import SCHEMAS, book, candles, source_timestamp, timestamp, trades, utc
from .producer import canonical_root, refresh_metadata
from .transport import Client, api_origin

DEFAULT_LIMITS = Limits(1000, 4 * 1024**3, 16 * 1024**3, 16 * 1024**2, 2 * 1024**3)
FEEDS = {"candles", "trades", "books"}


def settings(args):
    selected = sorted(set(map(ticker, args.market_ticker or [])))
    if not 1 <= len(selected) <= 100 or not args.feed or set(args.feed) - FEEDS:
        raise ValueError("select 1..100 explicit market tickers and candles/trades/books feeds")
    start, end = timestamp(args.start), timestamp(args.end)
    if not start < end:
        raise ValueError("nonempty UTC [start,end) required")
    periods = sorted(set(args.period_interval or [60]))
    if set(periods) - {1, 60, 1440} or not 0 <= args.depth <= 100:
        raise ValueError("period 1/60/1440 and depth 0..100 required")
    if not 1 <= args.max_duration_seconds <= 86400:
        raise ValueError("duration must be 1..86400 seconds")
    pinned = bool(args.metadata and args.metadata_sha256)
    delegated = bool(args.events_executable and args.events_root)
    if pinned == delegated or (bool(args.metadata) != bool(args.metadata_sha256)):
        raise ValueError("choose pinned metadata or explicit producer executable/root")
    if pinned and (args.events_executable or args.events_root):
        raise ValueError("metadata modes cannot be combined")
    limits = Limits(**{name: getattr(args, name) for name in DEFAULT_LIMITS.as_dict()})
    spec = {
        "market_tickers": selected,
        "feeds": sorted(set(args.feed)),
        "start": utc(start),
        "end": utc(end),
        "periods": periods,
        "depth": args.depth,
        "max_duration_seconds": args.max_duration_seconds,
        "limits": limits.as_dict(),
        "api_url": api_origin(),
        "metadata": str(Path(args.metadata).absolute()) if pinned else None,
        "metadata_sha256": args.metadata_sha256 if pinned else None,
        "events_executable": str(Path(args.events_executable).resolve(strict=True))
        if delegated
        else None,
        "events_root": str(canonical_root(args.events_root)) if delegated else None,
    }
    return spec


class Journal:
    def __init__(self, store, spec=None, run_id=None):
        self.store = store
        self.run_id = store._name(run_id) if run_id else uuid.uuid4().hex
        self.path = store.root / ".state" / "runs" / (self.run_id + ".json")
        if run_id:
            self.data = read_json(self.path)
            if (
                self.data.get("run_id") != run_id
                or self.data.get("contract") != CONTRACT + ".run"
                or self.data.get("settings_sha256") != digest(json_bytes(self.data["settings"]))
            ):
                raise ValueError("run identity/settings mismatch")
        else:
            self.data = {
                "contract": CONTRACT + ".run",
                "run_id": self.run_id,
                "settings": spec,
                "settings_sha256": digest(json_bytes(spec)),
                "stage": "metadata",
                "plan_id": None,
                "acquisition_id": None,
                "metadata_sha256": None,
                "release_id": None,
                "usage": {},
                "error": None,
                "created_at": now(),
            }
        store.limits = Limits.from_dict(self.data["settings"]["limits"])
        self.meter = RunMeter(store.limits, self.data["usage"], self.write)
        store.run_meter = self.meter
        if not run_id:
            self.write()

    def write(self):
        self.data["updated_at"] = now()
        encoded = json_bytes(self.data)
        if len(encoded) > self.store.limits.max_temp_bytes:
            raise BudgetExceeded("journal temporary budget exhausted")
        self.store.atomic_write(self.path, encoded)


def raw_request(store, acquisition_id, client, cache, unit, endpoint, params, context):
    if len(unit) > 180:
        unit = digest(unit.encode())
    if unit in cache:
        capture = cache[unit]
        evidence = store.read_evidence(capture)
        if evidence["endpoint"] != endpoint or evidence["params"] != params:
            raise ValueError("resume request identity drift")
    else:
        result = client.get(endpoint, params)
        raw = result.pop("raw")
        evidence = {**result, **context, "raw_base64": base64.b64encode(raw).decode()}
        capture = store.capture(acquisition_id, unit, {}, evidence=evidence)
        cache[unit] = capture
        # The immutable raw capture commits before any mutable cursor checkpoint.
        store.atomic_write(
            store.root / "acquisitions" / acquisition_id / "checkpoint.json",
            json_bytes({"last_unit": unit, "capture_id": capture["capture_id"]}),
        )
    if evidence["status"] != 200:
        return capture, evidence, None
    payload = json_loads(base64.b64decode(evidence["raw_base64"], validate=True))
    if not isinstance(payload, dict):
        raise ValueError("source JSON object required")
    return capture, evidence, payload


def cutoff(store, acquisition_id, client, cache, unit):
    capture, evidence, payload = raw_request(
        store, acquisition_id, client, cache, unit, "/historical/cutoff", {}, {"feed": "cutoff"}
    )
    if payload is None:
        raise ValueError("historical cutoff unavailable")
    return {
        "market_settled_ts": source_timestamp(payload.get("market_settled_ts")),
        "trades_created_ts": source_timestamp(payload.get("trades_created_ts")),
    }


def paginated_trades(store, acquisition_id, client, cache, market, endpoint, start, end, label):
    cursor, seen, page, generation = "", set(), 0, 0
    while True:
        params = {"ticker": market, "min_ts": start - 1, "max_ts": end, "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        unit = f"trades-{market}-{label}-{generation}-{page}"
        capture, evidence, payload = raw_request(
            store,
            acquisition_id,
            client,
            cache,
            unit,
            endpoint,
            params,
            {"feed": "trades", "market_ticker": market, "start": start, "end": end},
        )
        if payload is None:
            if cursor and evidence["status"] in (400, 410) and generation < 2:
                generation += 1
                cursor, seen, page = "", set(), 0
                continue
            raise ValueError("trade traversal failed")
        trades(payload, market, start, end)
        next_cursor = payload.get("cursor")
        if not isinstance(next_cursor, str) or len(next_cursor) > 4096:
            raise ValueError("missing or malformed source cursor")
        if not next_cursor:
            return
        if next_cursor in seen:
            raise ValueError("repeated source cursor")
        seen.add(next_cursor)
        cursor, page = next_cursor, page + 1


def acquire(store, journal, client):
    spec = journal.data["settings"]
    acquisition_id = journal.data["acquisition_id"]
    captures = store.committed_captures(acquisition_id=acquisition_id)
    cache = {row["unit_id"]: row for row in captures}
    bundle = load_metadata(
        store.root / "inputs" / journal.data["metadata_sha256"], journal.data["metadata_sha256"]
    )
    markets = select_markets(bundle, spec["market_tickers"])
    start, end = timestamp(spec["start"]), timestamp(spec["end"])
    routing = cutoff(store, acquisition_id, client, cache, "cutoff-initial")
    observed_start = min((row["created_at"] for row in captures), default=now())
    for market in markets:
        native = market["market_ticker"]
        if "candles" in spec["feeds"]:
            settled = market.get("settlement_ts")
            historical = (
                settled is not None and source_timestamp(settled) < routing["market_settled_ts"]
            )
            primary = (
                f"/historical/markets/{native}/candlesticks"
                if historical
                else f"/series/{market['series_ticker']}/markets/{native}/candlesticks"
            )
            alternate = (
                f"/series/{market['series_ticker']}/markets/{native}/candlesticks"
                if historical
                else f"/historical/markets/{native}/candlesticks"
            )
            for period in spec["periods"]:
                lower = start
                while lower < end:
                    upper = min(end, lower + 499 * period * 60)
                    params = {
                        "start_ts": lower,
                        "end_ts": upper - 1,
                        "period_interval": period,
                        "include_latest_before_start": "false",
                    }
                    valid = False
                    # Reconcile the two tiers because an observed market may archive during a run.
                    for tier, endpoint in enumerate((primary, alternate)):
                        unit = f"candles-{native}-{period}-{lower}-{tier}"
                        capture, evidence, payload = raw_request(
                            store,
                            acquisition_id,
                            client,
                            cache,
                            unit,
                            endpoint,
                            params,
                            {
                                "feed": "candles",
                                "market_ticker": native,
                                "period_interval": period,
                                "start": lower,
                                "end": upper,
                            },
                        )
                        if payload is None:
                            if evidence["status"] == 404:
                                continue
                            raise ValueError("candle acquisition failed")
                        candles(
                            payload,
                            native,
                            period,
                            lower,
                            upper,
                            capture["capture_id"],
                            evidence["received_at"],
                        )
                        valid = True
                    if not valid:
                        raise ValueError("market candles absent from both tiers")
                    lower = upper
        if "trades" in spec["feeds"]:
            # Read live first, then archive, so a row moving tiers remains recoverable.
            paginated_trades(
                store, acquisition_id, client, cache, native, "/markets/trades", start, end, "live"
            )
            archive_end = min(end, math.ceil(routing["trades_created_ts"]) + 1)
            if archive_end > start:
                paginated_trades(
                    store,
                    acquisition_id,
                    client,
                    cache,
                    native,
                    "/historical/trades",
                    start,
                    archive_end,
                    "archive",
                )
        if "books" in spec["feeds"]:
            capture, evidence, payload = raw_request(
                store,
                acquisition_id,
                client,
                cache,
                f"books-{native}",
                f"/markets/{native}/orderbook",
                {"depth": spec["depth"]},
                {"feed": "books", "market_ticker": native},
            )
            if payload is None:
                raise ValueError("book snapshot acquisition failed")
            book(
                payload,
                native,
                capture["capture_id"],
                evidence["requested_at"],
                evidence["received_at"],
                spec["depth"],
            )
    final = cutoff(store, acquisition_id, client, cache, "cutoff-final-" + uuid.uuid4().hex)
    if "trades" in spec["feeds"] and final["trades_created_ts"] > routing["trades_created_ts"]:
        for native in spec["market_tickers"]:
            archive_end = min(end, math.ceil(final["trades_created_ts"]) + 1)
            if archive_end > start:
                paginated_trades(
                    store,
                    acquisition_id,
                    client,
                    cache,
                    native,
                    "/historical/trades",
                    start,
                    archive_end,
                    "reconcile-" + str(archive_end),
                )
    coverage = [
        {
            "market_ticker": native,
            "feed": feed,
            "status": "complete",
            "detail": "REST observation" if feed == "books" else "bounded traversal",
            "observed_start": observed_start,
            "observed_end": now(),
        }
        for native in spec["market_tickers"]
        for feed in spec["feeds"]
    ]
    committed = store.committed_captures(acquisition_id=acquisition_id)
    complete = {
        "plan_id": journal.data["plan_id"],
        "acquisition_id": acquisition_id,
        "capture_ids": sorted(row["capture_id"] for row in committed),
        "coverage": coverage,
        "routing": routing,
        "routing_final": final,
    }
    store.atomic_write(
        store.root / "acquisitions" / acquisition_id / "complete.json", json_bytes(complete)
    )
    return complete


def build(store, acquisition_id):
    acquisition = store.acquisition(acquisition_id)
    plan = store.load_plan(acquisition["plan_id"])
    complete = read_json(store.root / "acquisitions" / acquisition_id / "complete.json")
    captures = store.committed_captures(acquisition_id=acquisition_id)
    if (
        complete["plan_id"] != acquisition["plan_id"]
        or complete["acquisition_id"] != acquisition_id
        or complete["capture_ids"] != sorted(row["capture_id"] for row in captures)
    ):
        raise ValueError("acquisition incomplete or completion inventory drift")
    expected = {(native, feed) for native in plan["market_tickers"] for feed in plan["feeds"]}
    if {(row["market_ticker"], row["feed"]) for row in complete["coverage"]} != expected:
        raise ValueError("requested acquisition coverage incomplete")
    bundle = load_metadata(store.root / "inputs" / plan["metadata_sha256"], plan["metadata_sha256"])
    relations = {name: [] for name in SCHEMAS}
    for market in select_markets(bundle, plan["market_tickers"]):
        for side in ("YES", "NO"):
            relations["instruments"].append(
                {
                    **{
                        key: market.get(key)
                        for key in (
                            "market_ticker",
                            "event_ticker",
                            "series_ticker",
                            "status",
                            "observation_id",
                            "received_at",
                        )
                    },
                    "settlement_ts": str(market["settlement_ts"])
                    if market.get("settlement_ts") is not None
                    else None,
                    "outcome_side": side,
                    "metadata_sha256": plan["metadata_sha256"],
                    "source_release_id": bundle.manifest["source_release_id"],
                    "native_payload": json_bytes(market).decode(),
                }
            )
    unique_trades = {}
    for capture in sorted(captures, key=lambda row: (row["created_at"], row["capture_id"])):
        evidence = store.read_evidence(capture)
        if evidence["status"] != 200 or evidence["feed"] == "cutoff":
            continue
        payload = json_loads(base64.b64decode(evidence["raw_base64"], validate=True))
        native, feed = evidence["market_ticker"], evidence["feed"]
        if native not in plan["market_tickers"] or feed not in plan["feeds"]:
            raise ValueError("capture outside sealed plan")
        if feed == "candles":
            relations["candlesticks"].extend(
                candles(
                    payload,
                    native,
                    evidence["period_interval"],
                    evidence["start"],
                    evidence["end"],
                    capture["capture_id"],
                    evidence["received_at"],
                )
            )
        elif feed == "trades":
            for row in trades(payload, native, timestamp(plan["start"]), timestamp(plan["end"])):
                key = (native, row["trade_id"])
                fact = {
                    key: value
                    for key, value in row.items()
                    if key not in {"native_payload", "created_time"}
                }
                if key in unique_trades:
                    previous, retained = unique_trades[key]
                    for name, value in fact.items():
                        if (
                            previous[name] is not None
                            and value is not None
                            and previous[name] != value
                        ):
                            raise ValueError("conflicting duplicate native trade")
                        if previous[name] is None and value is not None:
                            previous[name] = retained[name] = value
                else:
                    unique_trades[key] = (fact, row)
        else:
            headers, levels = book(
                payload,
                native,
                capture["capture_id"],
                evidence["requested_at"],
                evidence["received_at"],
                plan["depth"],
            )
            relations["book_snapshots"].extend(headers)
            relations["book_levels"].extend(levels)
    relations["trades"] = [unique_trades[key][1] for key in sorted(unique_trades)]
    latest = {}
    for row in sorted(
        relations["candlesticks"], key=lambda row: (row["received_at"], row["capture_id"])
    ):
        latest[(row["market_ticker"], row["period_interval"], row["end_period_ts"])] = row
    relations["latest_candlesticks"] = [latest[key] for key in sorted(latest)]
    relations["coverage"] = complete["coverage"]
    return store.build_release(
        acquisition["plan_id"], captures, relations, complete["coverage"], schemas=SCHEMAS
    )


def run(root, spec=None, run_id=None, *, transport=None, sleep=None, publish=True):
    started = time.monotonic()
    if spec is not None:
        spec = {**spec, "publish": publish}
    store = Store(root, CONTRACT, Limits.from_dict(spec["limits"]) if spec else None)
    with store.lock:
        journal = Journal(store, spec, run_id)
        spec = journal.data["settings"]
        deadline = started + spec["max_duration_seconds"]
        if journal.data["stage"] == "complete":
            store.verify(journal.data["release_id"])
            if spec["publish"]:
                pointer = store.root / "published" / (journal.data["plan_id"] + ".current.json")
                if (
                    not pointer.exists()
                    or read_json(pointer)["release_id"] != journal.data["release_id"]
                ):
                    store.publish(journal.data["release_id"])
            return journal.data
        if spec["api_url"] != api_origin():
            raise ValueError("resume source configuration changed")
        try:
            if journal.data["plan_id"] is None:
                bundle = (
                    pin_metadata(store.root, spec["metadata"], spec["metadata_sha256"], store=store)
                    if spec["metadata"]
                    else refresh_metadata(
                        store,
                        spec["market_tickers"],
                        spec["events_executable"],
                        spec["events_root"],
                        timeout=max(0.001, deadline - time.monotonic()),
                    )
                )
                select_markets(bundle, spec["market_tickers"])
                if hasattr(store, "producer_report"):
                    journal.data["producer_refresh"] = store.producer_report
                pin = digest((bundle.path / "manifest.json").read_bytes())
                journal.data["metadata_sha256"] = pin
                journal.data["plan_id"] = store.create_plan({**spec, "metadata_sha256": pin})
                journal.data["acquisition_id"] = store.new_acquisition(
                    journal.data["plan_id"], "kalshi"
                )
                journal.data["stage"] = "acquire"
                journal.write()
            complete_path = (
                store.root / "acquisitions" / journal.data["acquisition_id"] / "complete.json"
            )
            if not complete_path.exists():
                if time.monotonic() >= deadline:
                    raise BudgetExceeded("invocation deadline exhausted")
                kwargs = {"sleep": sleep} if sleep is not None else {}
                client = Client(
                    store, duration=deadline - time.monotonic(), transport=transport, **kwargs
                )
                try:
                    acquire(store, journal, client)
                finally:
                    client.close()
            journal.data["stage"] = "build"
            journal.write()
            release_id = build(store, journal.data["acquisition_id"])
            if time.monotonic() >= deadline:
                raise BudgetExceeded("invocation deadline exhausted")
            journal.data.update(stage="complete", release_id=release_id, error=None)
            journal.write()
            if spec["publish"]:
                store.publish(release_id)
            return journal.data
        except Exception as exc:
            journal.data["error"] = type(exc).__name__
            journal.write()
            raise


def replay(root, acquisition_id, *, publish=False):
    store = Store(root, CONTRACT)
    with store.lock:
        release = build(store, acquisition_id)
        store.verify(release)
        if publish:
            store.publish(release)
        return {"release_id": release, "verified": True}
