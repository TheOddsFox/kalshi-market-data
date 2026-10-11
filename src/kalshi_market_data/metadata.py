"""Pin selected metadata without trusting paths or producer retention."""

from __future__ import annotations

import os
import re
import shutil
import stat
import tempfile
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path

from .foundation import (
    BudgetExceeded,
    atomic_write,
    digest,
    fsync_dir,
    json_bytes,
    json_loads,
    read_bytes,
)

CONTRACT = "oddsfox.kalshi.metadata.v1"
FILES = {"series.json", "events.json", "markets.json", "coverage.json"}
TICKER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,255}\Z")
MAX_FILE = 128 * 1024**2
MAX_BUNDLE = 128 * 1024**2


@dataclass(frozen=True)
class MetadataBundle:
    path: Path
    manifest: dict
    markets: list
    series: list
    events: list
    coverage: list


def _file(fd, name):
    child = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
    try:
        info = os.fstat(child)
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_FILE:
            raise ValueError("metadata requires bounded regular files")
        with os.fdopen(child, "rb", closefd=False) as stream:
            result = stream.read(MAX_FILE + 1)
        if len(result) > MAX_FILE:
            raise ValueError("metadata file limit")
        return result
    finally:
        os.close(child)


def _read(directory, expected_digest):
    if not re.fullmatch(r"[0-9a-f]{64}", expected_digest or ""):
        raise ValueError("operator-pinned metadata manifest SHA256 required")
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        raw_manifest = _file(fd, "manifest.json")
        if digest(raw_manifest) != expected_digest:
            raise ValueError("metadata manifest digest mismatch")
        manifest = json_loads(raw_manifest)
        if (
            not isinstance(manifest, dict)
            or manifest.get("contract") != CONTRACT
            or set(manifest.get("files", {})) != FILES
        ):
            raise ValueError("unsupported metadata contract or inventory")
        if set(os.listdir(fd)) != FILES | {"manifest.json"}:
            raise ValueError("unexpected metadata file")
        if manifest.get("coverage_scope") != "targeted":
            raise ValueError("targeted metadata coverage required")
        if manifest.get("source_release_id") != digest(json_bytes(manifest["files"])):
            raise ValueError("metadata release inventory identity mismatch")
        blobs = {"manifest.json": raw_manifest}
        total = len(raw_manifest)
        for name in sorted(FILES):
            raw = _file(fd, name)
            total += len(raw)
            expected = manifest["files"][name]
            if len(raw) != expected["size"] or digest(raw) != expected["sha256"]:
                raise ValueError("metadata file digest mismatch")
            if total > MAX_BUNDLE:
                raise ValueError("metadata bundle limit")
            rows = json_loads(raw)
            if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                raise ValueError("metadata relation must contain objects")
            blobs[name] = raw
        return manifest, blobs
    finally:
        os.close(fd)


def load_metadata(directory, expected_digest):
    manifest, blobs = _read(directory, expected_digest)
    relations = {name[:-5]: json_loads(blobs[name]) for name in FILES}
    bundle = MetadataBundle(Path(directory), manifest, **relations)
    _validate(bundle)
    return bundle


def ticker(value):
    if not isinstance(value, str) or not TICKER.fullmatch(value):
        raise ValueError("invalid native ticker")
    return value


def _validate(bundle):
    indexed = {}
    for relation, key in (
        (bundle.series, "series_ticker"),
        (bundle.events, "event_ticker"),
        (bundle.markets, "market_ticker"),
        (bundle.coverage, "market_ticker"),
    ):
        rows = {}
        for row in relation:
            value = ticker(row.get(key))
            if value in rows:
                raise ValueError("duplicate metadata identity")
            rows[value] = row
        indexed[key if relation is not bundle.coverage else "coverage"] = rows
    for event in bundle.events:
        ticker(event.get("series_ticker"))
    for market in bundle.markets:
        ticker(market.get("event_ticker"))
        ticker(market.get("series_ticker"))
        if not isinstance(market.get("usable"), bool):
            raise ValueError("metadata usable must be boolean")
        event = indexed["event_ticker"].get(market["event_ticker"])
        if market["usable"] and (
            event is None
            or event["series_ticker"] != market["series_ticker"]
            or market["series_ticker"] not in indexed["series_ticker"]
        ):
            raise ValueError("usable market lacks authoritative parent identity")
        if market.get("ticker", market["market_ticker"]) != market["market_ticker"]:
            raise ValueError("native market ticker disagrees")
    for row in bundle.coverage:
        if row.get("status") not in {"found", "absent", "unresolved", "unusable"}:
            raise ValueError("invalid coverage state")
    if len(bundle.coverage) > 100:
        raise ValueError("metadata selection exceeds 100 markets")


def pin_metadata(root, directory, expected_digest, *, store=None):
    root = Path(root).absolute()
    if any(parent.is_symlink() for parent in (root, *root.parents)):
        raise ValueError("symlink metadata root")
    if store is not None and root.absolute() != store.root:
        raise ValueError("metadata budget store/root disagreement")
    with store.lock if store is not None else nullcontext():
        manifest, blobs = _read(directory, expected_digest)
        # Validate before any committed input appears.
        load_metadata(directory, expected_digest)
        parent = root / "inputs"
        if parent.is_symlink():
            raise ValueError("symlink metadata destination")
        parent.mkdir(parents=True, exist_ok=True)
        target = parent / expected_digest
        if target.exists():
            return load_metadata(target, expected_digest)
        incoming = sum(map(len, blobs.values()))
        if store is not None:
            store._budget(incoming)
            if store.limits and incoming > store.limits.max_temp_bytes:
                raise BudgetExceeded("metadata temporary byte budget exhausted")
        temporary = Path(tempfile.mkdtemp(prefix=".pending-", dir=parent))
        try:
            for name, raw in blobs.items():
                (store.atomic_write if store is not None else atomic_write)(temporary / name, raw)
            load_metadata(temporary, expected_digest)
            (store.rename if store is not None else os.rename)(temporary, target)
            fsync_dir(parent)
        finally:
            if temporary.exists():
                (store.remove if store is not None else shutil.rmtree)(temporary)
        return load_metadata(target, expected_digest)


def select_markets(bundle, market_tickers):
    selected = sorted(set(map(ticker, market_tickers)))
    if not 1 <= len(selected) <= 100:
        raise ValueError("select 1..100 explicit market tickers")
    markets = {row["market_ticker"]: row for row in bundle.markets}
    coverage = {row["market_ticker"]: row for row in bundle.coverage}
    for native in selected:
        if native not in markets or markets[native]["usable"] is not True:
            raise ValueError("selected market missing or unusable")
        if coverage.get(native, {}).get("status") != "found":
            raise ValueError("selected metadata coverage unresolved")
    return [markets[native] for native in selected]


def manifest_digest(directory):
    return digest(read_bytes(Path(directory) / "manifest.json"))
