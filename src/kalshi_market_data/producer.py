"""Bounded producer diagnostics and isolated metadata process configuration."""

from __future__ import annotations

import math
import os
import selectors
import signal
import subprocess
import tempfile
import time
from pathlib import Path

from .budget import nonnegative
from .foundation import BudgetExceeded, json_loads
from .metadata import MAX_BUNDLE, pin_metadata


def canonical_root(value):
    path = Path(value).absolute()
    if any(parent.is_symlink() for parent in (path, *path.parents)):
        raise ValueError("symlink producer root")
    return path.resolve()


def environment(root, limits=None):
    root = canonical_root(root)
    result = {
        key: value for key, value in os.environ.items() if not key.startswith("KALSHI_EVENTS_")
    }
    result.update(
        KALSHI_EVENTS_ROOT=str(root),
        KALSHI_EVENTS_DATA_DIR=str(root / "data"),
        KALSHI_EVENTS_STATE_DIR=str(root / ".state"),
    )
    # Fixture delegation is explicit and independent of ambient producer settings.
    if os.environ.get("KALSHI_MARKET_DATA_ALLOW_LOOPBACK") == "1":
        from .transport import api_origin

        result["KALSHI_EVENTS_ALLOW_LOOPBACK"] = "1"
        result["KALSHI_EVENTS_API_URL"] = api_origin()
    return result


def bounded_run(argv, *, env, directory, store, timeout=120, max_diagnostics=64 * 1024):
    """Drain pipes without unbounded capture_output or leaving a timed-out child alive."""
    deadline = time.monotonic() + timeout
    stdout = bytearray()
    diagnostic_bytes = 0
    allowance = min(store.limits.max_temp_bytes, store.available_bytes())
    if allowance <= 0:
        raise BudgetExceeded("metadata temporary storage limit exhausted")
    try:
        with store.external_write([Path(directory)], allowance) as workspace:
            with subprocess.Popen(
                argv,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            ) as child:
                try:
                    with selectors.DefaultSelector() as selector:
                        selector.register(child.stdout, selectors.EVENT_READ, True)
                        selector.register(child.stderr, selectors.EVENT_READ, False)
                        while selector.get_map() or child.poll() is None:
                            if time.monotonic() >= deadline:
                                raise ValueError("selected metadata refresh deadline exhausted")
                            size = workspace.check()
                            if size > store.limits.max_temp_bytes:
                                raise BudgetExceeded("metadata temporary storage limit exhausted")
                            for key, _ in selector.select(min(0.1, deadline - time.monotonic())):
                                data = os.read(key.fileobj.fileno(), 8192)
                                if not data:
                                    selector.unregister(key.fileobj)
                                    continue
                                diagnostic_bytes += len(data)
                                if diagnostic_bytes > max_diagnostics:
                                    raise ValueError(
                                        "selected metadata refresh diagnostics limit exhausted"
                                    )
                                if key.data:
                                    stdout.extend(data)
                    return subprocess.CompletedProcess(argv, child.wait(), bytes(stdout), b"")
                finally:
                    try:
                        os.killpg(child.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    child.wait()
    except BudgetExceeded:
        raise BudgetExceeded(
            "metadata temporary storage or retained storage limit exhausted"
        ) from None


def refresh_metadata(store, market_tickers, events_executable, events_root, *, timeout=120):
    producer_root = canonical_root(events_root)
    if producer_root.is_relative_to(store.root) or store.root.is_relative_to(producer_root):
        raise ValueError("producer and consumer roots must be disjoint")
    executable = Path(events_executable).resolve(strict=True)
    if not executable.is_file() or not os.access(executable, os.X_OK):
        raise ValueError("explicit events executable must be executable")
    meter = store.run_meter
    remaining = meter.remaining
    if min(remaining) <= 0:
        raise BudgetExceeded("metadata refresh allowance exhausted")
    output_budget = min(MAX_BUNDLE, store.limits.max_temp_bytes // 2, store.available_bytes() // 2)
    if output_budget <= 0:
        raise BudgetExceeded("metadata refresh requires storage headroom")
    reservation = meter.reserve(*remaining, "metadata producer")
    temporary = Path(tempfile.mkdtemp(prefix=".metadata-", dir=store.root / ".state"))
    try:
        directory = temporary / "bundle"
        argv = [
            str(executable),
            "--root",
            str(producer_root),
            "metadata",
            "refresh",
            "--output",
            str(directory),
            "--max-output-bytes",
            str(output_budget),
            "--max-requests",
            str(remaining[0]),
            "--max-download-bytes",
            str(remaining[1]),
            "--max-response-bytes",
            str(min(store.limits.max_response_bytes, remaining[1])),
            "--max-duration-seconds",
            str(max(1, math.ceil(min(120, timeout)))),
        ]
        for native in market_tickers:
            argv.extend(["--market-ticker", native])
        result = bounded_run(
            argv,
            env=environment(producer_root),
            directory=temporary,
            store=store,
            timeout=min(120, timeout),
        )
        try:
            reply = json_loads(result.stdout)
            attempts, downloaded = reply["requests"], reply["downloaded_bytes"]
            nonnegative(attempts)
            nonnegative(downloaded)
            if reply.get("accounting_complete") is not True:
                raise ValueError("producer accounting incomplete")
        except (ValueError, UnicodeError, KeyError, TypeError):
            raise ValueError("metadata refresh lacks complete resource accounting") from None
        meter.settle(reservation, attempts, downloaded)
        if result.returncode:
            raise ValueError("metadata refresh failed or busy")
        if Path(reply.get("output", "")).absolute() != directory:
            raise ValueError("producer output path disagreement")
        bundle = pin_metadata(store.root, directory, reply["manifest_sha256"], store=store)
        if reply.get("source_release_id") != bundle.manifest["source_release_id"]:
            raise ValueError("producer source release disagreement")
        if "root" in reply and canonical_root(reply["root"]) != producer_root:
            raise ValueError("producer persistent root disagreement")
        retained = reply.get("retained_bytes")
        if retained is not None:
            nonnegative(retained)
        store.producer_report = {
            "root": str(producer_root),
            "retained_bytes": retained,
            "requests": attempts,
            "downloaded_bytes": downloaded,
        }
        return bundle
    finally:
        store.remove(temporary)
