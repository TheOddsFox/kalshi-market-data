"""Operator-invoked public acquisition; no implicit discovery or scheduler."""

import argparse
import sys

from filelock import Timeout

from . import CONTRACT
from .foundation import Store, digest, json_bytes
from .metadata import pin_metadata, select_markets
from .workflow import DEFAULT_LIMITS, replay, run, settings


def parser():
    value = argparse.ArgumentParser(prog="kalshi-market-data")
    value.add_argument("--root", required=True, help="canonical consumer operator root")
    commands = value.add_subparsers(dest="command", required=True)
    for name in ("run", "collect", "plan"):
        command = commands.add_parser(name)
        command.add_argument("--market-ticker", action="append", required=True)
        command.add_argument(
            "--feed", action="append", choices=("candles", "trades", "books"), required=True
        )
        command.add_argument("--start", required=True)
        command.add_argument("--end", required=True)
        command.add_argument("--period-interval", type=int, action="append", choices=(1, 60, 1440))
        command.add_argument("--depth", type=int, default=0)
        command.add_argument("--metadata")
        command.add_argument("--metadata-sha256")
        command.add_argument("--events-executable")
        command.add_argument("--events-root")
        command.add_argument("--max-duration-seconds", type=int, default=120)
        for limit, default in DEFAULT_LIMITS.as_dict().items():
            command.add_argument("--" + limit.replace("_", "-"), type=int, default=default)
    command = commands.add_parser("resume")
    command.add_argument("--run-id", required=True)
    for name in ("build", "replay"):
        command = commands.add_parser(name)
        command.add_argument("--acquisition-id", required=True)
        command.add_argument("--publish", action="store_true")
    command = commands.add_parser("publish")
    command.add_argument("--release-id", required=True)
    command = commands.add_parser("verify")
    command.add_argument("--release-id")
    commands.add_parser("status")
    return value


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if args.command in ("run", "collect"):
            result = run(args.root, settings(args), publish=args.command == "run")
        elif args.command == "resume":
            result = run(args.root, run_id=args.run_id)
        elif args.command == "plan":
            spec = settings(args)
            if not spec["metadata"]:
                raise ValueError(
                    "plan requires offline pinned metadata; use run for delegated refresh"
                )
            store = Store(args.root, CONTRACT, DEFAULT_LIMITS.from_dict(spec["limits"]))
            with store.lock:
                bundle = pin_metadata(
                    store.root, spec["metadata"], spec["metadata_sha256"], store=store
                )
                select_markets(bundle, spec["market_tickers"])
                pin = digest((bundle.path / "manifest.json").read_bytes())
                result = {"plan_id": store.create_plan({**spec, "metadata_sha256": pin})}
        elif args.command in ("build", "replay"):
            result = replay(args.root, args.acquisition_id, publish=args.publish)
        else:
            store = Store(args.root, CONTRACT)
            if args.command == "publish":
                result = store.publish(args.release_id)
            elif args.command == "verify":
                result = store.verify(args.release_id)
            else:
                result = store.status()
        print(json_bytes(result).decode())
        return 0
    except Timeout:
        print('{"error":"operator root busy"}', file=sys.stderr)
    except (ValueError, OSError, KeyError, TypeError) as exc:
        # URLs, credential-like response fields and source diagnostics never reach stderr.
        print(
            json_bytes(
                {
                    "error": type(exc).__name__,
                    "detail": "operation failed; inspect retained run evidence",
                }
            ).decode(),
            file=sys.stderr,
        )
    return 1
