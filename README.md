# Kalshi market data

An independent operator-invoked collector for public Kalshi candlesticks,
trades (including block trades), and current REST order books. Discovery,
rules, relationships and lifecycle metadata belong to `kalshi-events`.
This package verifies and copies metadata bundles; it imports no producer
or pipeline package and never opens their database.

Python 3.12 and uv are required: `uv sync --locked` or install the wheel.
Keep operator roots outside checkouts, or in ignored `data/`.
No schedules, trading, credentials or authenticated WebSockets are included.

## Pinned collection

Supply a trusted manifest SHA256 and 1–100 explicit native market tickers:

```sh
uv run kalshi-market-data --root /absolute/operator/kalshi-data run \
  --metadata /absolute/operator/kalshi-metadata --metadata-sha256 MANIFEST_SHA256 \
  --market-ticker KXEXAMPLE-EVENT-MARKET \
  --feed candles --feed trades --feed books \
  --start 2026-01-01T00:00:00Z --end 2026-01-02T00:00:00Z \
  --period-interval 1 --period-interval 60 --period-interval 1440
```

`run` captures, builds, verifies and publishes. `collect` builds and verifies
without changing the publication pointer. `plan` seals an offline pinned
selection without HTTP. Returned JSON contains `run_id`, immutable `plan_id`,
`acquisition_id`, `release_id` and durable resource accounting.

Alternatively delegate metadata acquisition to an installed producer using
its persistent, disjoint operator root:

```sh
uv run kalshi-market-data --root /absolute/operator/kalshi-data run \
  --events-executable /path/to/bin/kalshi-events --events-root /absolute/operator/kalshi-events \
  --market-ticker KXEXAMPLE-EVENT-MARKET --feed candles --feed trades --feed books \
  --start 2026-01-01T00:00:00Z --end 2026-01-02T00:00:00Z
```

The producer owns its retained evidence, lock, ledger and separate storage
limit. Measured producer HTTP attempts and downloads debit the consumer run.
Inherited `KALSHI_EVENTS_*` settings are removed. Cleanup removes only the
consumer's temporary handoff directory. Failed or busy refreshes cannot reuse
stale metadata. Production HTTP uses only
`https://external-api.kalshi.com/trade-api/v2`; redirects, ambient credentials
and proxies are disabled.

## Contracts and facts

Pinned `oddsfox.kalshi.metadata.v1` has exactly `series.json`, `events.json`,
`markets.json`, `coverage.json` and `manifest.json`. Each file and their total,
including the manifest, are limited to 128 MiB. File hashes, sizes, inventory,
native parent identity and the operator pin are verified before copying.
`source_release_id` hashes compact sorted JSON of `files`, without a trailing
newline. Metadata describes observed definitions; it does not establish rules
applicable at an earlier trade time.

Immutable `oddsfox.kalshi.market-data.v1` releases contain typed Parquet:

| Relation | Grain and meaning |
| --- | --- |
| `instruments` | Native market and YES/NO side, metadata provenance |
| `candlesticks` | Capture, ticker, 1/60/1440-minute period, native end timestamp; observed revisions |
| `latest_candlesticks` | Deterministic latest receipt per ticker, period, end timestamp |
| `trades` | Native ticker and trade ID, once; conflicting duplicates fail publication |
| `book_snapshots` | Capture and market, request/receipt time, depth, side-level counts, including empty books |
| `book_levels` | Native YES/NO bids with exact prices and quantities; no invented asks |
| `coverage` | Selected ticker/feed, completed observation interval |

Financial facts use exact `DECIMAL(38,18)` supported by Arrow and DuckDB.
Unsupported precision fails rather than rounds. Raw wire bytes and source
financial strings remain in immutable evidence; normalized rows keep native
payloads. Null prices, lifecycle states, trade direction and block flags are
preserved. Malformed responses or missing book sides fail; valid empty books
have a header and zero levels. Live `*_dollars` and archive dollar strings are
normalized as dollars; legacy numeric cents are converted exactly.

Historical intervals are whole-second UTC `[start,end)`. Inclusive source
candle bounds are adapted and filtered. Trade timestamps retain nanoseconds
using RFC 3339 date-times with `Z` or a `±HH:MM` offset. Compact ISO forms,
invalid offsets and fractional timezone offsets are rejected rather than
silently truncated. Source filters overlap boundaries, and normalized facts
enforce the interval.
No synthetic pre-start candle is requested. Candles route by market settlement
time and reconcile both tiers. Trades read live first, route archives by
`trades_created_ts`, and recheck the cutoff for movement. See
[Kalshi historical data](https://docs.kalshi.com/getting_started/historical_data).
REST books observe the current book independently of the historical interval;
coverage claims neither continuous books nor an atomic exchange snapshot.

## Limits, recovery and verification

Defaults: one worker, two attempts per second, 1,000 attempts, 4 GiB downloads,
16 GiB consumer storage, 2 GiB temporary output, 16 MiB responses, 120 seconds
per invocation. Corresponding `--max-*` flags set immutable run limits.
Retries count. Unknown crash grants stay charged until accounting is proven.
Completed resume performs no acquisition HTTP.

```sh
uv run kalshi-market-data --root /absolute/operator/kalshi-data resume --run-id RUN_ID
uv run kalshi-market-data --root /absolute/operator/kalshi-data replay --acquisition-id ACQUISITION_ID
uv run kalshi-market-data --root /absolute/operator/kalshi-data publish --release-id RELEASE_ID
uv run kalshi-market-data --root /absolute/operator/kalshi-data verify
uv run kalshi-market-data --root /absolute/operator/kalshi-data status
scripts/verify-fast
scripts/verify
uv run --locked python scripts/check-distributions.py
uv run --locked python scripts/check-installed.py
```

Raw pages commit before cursor checkpoints. Resume verifies existing raw pages
and restarts expired cursors with bounded generations. Incomplete or conflicting
work cannot publish. Atomic publication preserves the previous pointer on
failure. Replay verifies plans, captures, metadata pins and release inputs;
replaying twice yields the same release. Back up the entire stopped root,
including `.state`, inputs, plans, acquisitions and publications. Verify restored
copies before replay. Never reconstruct accounting from captures or reset
unknown grants. Exhausted sealed runs need a new explicitly larger run, rather
than changed settings.

Use canonical absolute operator paths; symlinks in a root or its parents are
rejected. For cross-repository acceptance, install each built wheel in its own
environment and run the synthetic exchange harness outside both checkouts:

```sh
/path/to/data-env/bin/python scripts/check-handoff.py \
  --events-executable /path/to/events-env/bin/kalshi-events \
  --market-data-executable /path/to/data-env/bin/kalshi-market-data \
  --python /path/to/data-env/bin/python
```

The harness verifies catalogue publication, metadata compatibility, all feeds,
exact decimals, replay, backup/restore, persistent refresh and consumption after
the producer becomes unavailable. The consumer environment must contain no
producer package.

Tests and CI are synthetic. Only explicit fixture configuration
`KALSHI_MARKET_DATA_ALLOW_LOOPBACK=1` plus
`KALSHI_MARKET_DATA_API_URL=http://127.0.0.1:PORT/trade-api/v2` permits loopback.
The delegated producer receives this explicit origin. Distributions exclude
operator data, state, credentials, Pad and compiled caches.

Live acquisition and archive movement timing remain unvalidated. The donor
`oddsfox-pipeline` and its WC2026 analytics remain unchanged.
