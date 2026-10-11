# Project agent notes

Public bounded Kalshi candles, trades and REST book snapshots. Consume pinned oddsfox.kalshi.metadata.v1 bundles; metadata acquisition belongs to kalshi-events. Stack: Python3.12, uv, HTTPX, DuckDB and Parquet; no dlt, dbt or Dagster.

## Invariants

- Keep oddsfox-pipeline unchanged; no runtime imports, shared warehouse or state migration.
- Preserve raw evidence, native tickers, exact decimals and honest per-selection coverage.
- Bounded requests, downloads, responses, duration and storage; commit raw before checkpoints.
- One canonical writer lock per root; failed/incomplete work cannot promote publication.
- Tests are offline; data, credentials and Pad content stay outside Git and distributions.
- No trading, authenticated WebSocket recording, activated schedules, commit or push.

## Verification

Use scripts/verify-fast and scripts/verify, installed wheels outside checkouts and synthetic source fixtures.
