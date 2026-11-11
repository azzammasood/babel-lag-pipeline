# Decisions (ADRs)

Newest first. Each: context, choice, why.

## ADR-006 (2026-11-11) Backfill window: all Wikipedias, full history, pageviews from 2015-07-01
- Rule (brief section 3): the largest window and language set at $0 whose unattended,
  resumable wall time is <= 7 days; minimum all languages, pageviews 2015+, full creation history.
- Inputs (spike): history 2026-10 Wikipedias ~326 GB; pageview `user` files from 2015-07-01
  ~2,130 GB (*est.* from listing); sitelinks 1.9 GB; total ~2.46 TB. Single-core Python bz2
  parse measured at ~3.0 MB/s compressed (707 MB in 236 s); umu.se mirror 11.3 MB/s per
  connection from this machine. Etiquette caps bulk downloads at 2 concurrent.
- *est.* wall time on Actions (public repo, unlimited minutes), 2 concurrent chunk jobs:
  worst case single-core parse 2.46e6 MB / (2 x 3.0 MB/s) = ~4.7 days; with parallel bz2
  decompression on the 4-core runners (lbzip2), download-bound at ~2 x 11 MB/s = ~1.3 days.
  Both <= 7 days, so the maximum is chosen: **all ~380 Wikipedias, full edit history since 2001,
  `user` pageviews from 2015-07-01** (the start of the modern pageview definition used by the
  Pageviews API; pageview_complete before that is back-filled from older pagecounts and is not
  comparable). Spider and automated traffic are excluded (demand means humans).
- Guard: the first milestone 3 chunks measure real rates; if the projection exceeds 7 days the
  pageview start moves forward a year at a time and this ADR is updated. Needs the repo public
  (ADR-003); until then backfill chunks run only for the ~20-language milestone 2 slice.

## ADR-005 (2026-11-11) Databricks ingestion: sanitized micro-batches to a UC Volume, Auto Loader
- Context: Free Edition cannot reach Wikimedia (outbound restricted). Spike section 7: a token
  from this machine created schemas and a volume, uploaded NDJSON with the Files API (2.5 s) and
  triggered a serverless Lakeflow pipeline whose Auto Loader table picked up both batches exactly
  (1,020 then 2,041 rows; 46.9 s full, 12.8 s incremental).
- Choice: the collector (or the Actions catch-up) sanitizes events (allow-list in
  `stream/sanitize.py`), writes NDJSON micro-batches and uploads them to
  `/Volumes/workspace/babel_landing/live/`; the triggered (not continuous) pipeline
  `babel-bronze-live` builds bronze with expectations. Triggered mode spares the daily quota;
  `babel profile check` falls back to `free` when compute is refused (Keystone ADR-087).
- Personal data never reaches Databricks: sanitization happens before upload.
- New account (owner), so its quota is not shared with Keystone's blocked workspace.

## ADR-004 (2026-11-11) Bulk work runs on Actions runners; the local disk is for samples and Docker
- Context: `babel machine` measured 25.9 GB free on C: (budget 70% = 18.1 GB), below the brief's
  50 GB threshold. Docker Desktop is installed but was not running; RAM 16.8 GB total.
- Choice: the history and pageview backfill run as resumable chunk jobs on GitHub-hosted runners
  that stream and delete (Keystone's pattern); local runs use samples only. Docker images for the
  showcase profiles count against the 18.1 GB budget and are measured before pulling.
- Why: $0, bounded local disk, proven in Keystone. Flagged to the owner (more disk widens options).
- Update (same day): the owner freed space; 51.1 GB free, budget 35.8 GB. Choice unchanged; the
  Compose showcase profiles now fit. At project end `babel clean` (dry run first, owner approves)
  removes `data/`, `.venv` and this project's Compose images and volumes.

## ADR-003 (2026-11-11) Scheduled publishing and backfill wait for a public repo
- Context: on a free GitHub plan a private repo gets 2,000 Actions minutes per month; jobs are
  billed per started minute. A 15-30 minute publish (48-96 runs/day at 2+ min) needs about
  2,900-5,800 min/month; the backfill needs tens of job hours. Public repos have no minute limit.
- Choice: while private, CI runs on push only and schedules stay behind a repository variable
  `BABEL_SCHEDULES` (off). Live catch-up uses EventStreams `since`, so a later start loses nothing
  inside the replay window measured in the spike. Going public is the owner's call (hard stop).
- Why: $0 first; no surprise exhaustion of the owner's minutes.

## ADR-002 (2026-11-11) Python package `babel_lag`, CLI `babel`, lean tooling
- Choice: `src/babel_lag`, Typer CLI `babel`, uv, ruff, pytest, pre-commit with gitleaks; CI runs
  ruff, pytest and gitleaks. No mypy gate (speed mode, brief section 0 rule 10).
- Why: Keystone's layout minus the gates the brief drops.

## ADR-001 (2026-11-11) Keystone is the template
- Context: Keystone (`../keystone`) already solved profiles with fallback, a Databricks Free
  Edition rung over the Files API, Kafka with a schema registry, OpenLineage/Marquez, the export
  envelope and `pipeline_metrics.json`, failure issues and a watchdog for schedules.
- Choice: copy its patterns and adapt; do not re-derive. Notable lessons carried over: Free
  Edition cannot fetch data (stage files into a Volume); system-table lineage lags; Free Edition
  can block compute (`DENY_NEW_AND_EXISTING_RESOURCES`), so the keepalive proves activity via the
  control plane and compute stays best effort; cap DuckDB `memory_limit` on runners; new release
  tags go on the default branch head.
