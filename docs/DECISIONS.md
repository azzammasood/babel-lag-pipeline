# Decisions (ADRs)

Newest first. Each: context, choice, why.

## ADR-011 (2026-11-11) Late and duplicate events: a deterministic rule both engines share
- Spark's watermark depends on processing order, which DuckDB cannot replay. The core SQL
  defines lateness from data alone: a batch's watermark is the latest event time of earlier
  batches *of the same stream* (streams replay at different speeds under `since`); an event more
  than 600 s older is late. Duplicates (redelivery after a reconnect) are dropped on the event id,
  earliest arrival kept. Late events are counted apart, not in activity. The Spark Structured
  Streaming job also runs native watermarked windows (`stream_live_windows`) as the showcase.

## ADR-010 (2026-11-11) Catalogs: OpenLineage to Marquez, OpenMetadata over REST, UC by SQL
- One lineage graph (`lineage/graph.py`) and one metadata file (`config/datasets.yml`) feed every
  tool: OpenLineage events per stage (file always, Marquez when `OPENLINEAGE_URL` is set),
  OpenMetadata tables, owners, domain, glossary and column lineage through its REST API (lighter
  than the ingestion framework, same entities), UC comments and tags through SQL, and the static
  `metadata/*.json`. Completeness counts the real Delta and dbt columns, not only the documented
  ones, so an undocumented new column lowers the gate number.

## ADR-009 (2026-11-11) Soda Core and datacontract-cli run in isolated `uvx` environments
- Soda Core 3.5.6 pins OpenTelemetry 1.11, which conflicts with the pipeline's OTel 1.4x;
  datacontract-cli pulls a large tree. Both run through `uvx` with pinned versions, so they do real
  work without constraining the project's dependencies. Their results feed `quality` in metrics.
- History dumps are parsed by DuckDB `read_csv` (1.4 s vs 6.7 s for 10.5 MB of zuwiki, identical
  68,874 rows) after a multithreaded bz2 decompression (lbzip2 on runners, 7-Zip on Windows, 2x
  faster than bzip2 here); rejects go to quarantine with DuckDB's error type, never the raw line.

## ADR-008 (2026-11-11) What `full` means where, and the fallback
- `full` = Databricks Free Edition (UC volume, Lakeflow `babel-lake`, dbt-databricks, SQL
  warehouse) plus, on the owner's machine, the compose `stream` profile (Kafka KRaft + schema
  registry, Spark Structured Streaming). On GitHub runners there is no Kafka, so `full` there is the
  Databricks rung only (`BABEL_FULL_STREAM`). `babel profile check` probes Databricks (missing or
  revoked token, deleted account, blocked workspace) and, where expected, Kafka and the registry;
  any failure, or a failing full job, runs `free` and records `fallback` and the reason in every
  export and in `pipeline_metrics.json`. A fallback opens (then edits) one issue.
- Quota: the pipeline is triggered, not continuous; a 30-minute cadence costs one short update per
  run. If the Free Edition quota stops compute, the probe or the job fails and `free` publishes.

## ADR-007 (2026-11-11) The milestone 2 slice: 20 single-file Wikipedias
- 20 languages whose 2026-10 history is one all-time file, 7.1 GB in all (listing): ur, bn, hi,
  sw, ta, te, mr, ne, pa, el, ms, cy, eo, tl, am, yo, ha, ig, so, zu; pageviews for 2026-11-10 (one day);
  sitelinks for the same sites. Small and under-served languages first (the question is who stays
  in the dark), and every file fits the local disk budget while streamed and deleted.
- Coverage lag in milestone 2 is relative to the earliest article *within the slice*
  (`reference_set = slice20`, stated in every row and export); milestone 3 widens it to all
  Wikipedias. Live events cover every wiki already.

## ADR-006 (2026-11-11) Backfill window: all Wikipedias, full history, pageviews from 2015-07-01
- Rule (brief section 3): the largest window and language set at $0 whose unattended,
  resumable wall time is <= 7 days; minimum all languages, pageviews 2015+, full creation history.
- Inputs (spike, corrected crawl): history 2026-10, 377 Wikipedias, 354.6 GB; pageview `user`
  files from 2015-07-01 ~2,137 GB (*est.* from listing); sitelinks 1.9 GB; total ~2.49 TB. Single-core Python bz2
  parse measured at ~3.0 MB/s compressed (707 MB in 236 s); umu.se mirror 11.3 MB/s per
  connection from this machine. Etiquette caps bulk downloads at 2 concurrent.
- *est.* wall time on Actions (public repo, unlimited minutes), 2 concurrent chunk jobs:
  worst case single-core parse 2.49e6 MB / (2 x 3.0 MB/s) = ~4.8 days; with parallel bz2
  decompression on the 4-core runners (lbzip2), download-bound at ~2 x 11 MB/s = ~1.3 days.
  Both <= 7 days, so the maximum is chosen: **all 377 Wikipedias, full edit history since 2001,
  `user` pageviews from 2015-07-01** (the start of the modern pageview definition used by the
  Pageviews API; pageview_complete before that is back-filled from older pagecounts and is not
  comparable). Spider and automated traffic are excluded (demand means humans).
- Update (runner probe, same day): on ubuntu-latest the mirror delivered 87.7 MB/s and lbzip2
  decompressed at 50.9 MB/s (bzip2 11.5 MB/s). *est.* with 2 concurrent jobs, decompress-bound:
  2.49e6 MB / (2 x 50.9 MB/s) = ~6.8 h of transfer and decompression; parsing is the open cost
  (DuckDB over the decompressed stream, measured in milestone 2). Far inside 7 days either way.
- Guard: the first milestone 3 chunks measure real rates; if the projection exceeds 7 days the
  pageview start moves forward a year at a time and this ADR is updated. Needs the repo public
  (ADR-003); until then backfill chunks run only for the ~20-language milestone 2 slice.

## ADR-005 (2026-11-11) Databricks ingestion: sanitized micro-batches to a UC Volume, Auto Loader
- Context: Free Edition cannot reach Wikimedia (outbound restricted). Spike section 7: a token
  from this machine created schemas and a volume, uploaded NDJSON with the Files API (2.6 s) and
  triggered a serverless Lakeflow pipeline whose Auto Loader table picked up both batches exactly
  (1,098 then 2,196 rows; 47.1 s full, 24.1 s incremental; re-run after the sanitizer fix).
- Choice: the collector (or the Actions catch-up) sanitizes events (allow-list in
  `stream/sanitize.py`), writes NDJSON micro-batches and uploads them to
  `/Volumes/workspace/babel_landing/live/`; the triggered (not continuous) pipeline
  `babel-bronze-live` builds bronze with expectations. Triggered mode spares the daily quota;
  `babel profile check` falls back to `free` when compute is refused (Keystone ADR-087).
- Verified from GitHub Actions too (run 42972554683): upload 0.42 s, pipeline 41.5 s / 10.6 s.
  Auto Loader skipped a file overwritten under the same name, so batch files are named
  `<stream>/<yyyy>/<mm>/<dd>/<first-event-dt>_<run-id>.ndjson` and are never rewritten.
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
