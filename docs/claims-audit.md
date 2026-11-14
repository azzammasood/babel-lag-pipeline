# Claims audit (milestone 3)

Every claim in the README and the exports, with its evidence. Status: **verified** (evidence
checked), **gap** (not delivered or not claimed).

| # | Claim | Evidence | Status |
| --- | --- | --- | --- |
| 1 | Every live change on every Wikimedia wiki is ingested | collector over recentchange, revision-create, page-create; 46.07 events/s and 134 wikis in 10 min (spike section 1); 334 wikis in the local live silver (local `pipeline_metrics.json` `totals.wikis`, 48 h bronze retention) | verified |
| 2 | Usernames, IPs and comments are dropped at ingestion | `stream/sanitize.py`, `parsers/history.py`; `test_privacy.py` scans silver, gold and every export of an end-to-end run fed with names, IPs and comments; review m2 checked exports | verified |
| 3 | Full history of every Wikipedia (2026-10 snapshot, 365 editions, 352.5 GB) | backfill run 43578966431, 10/10 chunks, `pages_<wiki>.parquet` for every wiki in release `data-history-2026-10`; ADR-012 corrects 377 -> 365 | verified |
| 4 | Coverage lag against all Wikipedias | assemble run (release `data-gold`, `reference_set = all`); every export row carries `reference_set` | verified in `data-gold` (assemble run 43601433544, 17,806,419 reference items, 357 languages); `site-data` shows it after the next publish |
| 5 | Coverage depth, demand-without-supply proxy | `rollup/sql/04,05,07`; method.md states the proxy's limits | verified |
| 6 | Update lag | not computed (method.md "Update lag (not delivered)") | **gap** |
| 7 | Live topic tracking across languages | live activity per wiki and 15-minute window only; no item-level live spread | **gap** |
| 8 | Backtests with published criteria | `BACKTEST_SQL` (20 items first created 2020-2024 with the most languages), `gold_backtests` in `data-gold` | verified with the assemble run |
| 9 | Same logic on DuckDB and Spark/Databricks | CI parity test (Spark leg, fixtures with late and duplicate events); real slice: 0 differences in 20 language rows DuckDB vs Databricks (PROGRESS 2026-11-13) | verified |
| 10 | `full` falls back to `free` automatically | simulated outage (HTTP 401) and a real network loss both published `free` with `fallback=true` (PROGRESS 2026-11-12/13) | verified |
| 11 | Kafka + schema registry, Spark Structured Streaming | 61,873 Avro messages, BACKWARD compatibility checked; Spark bronze 61,873 rows (spike section 9) | verified (local Docker only) |
| 12 | Lakeflow + Auto Loader + expectations, Asset Bundle from CI | pipeline `babel-lake` COMPLETED (98-108 s); bundle deployed by `cloud.yml`; bundle job `babel-refresh` run SUCCESS (2026-11-14) | verified |
| 13 | Lineage live event -> export field in UC, Marquez, OpenMetadata | `docs/evidence/uc_lineage_*.json` (UC records the pipeline, not the volume, for the Auto Loader stream), `marquez_lineage.json`, `openmetadata-lineage.png` | verified |
| 14 | Metadata completeness >= 95% (CI gate) | `babel metadata build --gate 95` in ci.yml: 100% (162/162) | verified |
| 15 | dbt on DuckDB and Databricks, Soda, ODCS contracts | runs.jsonl: dbt-duckdb and dbt-databricks 8/8 tests, Soda 10/10, 12 contracts valid | verified |
| 16 | OpenTelemetry, Prometheus, Grafana | `docs/evidence/grafana.png`, series listed in spike section 9 | verified (local Docker only) |
| 17 | Dagster asset graph | 5 assets materialized (runs.jsonl `dagster_materialize`, 2026-11-14) | verified |
| 18 | Scheduled publishing, failures open an issue | cron every 30 minutes with `BABEL_SCHEDULES=true`; GitHub delivered 1 scheduled run in the first 4 hours (10:05 UTC, success, with a correct fallback to free); the backfill assemble also triggers a publish; issue scripts in `.github/scripts` | verified that runs publish; the cadence is GitHub best effort, not 30 minutes |
| 19 | Weekly AI report, numbers validated, $0 | `narrate/report.py` with number validator and free-model guard; not run (no API key) | **gap** (not claimed as running) |
| 20 | Benchmarks measured, not estimated | `docs/benchmarks.md`, every row cites runs.jsonl, the spike or a workflow run; peak memory not measured (stated) | verified |
| 21 | Cost $0 | public repo (unlimited Actions minutes), Databricks Free Edition, local Docker; `cost.usd = 0` in metrics | verified |
| 22 | Wikimedia attribution | README Sources, `sources` in every export envelope | verified |
