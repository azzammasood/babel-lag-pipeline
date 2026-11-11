# Milestone 2 plan: live vertical slice, fully observable

Goal (brief section 7): the whole path end to end on a thin slice. Live stream for all wikis;
history, pageviews and sitelinks for 20 languages. Checkpoint: a `free` run publishes fresh data;
a simulated `full` outage falls back; lineage from live event to export field visible in Unity
Catalog, Marquez and OpenMetadata; metadata completeness >= 95%; metrics validate.

## Slice (ADR-007)
20 Wikipedias whose 2026-10 history ships as one all-time file (7.1 GB in all, measured listing):
ur, bn, hi, sw, ta, te, mr, ne, pa, el, ms, cy, eo, tl, am, yo, ha, ig, so, zu. Small and
under-served languages first, since the question is who stays in the dark. Coverage lag in this
milestone is relative to the earliest article *within the slice* (`reference_set = slice20`);
milestone 3 widens the reference to all languages.

## Checkpoint 2a: ingestion
- `stream/collector.py`: async SSE over httpx, one connection, checkpoint per stream
  (`Last-Event-ID` offsets, else `since`), reconnect with exponential backoff, sanitize before
  storing, micro-batches `<stream>/<yyyy>/<mm>/<dd>/<first-event-dt>_<run-id>.ndjson`.
- Sinks: files (`free`); Kafka topic with an Avro schema in the schema registry (`full`).
- `dumps/`: history (78-column TSV, verified on zuwiki), pageview_complete, `wb_items_per_site`;
  stream and delete, parse to compact Parquet, quarantine with reasons. One fixture test each.

## Checkpoint 2b: lakehouse and logic
- Delta via delta-rs: `bronze_live_events`, `silver_live_events` (dedup on event id, late flag),
  `silver_page_revisions`, `silver_sitelinks`, `silver_pageviews_daily`, gold `gold_live_activity`
  (15-minute event-time windows) and `gold_coverage_lag`.
- Core SQL once (`rollup/sql/*.sql`), run by DuckDB and Spark; parity test on shared fixtures with
  late and duplicate events (Spark leg in CI with Java).
- `full`: Kafka (KRaft) + schema registry (Compose `stream`), Spark Structured Streaming from
  Kafka to Delta, Lakeflow pipeline (Auto Loader + expectations) bronze to gold on the UC Volume.

## Checkpoint 2c: models, quality, metadata
- dbt (duckdb + databricks targets, same models), tests and docs; completeness gate >= 95%.
- ODCS contracts (datacontract-cli lint), Soda Core checks on DuckDB.
- OpenLineage from every stage (file transport always, Marquez when `lineage` is up), dbt via
  its manifest; OpenMetadata ingestion (Compose `catalog`); UC tags and comments.
- OTel gauges to Prometheus and Grafana (Compose `obs`); `pipeline_metrics.json` with a schema.
- Static metadata export (`metadata/catalog.json`, `lineage.json`, `glossary.json`) and the
  versioned JSON export with JSON Schema.

## Checkpoint 2d: operation
- `babel profile check [--run]` with probes and automatic fallback; `produced_by` everywhere.
- Databricks Asset Bundle (`databricks.yml`) deployed from CI; Dagster asset graph over the CLI.
- `publish.yml` every 15-30 minutes, gated by `BABEL_SCHEDULES` (off while private, ADR-003);
  failures open or update an issue. Manual/local runs prove the checkpoint while schedules are off.

## Then
Blocker-only review in a fresh subagent -> `docs/reviews/m2.md`; fixes; CI green; push.
