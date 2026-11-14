# Benchmarks: the scale ladder (measured)

Every number below comes from `data/telemetry/runs.jsonl`, the spike report or a workflow log.
Peak memory was not instrumented in milestone 2/3 and is not reported. Cost is $0 throughout:
GitHub-hosted runners on a public repo, Databricks Free Edition, local Docker.

## Same logic, three engines (core SQL `rollup/sql`, one source)
| Engine | Where | Input | Work | Wall time | Evidence |
| --- | --- | --- | --- | --- | --- |
| DuckDB 2.0 (`free`) | owner's laptop, 8 CPUs, ~1-5 GB RAM free, 3 GB memory limit + spill | 72,402,118 article revisions (20-wiki slice) + 5,142,882 sitelinks + 923,931 pageview rows | all 7 core steps, 12,557,037 output rows | **103.9 s** | runs.jsonl `rollup` 2026-11-12 15:35 |
| Databricks serverless Lakeflow (`full`) | Free Edition, triggered pipeline | slice page creations, sitelinks, pageviews (Parquet on a UC volume) + live bronze | bronze streaming table + 9 materialized views | **105-108 s** per update | runs.jsonl `lakeflow` 2026-11-13; spike section 7 (47.1 s full / 24.1 s incremental on 1 table) |
| Spark 4.0.1 Structured Streaming (`full`, local Docker) | 1.5 GB container | 61,873 Kafka Avro events | Kafka -> Delta bronze, watermarked windows, core SQL | 387 s cold (incl. jar download), 144 s warm | Docker session note, spike section 9 |
| Parity | | | DuckDB vs Databricks on the real slice: **0 differences** in 20 language rows; DuckDB vs Spark on fixtures: identical (CI) | | PROGRESS 2026-11-13; ci.yml |

## Ingestion
| Stage | Measured |
| --- | --- |
| Live catch-up (EventStreams `since`) | 945,725 events in 600 s (1,576 events/s replay throughput); live rate 46.07 events/s (spike, follow mode) |
| Bronze (delta-rs) | 1,182,829 events, 514 MB of NDJSON in 74.7 s |
| Dump slice load (delta-rs, streamed batches) | 78,468,931 rows in 87.4 s |
| History parse | DuckDB `read_csv` 10.5 MB bz2 (95.7 MB TSV) in 1.4 s; Python line parser 6.7 s, identical rows |
| Decompression | runner lbzip2 50.9 MB/s; laptop 7-Zip ~3 MB/s, bzip2 1.5 MB/s (RAM-starved) |
| Upload to UC volume | 449 batches in 1,544 s from the laptop (shared link); 0.42 s per batch from Actions (spike) |
| dbt build | dbt-duckdb 2 models + 8 tests in 31-102 s; dbt-databricks same models + 8 tests in 84 s |
| Quality | Soda 10 checks in 6 s; 12 ODCS contracts (datacontract-cli) in 53-115 s |

## Backfill (all 377 Wikipedias, Actions)
Run https://github.com/azzammasood/babel-lag-pipeline/actions/runs/43578966431: per-chunk bytes,
pages and seconds land in the `data-history-2026-10` release (`stats_<wiki>.json`) and are
summarised here once the run completes.

## Quota behaviour
No Free Edition quota message seen across ~10 pipeline updates and ~15 warehouse sessions in
milestones 1-2. Databricks publishes no quota meter, so `free_quota_use_pct` stays null.
