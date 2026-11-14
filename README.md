# Babel Lag (pipeline)

**When the world learns something new, how long until it reaches each language on Wikipedia,
and which languages stay in the dark?**

Babel Lag measures, for every Wikidata topic, when each Wikipedia language edition got its
article, how long it waited after the first language (coverage lag), how deep the article is
(coverage depth), and how much reader interest a language leaves unserved (demand without
supply). A live layer follows every change to every Wikimedia wiki as it happens. Lag is measured
on Wikipedia only and says nothing about other media in a language.

Third project after Roshni and Keystone. Costs $0. Non-commercial. MIT.

## Sources (attribution)
All data comes from the Wikimedia Foundation. Thank you, Wikimedia and its editors.
- **EventStreams** (recentchange, revision-create, page-create): every live change.
- **MediaWiki history dumps** (2026-10 snapshot, CC0): every revision since 2001.
- **Pageview complete dumps** (`user` agent, CC0): human pageviews per article and day.
- **Wikidata `wb_items_per_site`** (CC0): which article in which language is which topic.
Bulk files come from the ftp.acc.umu.se mirror, at most 2 downloads at a time, with a
descriptive User-Agent ([config/policy.yml](config/policy.yml)).

## Method and limits
[docs/method.md](docs/method.md). In short:
- an article's creation time is its earliest surviving revision;
- redirects, deleted pages and bot-created pages are excluded;
- coverage lag is the gap to the topic's first article in the reference set: 20 languages in
  milestone 2, all 365 Wikipedias after the backfill. Every row says which.

Bots are excluded by flag. The demand measure is a stated proxy: views elsewhere of topics a
language lacks, not searches in that language.

## Privacy
- Editor names, IP addresses and comments are dropped at ingestion
  ([sanitize.py](src/babel_lag/stream/sanitize.py), [history parser](src/babel_lag/parsers/history.py)).
- A salted hash exists only for distinct counts and is never published.
- Every export is an aggregate by language, topic, wiki or time.
- A test runs the pipeline on events carrying names, IPs and comments, then scans every silver
  and gold table and every export for them.

## Architecture: one logic, two profiles
| Layer | `full` (showcase) | `free` (production, $0) |
| --- | --- | --- |
| Live ingestion | async SSE collector -> Kafka (KRaft) + schema registry (Avro, backward compatibility) | same collector on GitHub Actions, catching up with `since` |
| Lakehouse | UC volume -> Auto Loader -> Lakeflow pipeline `babel-lake` (expectations), deployed by an Asset Bundle | delta-rs Delta tables |
| Core logic | Spark SQL / Spark Structured Streaming | DuckDB, same SQL ([rollup/sql](src/babel_lag/rollup/sql)) |
| Models | dbt-databricks | dbt-duckdb, same models |
| Quality | Lakeflow expectations, dbt tests, Soda Core, ODCS contracts (datacontract-cli) | same |
| Lineage and catalog | Unity Catalog, OpenLineage -> Marquez, OpenMetadata | static metadata export |
| Observability | OpenTelemetry -> Prometheus -> Grafana | `pipeline_metrics.json` |
| Orchestration | Databricks Jobs, Dagster asset graph | GitHub Actions every 30 minutes |

`babel profile check --run` probes `full` and falls back to `free` automatically. Every export
records `produced_by` and any fallback. Parity: the CI parity test proves DuckDB and Spark give
identical outputs on fixtures with late and duplicate events. On the real 20-language slice,
DuckDB and Databricks agreed on all 20 language rows.

## What produced the current data
Each export (`site-data` pre-release) carries `produced_by.profile`, `fallback` and `run_id`.
`pipeline_metrics.json` lists only engines that ran, with versions and last use.

Tools with evidence of real work:
- **Run log:** `data/telemetry/runs.jsonl`.
- **Unity Catalog lineage:** [uc_lineage_*.json](docs/evidence).
- **Marquez:** [marquez_lineage.json](docs/evidence/marquez_lineage.json).
- **OpenMetadata and Grafana:** [screenshots](docs/evidence).
- **Docker profiles:** [spike report section 9](docs/spike-report.md).

The weekly AI report (`babel report`, free OpenRouter models only, numbers validated) is built
but has not run: no API key is set, so it is not claimed as a working tool.

## Benchmarks
[docs/benchmarks.md](docs/benchmarks.md):
- DuckDB rolls up 72.4 M revisions in 104 s on a laptop.
- Lakeflow updates the same tables in about 106 s on Databricks Free Edition.
- The backfill of all 365 Wikipedias (352.5 GB) runs on GitHub-hosted runners.

## Run it
```
uv sync --all-groups
uv run babel dumps slice                          # 20-language slice (stream and delete)
uv run babel profile check --run --want free --dumps
uv run babel backfill plan                        # chunks for .github/workflows/backfill.yml
```
Docs:
- **Decisions:** [docs/DECISIONS.md](docs/DECISIONS.md).
- **Progress:** [docs/PROGRESS.md](docs/PROGRESS.md).
- **Feasibility:** [docs/spike-report.md](docs/spike-report.md).
- **Reviews:** [docs/reviews](docs/reviews).
- **Claims audit:** [docs/claims-audit.md](docs/claims-audit.md).

Corrections: open an issue.
