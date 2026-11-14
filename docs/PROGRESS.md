# Progress

Resume with: "Continue from docs/PROGRESS.md". Current milestone: **3 (scale-out, method, polish)**. Milestone 2 closed 2026-11-13. Milestone 1 closed 2026-11-11.

## Done
- 2026-11-11 Checkpoint 1a scaffold: uv project, `babel machine`, policy.yml, pre-commit, CI,
  Dependabot, MIT, CLAUDE.md, PLAN, DECISIONS (ADR-001..004). Local lint and tests pass.

- 2026-11-11 Checkpoint 1b spike (docs/spike-report.md): live rate, since depth, dump sizes,
  pageview day, sitelinks, throughput, Databricks Volume + Auto Loader. ADR-005 (Databricks path),
  ADR-006 (backfill window). Oracles frozen in tests/fixtures/oracles/spike.json.
  Sanitizer + privacy test. Review clean after fixes (docs/reviews/m1.md). CI green (6531f78).
- 2026-11-11 Databricks from Actions verified (run 42972554683). **Milestone 1 closed.**
- 2026-11-11 Runner probe: 4 CPU, 15 GB RAM, 88 GB free disk, 87.7 MB/s, lbzip2 50.9 MB/s.
  Repo secrets DATABRICKS_HOST/TOKEN set (owner asked); gh active account now azzammasood.

## Measured (details in docs/spike-report.md)
- Machine: 51.1 GB free, budget 35.8 GB; 16.8 GB RAM; 8 CPUs; Docker 28.0.4 (daemon off).
- Live 46.07 events/s (3 streams), replay ~7.8 days; history 2026-10 676.4 GB (377 Wikipedias 354.6 GB);
  pageviews `user` 2,917 GB since 2011-12 (~2,137 GB since 2015-07, est.); sitelinks 1.9 GB;
  umu.se 11.3 MB/s; Databricks upload 2.6 s/480 KB, Lakeflow 47.1 s full / 24.1 s incremental.

- 2026-11-11 M2 build (commits efa5606, dbca7b5 and later): collector (resumed from Last-Event-ID,
  28,084 events caught up = 46 ev/s, matches spike), parsers (history on DuckDB, ADR-009), Delta
  lake, core SQL + parity fixtures, lineage/metadata/contracts, dbt, Soda, profiles, compose,
  asset bundle, Dagster, publish/cloud workflows, e2e smoke + privacy downstream test (local green).

- 2026-11-11 CI green at 0f469b0 incl. the Spark parity leg (Spark == DuckDB on fixtures),
  completeness 100% (147/147), 13 ODCS contracts valid; asset bundle deployed from CI (cloud.yml).
- Side session `m2-docker-stack` (local_2f40f46a) runs the Docker profiles one at a time; its
  hand-over note goes to the shared notes folder (`m2-docker-stack.md`).

- 2026-11-12/13 **Milestone 2 checkpoint runs (measured):**
  - Slice staged: 72,402,118 article revisions (20 wikis), 5,142,882 sitelinks, 923,931
    pageview rows (2026-11-10); 0 quarantined.
  - `free` publish (`babel profile check --run --want free`): collect -> bronze -> core SQL
    (DuckDB) -> dbt-duckdb (8 tests) -> Soda (10 checks) -> 12 ODCS contracts -> export; all 20
    languages; freshness 247-522 s from newest event to publish.
  - Simulated outage (DATABRICKS_TOKEN invalid): probe HTTP 401 -> `free`, `fallback=true` and the
    reason in every export and in pipeline_metrics.json; 30/30 checks passed. A real overnight
    network loss also fell back correctly (dbt-databricks DNS failure).
  - `full` (BABEL_FULL_STREAM=0): upload to the UC volume, Lakeflow `babel-lake` (bundle-deployed)
    COMPLETED 108 s, UC tags on 9 tables + 81 column comments, dbt-databricks 8/8 tests, export
    from Databricks: **0 differences vs DuckDB on all 20 language rows** (real-data parity).
  - UC lineage (`babel cloud lineage`, data/evidence/uc_lineage_*.json): marts -> gold -> silver
    -> slice volume files; live: mart_live_hourly -> gold -> silver -> bronze -> pipeline
    babel-lake (UC records no file edge for Auto Loader streams; the pipeline reads
    /Volumes/workspace/babel_landing/live).
  - Docker profiles (side session, spike-report section 9): Kafka/registry 61,873 Avro messages,
    Spark Structured Streaming bronze 61,873 rows, Marquez, Grafana, OpenMetadata 18 tables /
    18 lineage edges / 9 glossary terms; evidence in docs/evidence/.

- 2026-11-13 Review (docs/reviews/m2.md): 1 blocker (publish cache growth -> empty export),
  fixed in e2240f5 (vacuum, bounded cache paths, no empty exports, lazy counts); CI green.
- 2026-11-13 Disk cleanup by the parent session (owner asked; C: was down to 4.7 GB): uv cache
  pruned; Delta VACUUM retention 0 on all tables (3.0 GB freed, no time travel); babel-lag
  Compose containers, volumes (kafka, marquez-db, om-db, om-search, prometheus, ivy) and images
  removed, i.e. **Docker volumes reset 2026-11-13** (Marquez/OpenMetadata/Kafka data wiped;
  evidence screenshots kept). Free disk 11.3 GB; local `data/` 7.5 GB (keep <= ~8 GB:
  lake 4.2, staging 1.7, landing 1.4). Disk budget now ~7.9 GB (70% of free); the owner will
  compact Docker's virtual disk.

- 2026-11-13 Marquez re-check (lineage profile only, then removed): every stage now emits lineage
  for every job it runs; Marquez API shows export_live <- gold <- silver <- bronze <- landing
  batches + Kafka topic <- EventStreams, and export_languages <- gold <- silver <- history,
  pageview and sitelink dumps (docs/evidence/marquez_lineage.json, uc_lineage_*.json).
  **Milestone 2 checkpoint holds; milestone 2 closed 2026-11-13.**

- 2026-11-14 Repo `azzammasood/babel-lag-pipeline` made **public** (owner approved; history
  scanned for the Databricks host and token patterns first, gitleaks green).
- 2026-11-14 Repo variable `BABEL_SCHEDULES=true` (owner approved): publish runs at :07 and :37.
  The publish workflow seeds itself (fetches the slice) when the cache holds no dump gold.

## Next
1. Milestone 3 needs the backfill on Actions, which needs the repo public (ADR-003, owner's call).
   Until then: method (update lag, demand proxy), benchmarks on the slice, README, claims audit.

## Later (owner)
- Review m2 non-blocking notes: quarantine unparseable stream lines; align `pageviews`,
  `late_events`, `events` descriptions with their SQL; `date_range` start format.
- Set `BABEL_EDITOR_SALT` in `.env` and as a repo secret: it is empty, so editor hashes are null
  (privacy-safe; distinct-editor counts unavailable until set).
- Repo variable `BABEL_SCHEDULES=true` once public (ADR-003); rotate the Databricks token.

## Blockers / waiting on owner
- Owner (2026-11-11): identity confirmed (alias github-azzammasood, Keystone email); PowerShell.
- Owner to create empty repos `babel-lag-pipeline` and `babel-lag-site` on azzammasood.
- Databricks resolved 2026-11-11: new account, creds in local `.env` (rotate the token later: it was pasted in chat).
- Disk resolved 2026-11-11: 51.1 GB free, budget 35.8 GB.
- RAM: only 0.8-4.7 GB available during M2 (owner browsers use ~5 GB); Docker profiles one at a time.

## Later (non-blocking)
- `babel clean` (dry run by default): remove data/, .venv, Compose images/volumes at project end
  (owner asked 2026-11-11; owner approves the actual delete).
