# Progress

Resume with: "Continue from docs/PROGRESS.md". Current milestone: **2 (live vertical slice)**. Milestone 1 closed 2026-11-11.

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

## Next (milestone 2)
1. Rewrite docs/PLAN.md for milestone 2.
2. SSE collector with checkpoints (resume from last event id / `since`), unique batch names,
   sanitizer; Actions catch-up (`free`) and Kafka + schema registry (`full`, `stream` profile).
3. ~20-language slice of history, pageviews and sitelinks; Delta bronze/silver/gold; DuckDB and
   Spark logic with the parity test; dbt on both targets; contracts, lineage, metrics, exports.
4. Compose profile RAM/disk measured at the first pull.

## Blockers / waiting on owner
- Owner (2026-11-11): identity confirmed (alias github-azzammasood, Keystone email); PowerShell.
- Owner to create empty repos `babel-lag-pipeline` and `babel-lag-site` on azzammasood.
- Databricks resolved 2026-11-11: new account, creds in local `.env` (rotate the token later: it was pasted in chat).
- Disk resolved 2026-11-11: 51.1 GB free, budget 35.8 GB.

## Later (non-blocking)
- `babel clean` (dry run by default): remove data/, .venv, Compose images/volumes at project end
  (owner asked 2026-11-11; owner approves the actual delete).
