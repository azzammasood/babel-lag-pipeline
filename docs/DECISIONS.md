# Decisions (ADRs)

Newest first. Each: context, choice, why.

## ADR-004 (2026-11-11) Bulk work runs on Actions runners; the local disk is for samples and Docker
- Context: `babel machine` measured 25.9 GB free on C: (budget 70% = 18.1 GB), below the brief's
  50 GB threshold. Docker Desktop is installed but was not running; RAM 16.8 GB total.
- Choice: the history and pageview backfill run as resumable chunk jobs on GitHub-hosted runners
  that stream and delete (Keystone's pattern); local runs use samples only. Docker images for the
  showcase profiles count against the 18.1 GB budget and are measured before pulling.
- Why: $0, bounded local disk, proven in Keystone. Flagged to the owner (more disk widens options).

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
