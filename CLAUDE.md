# Babel Lag pipeline: working notes for Claude

Question: when the world learns something new, how long until it reaches each language on
Wikipedia, and which languages stay in the dark? Third project after Roshni and Keystone; reuse
Keystone's patterns (`../keystone`, the closest template: profiles, Databricks rung, Kafka, lineage,
export envelope, metrics contract). Backend here; the site (`../babel-lag-site`) is milestone 4.
Long facts go in `docs/` (`spike-report.md`, `data-audit.md`, `method.md`, `benchmarks.md`).
Full brief (local only, git-ignored): `docs/BRIEF.md`. Plan: `docs/PLAN.md`. Decisions: `docs/DECISIONS.md` (ADRs). Status and resume point:
`docs/PROGRESS.md` ("Continue from docs/PROGRESS.md").

## Milestones (checkpoints auto-advance; one quick blocker-only review per milestone)
1. Foundation and feasibility: repo, CI, spike measurements, Databricks Volume test, backfill window.
2. Live vertical slice, fully observable (all wikis live; ~20 languages history and pageviews).
3. Scale-out, full method, backtests, benchmarks, AI report, README, claims audit.
4. Site (separate repo), behind a design gate (six concepts, four logos, preview.html).
Review: a fresh subagent looks only for blockers (wrong numbers, privacy leaks, secrets, broken
schedule, missing attribution); write `docs/reviews/<milestone>.md` in a few lines.

## Hard rules
- **Privacy:** drop editor usernames and IP addresses at ingestion. Where a distinct-editor count is
  needed keep only a salted hash (salt `BABEL_EDITOR_SALT`, env only). Never publish, rank or profile
  editors. Aggregates only. A test proves no username or IP reaches silver, gold or any export.
- **Wikimedia etiquette:** UA from `config/policy.yml` with contact (`BABEL_CONTACT_EMAIL`, env);
  at most 2 concurrent bulk downloads; prefer mirrors; one live stream connection per profile;
  reconnect with backoff; respect `robots.txt`. Attribute Wikimedia and the CC0 datasets.
- **Bots and vandalism:** exclude bot edits (`bot` flag + known bot lists) and reverted edits from
  lag measures; rules documented in `docs/method.md`.
- **No over-claiming:** lag is about Wikipedia only. Methods and limits are public.
- **Never invent data or numbers.** Unparseable records go to quarantine with a reason. Rates are
  measured; anything estimated is labelled. Measured spike numbers are regression oracles.
- **Money:** $0 only. Any possible charge is a hard stop. Tokens via env only. Non-commercial.
- **Tool credibility:** a tool appears in README/site only if it ran, with evidence (`engines[]`).
- All timestamps UTC. Bulk data in `data/` (git-ignored); stream and delete; stay in the disk budget
  (`babel machine`).

## Hard stops (ask the owner; otherwise decide and log an ADR)
Owner-only actions (repos, accounts, tokens, Docker/admin installs, freeing disk); any spend;
GitHub identity ambiguity or anything under **azzamverse**; making a repo public or publishing the
site; destructive actions on the owner's files or accounts; privacy/legal red flags; the frontend
design choice; an unmeetable requirement. If a stop blocks one part, skip it, log it, keep going.
Decision order: $0 > site updates forever > portfolio value > reliability > least effort.

## Repo and git safety
- Remote: `git@github-azzammasood:azzammasood/babel-lag-pipeline.git` (SSH alias, same as Roshni
  and Keystone). The owner creates the repos. Never `gh repo create`; never change visibility;
  never force-push without asking. Private until the owner says.
- `gh`: the owner switched the active account to azzammasood (2026-11-11) and had the
  Databricks repo secrets set with it. Before any `gh` use check `gh api user --jq .login` prints
  `azzammasood`; stop if not. Read-only calls (runs, logs) are fine; ask before writes.
- Commit identity set for this repo only. Never touch global git config.
- Small conventional commits (`feat:`, `fix:`, `build:`, `ci:`, `docs:`, `test:`, `chore:`).

## Speed mode: lean tests only
One small fixture test per parser; the `free` vs `full` parity test for the core lag metric; JSON
Schema validation of every export; one privacy test (no username/IP downstream of bronze); one
end-to-end smoke. Nothing else: no coverage targets, no mypy gate, no property tests. Validate by
running on real data and checking counts against the spike. Time-box integrations; log gaps.
Write metadata (descriptions, owners, tags) as you build.

## Owner is on a limited usage plan
Never print large files or tables into the conversation; don't re-read inspected files; keep
PROGRESS.md current at every checkpoint and before long runs.

## Commands (PowerShell or Git Bash)
```
uv sync ; uv run pre-commit install       # Python 3.12 + git hooks
uv run babel --help
uv run babel machine [--json]             # disk, RAM, CPUs, Docker, disk budget
uv run pytest                             # network tests skip unless BABEL_NETWORK_TESTS=1
uv run ruff check . ; uv run ruff format .
uv run python scripts/spike/<probe>.py    # milestone 1 measurements -> docs/spike-report.md
```

## Layout
```
src/babel_lag/   cli.py (Typer, `babel`), paths.py, policy.py
                 planned: stream (SSE collector), dumps (history, pageviews, wikidata),
                 parsers, lake (Delta), rollup (sql once; duck + spark), warehouse (dbt),
                 lineage, telemetry, export, profile, cloud (databricks), narrate
config/          policy.yml (UA, limits, disk budget)
scripts/spike/   one-off feasibility probes (milestone 1)
tests/           tests/fixtures/ (small, committed)
docs/            PLAN, PROGRESS, DECISIONS, spike-report, reviews/
data/            generated, git-ignored
```

## Conventions
- Python 3.12, uv, ruff (line 100), pytest, pathlib only, cross-platform scripts.
- Parsers return pydantic models -> Parquet/Delta + quarantine; source tables keep `source_file`,
  `source_sha256` (dumps) or `event_id` (stream), and `fetched_at`.
- Core logic written once in portable SQL; parity tests between engines fail CI on any diff.
- dbt SQL stays portable (DuckDB and Databricks). >= 95% models and columns documented (CI gate).
- GitHub Actions = production; Dagster and Databricks Jobs = showcase calling the CLI and dbt.
- Compose profiles `stream`, `lineage`, `catalog`, `obs`: start only what's needed; ~16 GB RAM total.
