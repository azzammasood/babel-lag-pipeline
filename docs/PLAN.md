# Milestone 1 plan: foundation and feasibility

Goal: a green CI on a clean repo, every section 3 number measured with tiny downloads, the
Databricks ingestion path proven (or ruled out), the backfill window chosen by rule, decisions logged.

## Checkpoint 1a: repo and CI (done in session 1 unless blocked on identity)
- uv project, `babel` CLI (`machine`), policy.yml, pre-commit (ruff, gitleaks, block data/),
  CI (ruff, pytest, gitleaks), Dependabot, MIT, `.gitignore`/`.gitattributes`.
- CLAUDE.md, PLAN, DECISIONS, PROGRESS. First commit; prove nothing large or secret is tracked.
- Push to `azzammasood/babel-lag-pipeline` once the owner has created it; CI green.

## Checkpoint 1b: spike measurements (`scripts/spike/`, results in `docs/spike-report.md`)
Each probe is time-boxed (minutes, not hours) and writes a small JSON under `data/spike/`.
1. **Live rate:** connect once to `recentchange,revision-create,page-create` for ~10 minutes;
   events/s by stream and by wiki (top 30 + long tail), bot share, payload bytes/event,
   reconnects observed. Confirm which fields carry usernames/IPs.
2. **`since` depth:** binary-search the oldest `since` timestamp that still returns events
   (probe 1, 3, 7, 14, 21, 31 days back; read first event only).
3. **History snapshot size:** list the latest `mediawiki_history` snapshot directory (mirror
   first), sum file sizes per wiki and total; download one small wiki's file to measure rows/GB
   and parse speed.
4. **Pageviews complete:** size per day (list sizes for ~30 days across 2015..2026), download one
   day to measure rows and the share needed for ~20 languages.
5. **Wikidata sitelinks:** compare candidate sources (`wb_items_per_site` SQL dump,
   per-wiki `page_props` `wikibase_item`, the JSON dump); pick the lightest correct one, record
   size and coverage (items with >= 1 Wikipedia sitelink, languages).
6. **Throughput:** time a ~200 MB download from 2 mirrors and dumps.wikimedia.org.
7. **Databricks Free Edition:** only if `DATABRICKS_HOST`/`DATABRICKS_TOKEN` exist: upload a file
   to a Unity Catalog Volume from this machine (Files API), create a tiny Lakeflow pipeline with
   Auto Loader, trigger it, record timings and quota behaviour. Repeat from Actions once pushed.
8. **Actions limits:** runner disk/RAM/CPU from Keystone's measured runs plus one probe job;
   minutes budget for private vs public repos.
9. **Compose RAM/disk:** image sizes and idle RAM per profile (`stream`, `lineage`, `catalog`,
   `obs`) measured when Docker is running; decide which profiles can run together.

## Checkpoint 1c: decisions and oracles
- Backfill rule (brief section 3): largest window and language set at $0 with unattended,
  resumable wall time <= 7 days; minimum all languages, pageviews 2015+, full creation history.
  Compute from 1b numbers; ADR.
- Freeze measured numbers in `tests/fixtures/oracles/spike.json` (ranges, not exact values).
- Milestone review (subagent, blockers only) -> `docs/reviews/m1.md`; PROGRESS updated.

## Known constraints going in (see DECISIONS)
- Local disk: 25.9 GB free, budget 18.1 GB (< 50 GB: flagged to owner). Bulk backfill runs on
  Actions runners, streaming and deleting, as Keystone did (1.9 TB over 38 job hours).
- Actions minutes: private repo on a free plan has 2,000 min/month; a 15-30 minute schedule and
  the backfill need the repo public (unlimited minutes for public repos).
- Keystone's Free Edition workspace currently refuses compute (ADR-087 there).
