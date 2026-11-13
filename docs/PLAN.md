# Milestone 3 plan: scale-out, method, polish

Goal (brief section 7): backfill every Wikipedia for the ADR-006 window, the full Babel Lag
method with live topic tracking and backtests, the scale ladder, the weekly AI report, README and
a claims audit. Checkpoint: blocker review clean, claims audit passed, all exports validate.

## Gate (owner)
The backfill (377 Wikipedias, ~355 GB history, pageviews from 2015-07) runs as resumable chunk
jobs on GitHub-hosted runners (ADR-004, ADR-006), which needs unlimited Actions minutes, i.e. a
**public repo** (ADR-003). Going public is the owner's call. Local data stays <= ~8 GB.

## 3a: work that does not need the backfill (start now)
- Method: update lag (burst rule with published parameters), demand-without-supply proxy,
  coverage depth; `docs/method.md` complete; review m2 notes (descriptions vs SQL, quarantine
  unparseable stream lines, `date_range` format).
- Scale ladder on the slice (`docs/benchmarks.md`): DuckDB, local Spark (Docker, one profile),
  Databricks serverless within quota; rows, bytes, wall time, peak memory, cost, quota.
- Live topic tracking (new items and bursts spreading across languages; Wikidata API at 1 rps).
- README (question, sources, method, limits, privacy, architecture, "what produced the data").

## 3b: backfill (after the repo is public)
- `backfill.yml`: chunk jobs (2 concurrent, umu.se mirror, lbzip2, stream and delete), each
  writing compact per-wiki facts to release assets; exact counts and bytes recorded.
- Reference set widens from `slice20` to all Wikipedias; backtests with published criteria.

## 3c: close
- Weekly AI "Babel report" (OpenRouter, prose only, numbers validated, cached, spend cap $0).
- Claims audit (`docs/claims-audit.md`), blocker review `docs/reviews/m3.md`, CI green, push.
