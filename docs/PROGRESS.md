# Progress

Resume with: "Continue from docs/PROGRESS.md". Current milestone: **1 (foundation and feasibility)**.

## Done
- 2026-11-11 Checkpoint 1a scaffold: uv project, `babel machine`, policy.yml, pre-commit, CI,
  Dependabot, MIT, CLAUDE.md, PLAN, DECISIONS (ADR-001..004). Local lint and tests pass.

## Measured
- Machine (2026-11-11): 25.9 GB free disk, budget 18.1 GB; 16.8 GB RAM; 8 logical CPUs;
  Docker installed, daemon not running.

## Next
1. First commit with the confirmed identity; prove nothing large or secret is tracked.
2. Add remote and push once `azzammasood/babel-lag-pipeline` exists; CI green.
3. Checkpoint 1b spike probes (docs/PLAN.md), starting with live rate and `since` depth.

## Blockers / waiting on owner
- Owner (2026-11-11): identity confirmed (alias github-azzammasood, Keystone email); PowerShell.
- Owner to create empty repos `babel-lag-pipeline` and `babel-lag-site` on azzammasood.
- Owner to create a new Databricks Free Edition account; DATABRICKS_HOST/DATABRICKS_TOKEN in `.env`.
- Disk resolved 2026-11-11: 51.1 GB free, budget 35.8 GB.

## Later (non-blocking)
- `babel clean` (dry run by default): remove data/, .venv, Compose images/volumes at project end
  (owner asked 2026-11-11; owner approves the actual delete).
