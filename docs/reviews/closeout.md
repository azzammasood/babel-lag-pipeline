# Close-out review (2026-11-17)

Fresh subagent, blockers only, over 94c6135..15db523 (pipeline) and the site's report page commit.
Checked: watchdog/publish/fallback-issue bash and YAML, report fails closed on non-free models and
unvalidated prose, report schema identical in both repos, stream quarantine holds no line content,
`babel clean` dry run by default, AI label on the site, no secrets, nothing billable.

## Blockers
None.

## Non-blocking notes (all applied)
1. Numbers in words and ordinal ranking words (first, ranks, ahead of) are now rejected.
2. publish.yml weekly step: a malformed released report.json no longer fails the step (`|| age=999`).
3. method.md says the last report stays published when a weekly run is rejected.
