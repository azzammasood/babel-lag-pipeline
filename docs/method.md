# Method (milestone 2 state; completed in milestone 3)

Lag is measured on Wikipedia only and says nothing about other media in a language.

## Units
- **Item**: a Wikidata item with at least one Wikipedia sitelink (`wb_items_per_site`).
- **Article**: a namespace-0 page of a Wikipedia, joined to its item through (wiki, title with
  underscores). Titles of the history dump are current titles.

## Coverage lag (core metric, `rollup/sql/04_gold_coverage_lag.sql`)
- Creation time of an article = timestamp of its earliest revision in the history dump.
- Excluded: redirects (current `page_is_redirect`), deleted pages, and pages whose first revision
  was made by a bot (bot-created stubs). Bots = `event_user_is_bot_by` non-empty (group or name).
- For each item, the reference start is the earliest eligible creation across the reference set;
  lag for language L = L's creation minus that start. Ties: every tied language counts as first.
- Reference set: the 20-language slice in milestone 2 (ADR-007); all Wikipedias from milestone 3.
- Per language: items covered, items first, and the median lag over items in 2+ languages that L
  covered after another language (`05_gold_language_lag.sql`).

## Demand (`06_gold_item_pageviews.sql`)
- Human pageviews (`user` agent files of pageview_complete), access types summed, per item,
  language and day. Views of redirect titles are not attributed (limit). The demand-without-supply
  proxy is defined in milestone 3.

## Live layer (`01_silver_live_events.sql`, `02_gold_live_activity.sql`)
- Events from `recentchange`, `revision-create` and `page-create` for every wiki, sanitized at
  ingestion (no usernames, IPs, comments; titles only in namespace 0 and never for log events).
- Duplicates dropped on the event id (earliest arrival). Late events: older than the per-stream
  watermark of earlier batches minus 600 s (ADR-011); counted apart.
- 15-minute event-time windows: on-time events, bot events (`bot` flag), human article edits
  (recentchange `edit`/`new` in namespace 0, not bots), new articles (page-create, namespace 0,
  not bots, not redirects). Reverted edits are excluded from lag measures that use revisions
  (history `revision_is_identity_reverted`); the live layer has no revert signal yet.

## Privacy
Editor names and IPs are dropped at ingestion; a salted hash (`BABEL_EDITOR_SALT`) exists only
for distinct counts and is never exported. Exports are aggregates by language, item, wiki and time.
A test runs the pipeline on events carrying names, IPs and comments and scans every silver, gold
and export file for them.
