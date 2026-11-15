# Method

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

- Reference set: `slice20` (milestone 2, 20 languages) or `all` (every Wikipedia, the
  backfill); every published row carries `reference_set`.

## Coverage depth (`04`, `05`)
- For each item and language: the current article size (bytes of the latest revision) divided by
  the size of the item's first article. Per language: the median over items it covered after
  another language. A ratio below 1 means shorter articles than the first language's.

## Demand (`06_gold_item_pageviews.sql`, `07_gold_language_demand.sql`)
- Human pageviews (`user` agent files of pageview_complete), access types summed, per item,
  language and day (1 day in the slice, 7 days in the backfill assemble). Views of redirect
  titles are not attributed (limit).
- **Demand without supply (proxy):** for language L, the views, in all languages, of reference
  items L has no eligible article for. It measures interest in topics L does not cover, not
  searches made in L (Wikimedia publishes no per-language search logs). Published next to
  `reference_views`, the views of all reference items.

## Backtests (`backfill.assemble`, all)
Criteria, fixed before looking at results: the 20 items whose first eligible article was created
between 2020-01-01 and 2024-12-31 (UTC) and that have the most languages today (ties by item id);
for each, every language's creation lag. Published as `gold_backtests`.

## Update lag (`rollup/sql/analysis/update_lag.sql`, 20-language slice)
Fixed before looking at results:
- **Daily series:** human (not bot), non-reverted article edits per page and day, 2024-11-01..2026-10-31.
- **Burst:** a day with at least 5 such edits and at least 3x the page's median active-day count
  in that window.
- **Event:** a multi-language item's first burst in any language in 2025-11-01..2026-10-31.
- **Per language L** (only items whose L article predates the event): the event is *led* when L
  bursts on its start day, *followed* when L bursts within 30 days after it, otherwise not
  updated. Published per language: events, led, followed, the share updated within 30 days, and
  the median follow lag in days over followed events.
- **Limit:** daily series exist only for the 20 slice languages (the all-language backfill keeps
  page-level facts), so `reference_set = slice20`. Follow counts are small: read each median with
  its `items_followed`.

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
