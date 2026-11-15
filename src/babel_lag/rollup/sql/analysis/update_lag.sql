-- gold_update_lag (docs/method.md "Update lag"): needs per-page daily edit series, which exist for
-- the 20-language slice only (silver_page_revisions), so reference_set is slice20.
-- Burst: a day with >= 5 human, non-reverted edits and >= 3x the page's median active-day edits
-- over the two years 2024-11-01..2026-10-31 (the baseline window).
-- Event: an item's first burst in any language between 2025-11-01 and 2026-10-31 (the last 12
-- months of the 2026-10 snapshot). Update lag of language L: days from the event start to L's
-- first burst within 30 days, for languages whose article existed before the event. Per
-- language: events it led (burst on the start day), events it followed (median lag over those),
-- and the share of events it updated within 30 days at all.
WITH daily AS (
  SELECT wiki, page_id, CAST(rev_ts AS DATE) AS day, count(*) AS edits
  FROM silver_page_revisions
  WHERE NOT is_bot AND NOT is_reverted
    AND rev_ts >= TIMESTAMPTZ '2024-11-01' AND rev_ts < TIMESTAMPTZ '2026-11-01'
  GROUP BY wiki, page_id, CAST(rev_ts AS DATE)
),
base AS (
  SELECT wiki, page_id, median(edits) AS med FROM daily GROUP BY wiki, page_id
),
bursts AS (
  SELECT d.wiki, d.page_id, d.day
  FROM daily d JOIN base b ON b.wiki = d.wiki AND b.page_id = d.page_id
  WHERE d.edits >= greatest(5, 3 * b.med)
    AND d.day >= DATE '2025-11-01' AND d.day < DATE '2026-11-01'
),
items AS (
  SELECT s.item_id, c.wiki, c.page_id, CAST(c.created_at AS DATE) AS created_day
  FROM silver_page_creations c
  JOIN silver_sitelinks s ON s.wiki = c.wiki AND s.page_title = c.page_title
  WHERE NOT c.is_redirect AND NOT c.is_deleted AND NOT c.created_by_bot
),
multi AS (SELECT item_id FROM items GROUP BY item_id HAVING count(*) >= 2),
item_bursts AS (
  SELECT i.item_id, i.wiki, b.day
  FROM bursts b JOIN items i ON i.wiki = b.wiki AND i.page_id = b.page_id
  WHERE i.item_id IN (SELECT item_id FROM multi)
),
events AS (SELECT item_id, min(day) AS t0 FROM item_bursts GROUP BY item_id),
per_lang AS (
  SELECT i.item_id, i.wiki, e.t0,
         min(CASE WHEN ib.day BETWEEN e.t0 AND e.t0 + 30 THEN ib.day END) AS first_burst
  FROM items i
  JOIN events e ON e.item_id = i.item_id
  LEFT JOIN item_bursts ib ON ib.item_id = i.item_id AND ib.wiki = i.wiki
  WHERE i.created_day < e.t0
  GROUP BY i.item_id, i.wiki, e.t0
)
SELECT wiki,
       CAST(count(*) AS BIGINT) AS items_with_event,
       CAST(count(CASE WHEN first_burst = t0 THEN 1 END) AS BIGINT) AS items_first,
       CAST(count(CASE WHEN first_burst > t0 THEN 1 END) AS BIGINT) AS items_followed,
       round(100.0 * count(first_burst) / count(*), 2) AS updated_pct,
       CAST(median(CASE WHEN first_burst > t0
                        THEN datediff('day', t0, first_burst) END) AS DOUBLE)
         AS median_follow_lag_days,
       'slice20' AS reference_set
FROM per_lang
GROUP BY wiki
