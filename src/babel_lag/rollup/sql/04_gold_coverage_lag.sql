-- gold_coverage_lag (core metric): for every item with an article in language L, the time from
-- the item's first article in the reference set to L's article. Redirects, deleted pages and
-- bot-created pages are excluded (docs/method.md). Reference set in milestone 2: the slice.
WITH eligible AS (
  SELECT s.item_id, c.wiki, c.created_at
  FROM silver_page_creations c
  JOIN silver_sitelinks s ON s.wiki = c.wiki AND s.page_title = c.page_title
  WHERE NOT c.is_redirect AND NOT c.is_deleted AND NOT c.created_by_bot
),
firsts AS (
  SELECT item_id, min(created_at) AS first_created_at, CAST(count(*) AS BIGINT) AS languages
  FROM eligible
  GROUP BY item_id
)
SELECT e.item_id, e.wiki, e.created_at, f.first_created_at, f.languages,
       CAST(EPOCH(e.created_at) - EPOCH(f.first_created_at) AS BIGINT) AS lag_seconds,
       CASE WHEN e.created_at = f.first_created_at THEN true ELSE false END AS is_first,
       'slice20' AS reference_set
FROM eligible e
JOIN firsts f ON e.item_id = f.item_id
