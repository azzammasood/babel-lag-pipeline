-- gold_language_demand: demand without supply (proxy, docs/method.md). For language L: human
-- pageviews of L's own articles, and the views (in every language) of reference items L has no
-- article for, i.e. readers' interest in topics that L does not cover. A proxy: it counts interest
-- elsewhere, not searches in L.
WITH item_views AS (
  SELECT item_id, CAST(sum(views) AS BIGINT) AS views
  FROM gold_item_pageviews
  WHERE item_id IN (SELECT item_id FROM gold_coverage_lag)  -- reference items only
  GROUP BY item_id
),
covered AS (
  SELECT c.wiki, CAST(sum(coalesce(v.views, 0)) AS BIGINT) AS covered_views
  FROM gold_coverage_lag c
  LEFT JOIN item_views v ON v.item_id = c.item_id
  GROUP BY c.wiki
),
own AS (
  SELECT wiki, CAST(sum(views) AS BIGINT) AS own_views FROM gold_item_pageviews GROUP BY wiki
)
SELECT c.wiki,
       CAST(coalesce(o.own_views, 0) AS BIGINT) AS pageviews,
       CAST((SELECT sum(views) FROM item_views) - c.covered_views AS BIGINT)
         AS demand_without_supply,
       CAST((SELECT sum(views) FROM item_views) AS BIGINT) AS reference_views
FROM covered c
LEFT JOIN own o ON o.wiki = c.wiki
