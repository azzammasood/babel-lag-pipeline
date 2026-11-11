-- gold_item_pageviews: human (`user` agent) pageviews per item, language and day for the slice
-- days. Views of redirect titles are not attributed (docs/method.md, limits).
SELECT p.wiki, s.item_id, p.day, CAST(sum(p.views) AS BIGINT) AS views
FROM silver_pageviews_daily p
JOIN silver_sitelinks s ON s.wiki = p.wiki AND s.page_title = p.page_title
GROUP BY p.wiki, s.item_id, p.day
