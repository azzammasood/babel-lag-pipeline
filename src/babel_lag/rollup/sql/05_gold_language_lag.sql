-- gold_language_lag: per language, how many reference items it covers and how late it got them.
-- Median lag over the items the language did not create first, among items in 2+ languages.
SELECT wiki,
       CAST(count(*) AS BIGINT) AS items_covered,
       CAST(sum(CASE WHEN is_first THEN 1 ELSE 0 END) AS BIGINT) AS items_first,
       CAST(sum(CASE WHEN languages >= 2 AND NOT is_first THEN 1 ELSE 0 END) AS BIGINT) AS items_followed,
       CAST(median(CASE WHEN languages >= 2 AND NOT is_first THEN lag_seconds END) AS DOUBLE)
         / 86400.0
         AS median_lag_days,
       (SELECT CAST(count(DISTINCT item_id) AS BIGINT) FROM gold_coverage_lag) AS items_in_reference
FROM gold_coverage_lag
GROUP BY wiki
