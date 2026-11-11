-- silver_page_creations: one row per article (namespace 0) with its first revision, from the
-- history dump. Creation time = timestamp of the earliest revision still in the history.
WITH firsts AS (
  SELECT wiki, page_id, page_title, rev_ts, is_bot, page_is_redirect, page_is_deleted,
         row_number() OVER (PARTITION BY wiki, page_id ORDER BY rev_ts, rev_id) AS rn
  FROM silver_page_revisions
),
agg AS (
  SELECT wiki, page_id, CAST(count(*) AS BIGINT) AS revisions,
         CAST(sum(CASE WHEN is_bot THEN 0 ELSE 1 END) AS BIGINT) AS human_revisions,
         CAST(sum(CASE WHEN is_reverted THEN 1 ELSE 0 END) AS BIGINT) AS reverted_revisions,
         max(rev_ts) AS last_rev_ts
  FROM silver_page_revisions
  GROUP BY wiki, page_id
)
SELECT f.wiki, f.page_id, f.page_title, f.rev_ts AS created_at, f.is_bot AS created_by_bot,
       f.page_is_redirect AS is_redirect, f.page_is_deleted AS is_deleted, a.revisions,
       a.human_revisions, a.reverted_revisions, a.last_rev_ts
FROM firsts f
JOIN agg a ON f.wiki = a.wiki AND f.page_id = a.page_id
WHERE f.rn = 1
