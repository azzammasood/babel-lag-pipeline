-- gold_live_activity: 15-minute event-time windows per wiki. Late events (silver flag) are
-- counted apart and kept out of the activity measures. Human = not flagged as a bot.
SELECT FROM_EPOCH(CAST(floor(EPOCH(event_dt) / 900) AS BIGINT) * 900) AS window_start,
       wiki,
       CAST(sum(CASE WHEN NOT is_late THEN 1 ELSE 0 END) AS BIGINT) AS events,
       CAST(sum(CASE WHEN is_late THEN 1 ELSE 0 END) AS BIGINT) AS late_events,
       CAST(sum(CASE WHEN NOT is_late AND coalesce(is_bot, false) THEN 1 ELSE 0 END) AS BIGINT) AS bot_events,
       CAST(sum(CASE WHEN NOT is_late AND NOT coalesce(is_bot, false) AND namespace = 0
                 AND event_type IN ('edit', 'new') THEN 1 ELSE 0 END) AS BIGINT) AS human_article_edits,
       CAST(sum(CASE WHEN NOT is_late AND NOT coalesce(is_bot, false) AND namespace = 0
                 AND event_type = 'page-create' AND NOT coalesce(is_redirect, false)
            THEN 1 ELSE 0 END) AS BIGINT) AS new_articles
FROM silver_live_events
GROUP BY FROM_EPOCH(CAST(floor(EPOCH(event_dt) / 900) AS BIGINT) * 900), wiki
