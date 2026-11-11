-- The published live table (export `live`): hourly activity per wiki over the last 48 hours of
-- event time seen, from the 15-minute windows.
with w as (
    select * from {{ lake('gold_live_activity') }}
),
edge as (
    select max(window_start) - interval 48 hour as since from w
)
select
    date_trunc('hour', w.window_start) as hour,
    w.wiki,
    cast(sum(w.events) as bigint) as events,
    cast(sum(w.late_events) as bigint) as late_events,
    cast(sum(w.human_article_edits) as bigint) as human_article_edits,
    cast(sum(w.new_articles) as bigint) as new_articles,
    cast(sum(w.bot_events) as bigint) as bot_events
from w
cross join edge
where w.window_start >= edge.since
group by date_trunc('hour', w.window_start), w.wiki
