-- The published per-language table (export `languages`). Coverage is relative to the reference
-- set (slice20 in milestone 2); pageviews are human views of covered items over the slice days.
with lag as (
    select * from {{ lake('gold_language_lag') }}
),
views as (
    select wiki, cast(sum(views) as bigint) as pageviews
    from {{ lake('gold_item_pageviews') }}
    group by wiki
)
select
    lag.wiki,
    replace(lag.wiki, 'wiki', '') as language,
    lag.items_covered,
    lag.items_first,
    round(100.0 * lag.items_covered / lag.items_in_reference, 2) as coverage_pct,
    round(lag.median_lag_days, 1) as median_lag_days,
    coalesce(views.pageviews, 0) as pageviews
from lag
left join views on views.wiki = lag.wiki
