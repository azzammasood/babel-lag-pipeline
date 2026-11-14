-- The published per-language table (export `languages`). Coverage is relative to the reference
-- set (slice20 or all377); pageviews and the demand proxy come from gold_language_demand.
with lag as (
    select * from {{ lake('gold_language_lag') }}
),
demand as (
    select * from {{ lake('gold_language_demand') }}
)
select
    lag.wiki,
    replace(lag.wiki, 'wiki', '') as language,
    lag.items_covered,
    lag.items_first,
    round(100.0 * lag.items_covered / lag.items_in_reference, 2) as coverage_pct,
    round(lag.median_lag_days, 1) as median_lag_days,
    round(lag.median_depth_ratio, 3) as median_depth_ratio,
    coalesce(demand.pageviews, 0) as pageviews,
    coalesce(demand.demand_without_supply, 0) as demand_without_supply,
    lag.reference_set
from lag
left join demand on demand.wiki = lag.wiki
