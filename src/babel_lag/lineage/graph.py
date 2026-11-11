"""The lineage graph: every job with its input and output datasets, plus column lineage from live
event and dump fields to published export fields (brief section 5b).

One declaration feeds the OpenLineage events (Marquez), the OpenMetadata lineage edges, and the
static `metadata/lineage.json` the site renders. Dataset metadata lives in config/datasets.yml.
"""

from __future__ import annotations

from functools import cache
from typing import Any

import yaml

from babel_lag.paths import CONFIG

# job -> (inputs, outputs)
JOBS: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "collect_live": (("eventstreams",), ("landing_live_batches", "kafka_live_events")),
    "ingest_bronze_live": (("landing_live_batches", "kafka_live_events"), ("bronze_live_events",)),
    "fetch_history": (("mediawiki_history_dump",), ("silver_page_revisions",)),
    "fetch_sitelinks": (("wikidata_sitelinks_dump",), ("silver_sitelinks",)),
    "fetch_pageviews": (("pageview_complete_dump",), ("silver_pageviews_daily",)),
    "rollup_silver_live_events": (("bronze_live_events",), ("silver_live_events",)),
    "rollup_gold_live_activity": (("silver_live_events",), ("gold_live_activity",)),
    "rollup_silver_page_creations": (("silver_page_revisions",), ("silver_page_creations",)),
    "rollup_gold_coverage_lag": (
        ("silver_page_creations", "silver_sitelinks"),
        ("gold_coverage_lag",),
    ),
    "rollup_gold_language_lag": (("gold_coverage_lag",), ("gold_language_lag",)),
    "rollup_gold_item_pageviews": (
        ("silver_pageviews_daily", "silver_sitelinks"),
        ("gold_item_pageviews",),
    ),
    "export_languages": (("gold_language_lag", "gold_item_pageviews"), ("export_languages",)),
    "export_live": (("gold_live_activity",), ("export_live",)),
}

# output dataset -> {output column: [(input dataset, input column), ...]}
COLUMNS: dict[str, dict[str, list[tuple[str, str]]]] = {
    "bronze_live_events": {
        "event_id": [("landing_live_batches", "meta.id")],
        "event_dt": [("landing_live_batches", "meta.dt")],
        "wiki": [("landing_live_batches", "wiki")],
        "is_bot": [("landing_live_batches", "bot")],
    },
    "landing_live_batches": {
        "meta.id": [("eventstreams", "meta.id")],
        "meta.dt": [("eventstreams", "meta.dt")],
        "wiki": [("eventstreams", "wiki")],
        "bot": [("eventstreams", "bot")],
    },
    "silver_live_events": {c: [("bronze_live_events", c)] for c in ("event_id", "event_dt", "wiki")}
    | {"is_late": [("bronze_live_events", "event_dt"), ("bronze_live_events", "ingested_at")]},
    "gold_live_activity": {
        "window_start": [("silver_live_events", "event_dt")],
        "wiki": [("silver_live_events", "wiki")],
        "events": [("silver_live_events", "event_id"), ("silver_live_events", "is_late")],
        "late_events": [("silver_live_events", "is_late")],
        "bot_events": [("silver_live_events", "is_bot")],
        "human_article_edits": [("silver_live_events", "event_type")],
        "new_articles": [("silver_live_events", "event_type")],
    },
    "export_live": {
        "hour": [("gold_live_activity", "window_start")],
        "wiki": [("gold_live_activity", "wiki")],
        "events": [("gold_live_activity", "events")],
        "late_events": [("gold_live_activity", "late_events")],
        "human_article_edits": [("gold_live_activity", "human_article_edits")],
        "new_articles": [("gold_live_activity", "new_articles")],
        "bot_events": [("gold_live_activity", "bot_events")],
    },
    "silver_page_creations": {
        "created_at": [("silver_page_revisions", "rev_ts")],
        "created_by_bot": [("silver_page_revisions", "is_bot")],
    },
    "gold_coverage_lag": {
        "item_id": [("silver_sitelinks", "item_id")],
        "lag_seconds": [("silver_page_creations", "created_at")],
    },
    "gold_language_lag": {
        "median_lag_days": [("gold_coverage_lag", "lag_seconds")],
        "items_covered": [("gold_coverage_lag", "item_id")],
    },
    "export_languages": {
        "median_lag_days": [("gold_language_lag", "median_lag_days")],
        "items_covered": [("gold_language_lag", "items_covered")],
        "coverage_pct": [
            ("gold_language_lag", "items_covered"),
            ("gold_language_lag", "items_in_reference"),
        ],
        "pageviews": [("gold_item_pageviews", "views")],
    },
}


@cache
def datasets() -> dict[str, dict[str, Any]]:
    raw = yaml.safe_load((CONFIG / "datasets.yml").read_text("utf-8"))
    defaults = raw["defaults"]
    return {d["name"]: {**defaults, **d} for d in raw["datasets"]}


def edges() -> list[tuple[str, str, str]]:
    """(input dataset, job, output dataset) triples."""
    return [(i, job, o) for job, (ins, outs) in JOBS.items() for i in ins for o in outs]
