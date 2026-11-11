"""Dagster asset graph (brief section 5: the orchestration showcase). Each asset calls the same
pipeline step the CLI runs; the DAG holds no logic. Materialize all:

    uv run --group orchestrate dagster asset materialize -m babel_lag.orchestrate.definitions \
        --select '*'
"""

import os

from dagster import AssetExecutionContext, Definitions, MaterializeResult, asset

from babel_lag import pipeline
from babel_lag.profile.run import produced

GROUP = "babel_lag"


@asset(group_name=GROUP, description="Sanitized live micro-batches caught up from EventStreams.")
def live_batches(context: AssetExecutionContext) -> MaterializeResult:
    res = pipeline.collect(float(os.environ.get("BABEL_DAGSTER_COLLECT_S", "120")))
    return MaterializeResult(metadata={"events": res["events"], "batches": len(res["batches"])})


@asset(
    group_name=GROUP,
    deps=[live_batches],
    description="Bronze, silver and gold Delta tables (delta-rs + the core SQL on DuckDB).",
)
def lake_tables(context: AssetExecutionContext) -> MaterializeResult:
    pipeline.ingest_and_rollup(dumps=False)
    return MaterializeResult(metadata={"engine": "duckdb"})


@asset(group_name=GROUP, deps=[lake_tables], description="dbt marts and tests on DuckDB.")
def dbt_marts(context: AssetExecutionContext) -> MaterializeResult:
    pipeline.dbt_build("duckdb")
    return MaterializeResult(metadata={"target": "duckdb"})


@asset(group_name=GROUP, deps=[dbt_marts], description="Soda Core checks on the marts.")
def quality_checks(context: AssetExecutionContext) -> MaterializeResult:
    pipeline.soda_scan()
    return MaterializeResult()


@asset(
    group_name=GROUP,
    deps=[quality_checks],
    description="Versioned JSON export, metadata export and pipeline_metrics.json.",
)
def site_export(context: AssetExecutionContext) -> MaterializeResult:
    pipeline.export(produced("free", False, "requested (dagster)"))
    return MaterializeResult()


defs = Definitions(assets=[live_batches, lake_tables, dbt_marts, quality_checks, site_export])
