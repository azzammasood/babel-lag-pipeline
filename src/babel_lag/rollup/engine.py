"""Core logic written once (`rollup/sql/NN_<table>.sql`), run by DuckDB (`free`) and Spark
(`full`). Two functions differ between the engines and are written as tokens in the SQL:
`EPOCH(ts)` (seconds since 1970) and `FROM_EPOCH(seconds)` (a UTC timestamp).
The parity test runs both engines on the same fixtures and fails on any difference.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pyarrow as pa

SQL_DIR = Path(__file__).parent / "sql"
DIALECT = {
    "duckdb": {"EPOCH": "epoch", "FROM_EPOCH": "to_timestamp"},
    "spark": {"EPOCH": "unix_timestamp", "FROM_EPOCH": "timestamp_seconds"},
}
INPUTS = (
    "bronze_live_events",
    "silver_page_revisions",
    "silver_sitelinks",
    "silver_pageviews_daily",
)


def steps() -> list[tuple[str, Path]]:
    """(table name, sql file) in dependency order."""
    return [(p.stem.split("_", 1)[1], p) for p in sorted(SQL_DIR.glob("[0-9][0-9]_*.sql"))]


def render(sql: str, engine: str) -> str:
    for token, fn in DIALECT[engine].items():
        sql = re.sub(rf"\b{token}\(", f"{fn}(", sql)
    return re.sub(r"^\s*--.*$", "", sql, flags=re.M).strip()


def run_duckdb(inputs: dict[str, Any], only: set[str] | None = None) -> dict[str, pa.Table]:
    """Inputs: name -> Arrow table or dataset (an empty table when a source is missing)."""
    import duckdb

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    for name, data in inputs.items():
        con.register(name, data)
    out: dict[str, pa.Table] = {}
    for table, path in steps():
        if only and table not in only:
            continue
        out[table] = con.sql(render(path.read_text("utf-8"), "duckdb")).to_arrow_table()
        con.register(table, out[table])
    con.close()
    return out


def run_spark(spark: Any, inputs: dict[str, Any], only: set[str] | None = None) -> dict[str, Any]:
    """Same SQL on a SparkSession (session time zone must be UTC). Returns Spark DataFrames."""
    for name, df in inputs.items():
        df.createOrReplaceTempView(name)
    out: dict[str, Any] = {}
    for table, path in steps():
        if only and table not in only:
            continue
        out[table] = spark.sql(render(path.read_text("utf-8"), "spark"))
        out[table].createOrReplaceTempView(table)
    return out
