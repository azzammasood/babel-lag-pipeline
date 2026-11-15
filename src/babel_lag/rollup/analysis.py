"""Batch analyses that need the slice's per-page daily edit series (local DuckDB only):
`analysis/update_lag.sql` -> Delta `gold_update_lag` + Parquet for the release `data-gold`."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from babel_lag.lake import delta
from babel_lag.paths import DATA

SQL = Path(__file__).parent / "sql" / "analysis" / "update_lag.sql"


def update_lag(out: Path = DATA / "backfill" / "gold") -> int:
    con = duckdb.connect()
    for stmt in ("SET TimeZone = 'UTC'", "SET memory_limit = '2GB'", "SET threads = 4",
                 "SET preserve_insertion_order = false", "SET enable_progress_bar = false",
                 "SET max_temp_directory_size = '4GiB'",
                 f"SET temp_directory = '{(DATA / 'tmp').as_posix()}'"):  # fmt: skip
        con.execute(stmt)
    for name in ("silver_page_revisions", "silver_page_creations", "silver_sitelinks"):
        if not delta.exists(name):
            raise RuntimeError(f"{name} missing: run `babel lake rollup --dumps` first")
        con.register(name, delta.dataset(name))
    table = con.execute(SQL.read_text("utf-8")).to_arrow_table()
    con.close()
    delta._write("gold_update_lag", table)
    out.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out / "gold_update_lag.parquet")
    return table.num_rows
