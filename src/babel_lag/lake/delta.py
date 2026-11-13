"""Delta Lake tables written by delta-rs (`free` profile): bronze, silver and gold under data/lake.

- `bronze_live_events`: append-only; each landing micro-batch is loaded once (its file name is
  the idempotency key, ADR-005) and flattened to the shared bronze shape (`stream/normalize.py`).
  Bronze keeps `BRONZE_RETENTION` of event time; older rows are deleted and vacuumed.
- `silver_page_revisions`, `silver_sitelinks`, `silver_pageviews_daily`: replaced from the slice
  staging Parquet (dumps are whole snapshots, never increments).
- Every table produced by `rollup/sql` is overwritten from the core SQL run on DuckDB, except
  `gold_live_activity`, which keeps windows older than the bronze retention from the previous run.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from babel_lag.paths import DATA, LAKE, LANDING
from babel_lag.rollup import engine
from babel_lag.stream.normalize import BRONZE_SCHEMA, flatten

BRONZE_RETENTION = timedelta(hours=48)
LAYER = {"bronze": "bronze", "silver": "silver", "gold": "gold"}
DESCRIPTIONS = {
    "bronze_live_events": "Sanitized live EventStreams events, one row per batch line.",
    "silver_live_events": "Live events de-duplicated on event id, with a late flag.",
    "gold_live_activity": "Live activity per wiki in 15-minute event-time windows.",
    "silver_page_revisions": "Article-namespace revisions from the history dump (slice).",
    "silver_sitelinks": "Wikidata item to Wikipedia article links (slice sites).",
    "silver_pageviews_daily": "Human pageviews per article and day (slice).",
    "silver_page_creations": "One row per article with its first revision.",
    "gold_coverage_lag": "Coverage lag per item and language (core metric).",
    "gold_language_lag": "Coverage and median coverage lag per language.",
    "gold_item_pageviews": "Human pageviews per item, language and day.",
}


def path_of(table: str) -> Path:
    return LAKE / table.split("_", 1)[0] / table


def exists(table: str) -> bool:
    return (path_of(table) / "_delta_log").exists()


def dataset(table: str):  # noqa: ANN201 - a pyarrow dataset: scanned lazily (bounded memory)
    return DeltaTable(str(path_of(table))).to_pyarrow_dataset()


def read(table: str) -> pa.Table:
    return DeltaTable(str(path_of(table))).to_pyarrow_table()


def _write(table: str, data: pa.Table, mode: str = "overwrite", partition: list[str] | None = None):
    write_deltalake(
        str(path_of(table)),
        data,
        mode=mode,
        partition_by=partition,
        schema_mode="overwrite" if mode == "overwrite" else None,
        description=DESCRIPTIONS.get(table),
        configuration={"delta.appendOnly": "false"},
    )
    if mode == "overwrite":  # old versions are never read: keep the lake (and the cache) bounded
        DeltaTable(str(path_of(table))).vacuum(
            retention_hours=0, enforce_retention_duration=False, dry_run=False
        )


def ingest_landing(landing: Path = LANDING / "live") -> dict[str, Any]:
    """Load new micro-batches into bronze; returns counts for the run log."""
    done: set[str] = set()
    if exists("bronze_live_events"):
        con = duckdb.connect()
        con.register("b", DeltaTable(str(path_of("bronze_live_events"))).to_pyarrow_dataset())
        done = {r[0] for r in con.execute("SELECT DISTINCT source_file FROM b").fetchall()}
        con.close()
    files = sorted(p for p in landing.rglob("*.ndjson"))
    now = datetime.now(UTC)
    # a batch whose first event is older than the bronze retention was loaded and pruned already
    edge = (now - BRONZE_RETENTION).strftime("%Y%m%dT%H%M%SZ")
    new = [p for p in files
           if p.relative_to(landing).as_posix() not in done and p.name[:16] >= edge]  # fmt: skip
    rows, nbytes, bad = [], 0, 0
    for p in new:
        rel = p.relative_to(landing).as_posix()
        raw = p.read_bytes()
        nbytes += len(raw)
        for line in raw.splitlines():
            try:
                row = flatten(json.loads(line))
            except ValueError:
                bad += 1
                continue
            row["source_file"], row["ingested_at"] = rel, now
            rows.append(row)
    if rows:
        t = pa.Table.from_pylist(rows, BRONZE_SCHEMA)
        _write("bronze_live_events", t, mode="append")
    pruned = prune_bronze(now)
    return {"files": len(new), "rows": len(rows), "bytes": nbytes, "unparseable": bad,
            "pruned": pruned}  # fmt: skip


def prune_bronze(now: datetime) -> int:
    if not exists("bronze_live_events"):
        return 0
    dt = DeltaTable(str(path_of("bronze_live_events")))
    cutoff = (now - BRONZE_RETENTION).strftime("%Y-%m-%d %H:%M:%S")
    res = dt.delete(f"event_dt < '{cutoff}'")
    n = int(res.get("num_deleted_rows", 0) or 0)
    if n:
        dt.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
    return n


def load_staging(staging: Path = DATA / "staging") -> dict[str, int]:
    """Replace the dump-derived silver tables from the slice staging Parquet."""
    out = {}
    for source, table, part in (
        ("history", "silver_page_revisions", ["wiki"]),
        ("sitelinks", "silver_sitelinks", None),
        ("pageviews", "silver_pageviews_daily", None),
    ):
        files = sorted((staging / source).glob("*.parquet"))
        if not files:
            continue
        # streamed in record batches: the slice history alone is 72 M rows (bounded memory)
        import pyarrow.dataset as pds

        from babel_lag.parsers import history, pageviews, sitelinks

        declared = {"history": history.SCHEMA, "sitelinks": sitelinks.SCHEMA,
                    "pageviews": pageviews.SCHEMA}[source]  # fmt: skip
        for name, typ in (("source_file", pa.string()), ("source_sha256", pa.string()),
                          ("fetched_at", pa.timestamp("us", tz="UTC"))):  # fmt: skip
            declared = declared.append(pa.field(name, typ))
        # the declared schema, not the first file's: an all-null column reads back as `null`
        d = pds.dataset([str(f) for f in files], format="parquet", schema=declared)
        reader = d.scanner(batch_size=250_000).to_reader()
        if source == "history":  # files staged before the parser coalesced empty flags
            reader = _false_for_null_flags(reader)
        _write(table, reader, partition=part)
        out[table] = d.count_rows()
    return out


def _false_for_null_flags(reader: pa.RecordBatchReader) -> pa.RecordBatchReader:
    """History flags are NULL only where the dump field was empty, which means not set."""
    import pyarrow.compute as pc

    flags = [f.name for f in reader.schema if pa.types.is_boolean(f.type)]

    def batches():
        for b in reader:
            cols = [pc.fill_null(b.column(n), False) if n in flags else b.column(n)
                    for n in b.schema.names]  # fmt: skip
            yield pa.RecordBatch.from_arrays(cols, schema=b.schema)

    return pa.RecordBatchReader.from_batches(reader.schema, batches())


def _input(name: str, schema_from: dict[str, pa.Schema]) -> Any:
    if exists(name):
        return DeltaTable(str(path_of(name))).to_pyarrow_dataset()
    return schema_from[name].empty_table()


def rollup(only: set[str] | None = None) -> dict[str, int]:
    """Run the core SQL on DuckDB over the Delta inputs and write every result table."""
    from babel_lag.parsers import history, pageviews, sitelinks

    lineage_cols = [("source_file", pa.string()), ("source_sha256", pa.string()),
                    ("fetched_at", pa.timestamp("us", tz="UTC"))]  # fmt: skip

    def with_lineage(s: pa.Schema) -> pa.Schema:
        for name, typ in lineage_cols:
            s = s.append(pa.field(name, typ))
        return s

    schemas = {
        "bronze_live_events": BRONZE_SCHEMA,
        "silver_page_revisions": with_lineage(history.SCHEMA),
        "silver_sitelinks": with_lineage(sitelinks.SCHEMA),
        "silver_pageviews_daily": with_lineage(pageviews.SCHEMA),
    }
    inputs = {n: _input(n, schemas) for n in engine.INPUTS}
    results = engine.run_duckdb(inputs, only)
    counts = {}
    for table, data in results.items():
        if table == "gold_live_activity" and exists(table) and data.num_rows:
            data = _keep_old_windows(data)
        _write(table, data)
        counts[table] = data.num_rows
    return counts


def _keep_old_windows(new: pa.Table) -> pa.Table:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.register("new_t", new)
    con.register("old_t", read("gold_live_activity"))
    out = con.execute(
        # The first new window may be cut by the bronze retention: the old copy of it wins.
        "WITH b AS (SELECT min(window_start) + INTERVAL 15 MINUTE AS edge FROM new_t) "
        "SELECT o.* FROM old_t o, b WHERE o.window_start < b.edge "
        "UNION ALL SELECT n.* FROM new_t n, b WHERE n.window_start >= b.edge "
        "ORDER BY window_start, wiki"
    ).to_arrow_table()
    con.close()
    return out
