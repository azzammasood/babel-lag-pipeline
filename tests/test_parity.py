"""`free` (DuckDB) vs `full` (Spark) parity on the core lag logic (brief section 0 rule 10).

The DuckDB leg always runs and is checked against hand-computed answers. The Spark leg needs
Java; it is skipped locally without it and required in CI (`BABEL_REQUIRE_SPARK=1`).
"""

from __future__ import annotations

import os
import shutil
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from babel_lag.rollup import engine
from tests.parity_fixtures import inputs


def _norm(v):
    if isinstance(v, datetime):
        v = v.replace(tzinfo=UTC) if v.tzinfo is None else v.astimezone(UTC)
        return v.strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(v, float | Decimal):
        return round(float(v), 6)
    return v


def _rows(records) -> list[tuple]:
    return sorted((tuple(_norm(v) for v in r.values()) for r in records), key=repr)


def duck() -> dict[str, list[tuple]]:
    return {k: _rows(t.to_pylist()) for k, t in engine.run_duckdb(inputs()).items()}


def test_duckdb_core_logic() -> None:
    out = engine.run_duckdb(inputs())
    silver = {r["event_id"]: r for r in out["silver_live_events"].to_pylist()}
    assert sorted(silver) == list("abcdefgh")  # duplicates dropped
    assert silver["c"]["source_file"] == "b1"  # earliest arrival wins
    assert silver["e"]["is_late"] and not silver["a"]["is_late"]
    act = {(_norm(r["window_start"]), r["wiki"]): r for r in out["gold_live_activity"].to_pylist()}
    ur0 = act[("2026-11-11T05:00:00", "urwiki")]
    assert (ur0["events"], ur0["bot_events"], ur0["human_article_edits"], ur0["new_articles"]) == (
        4, 1, 1, 1)  # fmt: skip
    assert act[("2026-11-11T05:00:00", "zuwiki")]["late_events"] == 1
    lag = {(r["item_id"], r["wiki"]): r for r in out["gold_coverage_lag"].to_pylist()}
    assert (
        lag[(843, "urwiki")]["is_first"] and lag[(843, "swwiki")]["lag_seconds"] == 170_812_800
    )  # 1977 days
    assert lag[(405, "urwiki")]["is_first"] and lag[(405, "zuwiki")]["is_first"]  # tie
    assert (405, "swwiki") not in lag and (99, "urwiki") not in lag  # bot-created, deleted
    lang = {r["wiki"]: r for r in out["gold_language_lag"].to_pylist()}
    assert lang["zuwiki"]["items_followed"] == 1 and lang["urwiki"]["items_in_reference"] == 2
    assert round(lang["zuwiki"]["median_lag_days"], 3) == 789.5
    views = {(r["wiki"], r["item_id"], r["day"].day): r["views"]
             for r in out["gold_item_pageviews"].to_pylist()}  # fmt: skip
    assert views == {("urwiki", 843, 10): 50, ("urwiki", 843, 9): 40, ("zuwiki", 405, 10): 3}


def _spark():
    if not shutil.which("java") and not os.environ.get("JAVA_HOME"):
        if os.environ.get("BABEL_REQUIRE_SPARK") == "1":
            pytest.fail("BABEL_REQUIRE_SPARK=1 but no Java found")
        pytest.skip("Spark leg needs Java (runs in CI)")
    pyspark = pytest.importorskip("pyspark.sql")
    return (
        pyspark.SparkSession.builder.master("local[1]")
        .appName("babel-parity")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def test_spark_matches_duckdb() -> None:
    spark = _spark()
    frames = {k: spark.createDataFrame(t) for k, t in inputs().items()}
    out = engine.run_spark(spark, frames)
    expected = duck()
    for table, df in out.items():
        got = _rows(r.asDict() for r in df.collect())
        assert got == expected[table], f"{table}: Spark and DuckDB differ"
