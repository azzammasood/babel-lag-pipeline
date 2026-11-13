"""Spark Structured Streaming for the `full` profile, run in the compose `spark` container:

    uv run babel spark stream   (docker compose ... run --rm spark spark-submit --packages
    <kafka, avro, delta> src/babel_lag/stream/spark_stream.py)

1. Kafka topic `babel.live.events` (Avro, Confluent wire format) -> Delta `bronze_live_events`
   with the same flattening as the Lakeflow bronze (trigger availableNow: drain and stop).
2. Event-time windows with a 10-minute watermark (late data dropped by Spark) -> Delta
   `stream_live_windows`, the streaming showcase.
3. The core SQL (rollup/sql, rendered for Spark) over Spark's bronze and the dump silver tables
   -> Delta gold under data/spark/lake. The host compares the dump-derived gold with DuckDB's.

Uses only pyspark and the standard library (the container has no other packages).
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from babel_lag.rollup import engine

REPO = Path("/repo")
LAKE = REPO / "data" / "spark" / "lake"
CKPT = REPO / "data" / "spark" / "checkpoints"
FREE = REPO / "data" / "lake"
AVRO = (REPO / "contracts" / "avro" / "live_event.avsc").read_text("utf-8")
PAYLOAD = (
    "meta STRUCT<id STRING, dt STRING, stream STRING, domain STRING, uri STRING>, wiki STRING, "
    "database STRING, type STRING, namespace BIGINT, page_namespace BIGINT, page_id BIGINT, "
    "title STRING, page_title STRING, rev_id BIGINT, bot BOOLEAN, page_is_redirect BOOLEAN, "
    "rev_len BIGINT, length_new BIGINT, editor_hash STRING"
)


def main(bootstrap: str = "kafka:29092") -> None:
    t0 = time.monotonic()
    spark = (
        SparkSession.builder.appName("babel-spark-stream")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", "babel.live.events")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")  # retention or a recreated topic resets offsets
        .load()
    )
    # Confluent wire format: 1 magic byte + 4-byte schema id, then the Avro body
    rec = from_avro(F.expr("substring(value, 6, length(value) - 5)"), AVRO).alias("r")
    ev = raw.select(rec, F.col("topic"), F.col("partition"), F.col("offset")).select(
        F.from_json("r.payload", PAYLOAD).alias("e"), "topic", "partition", "offset"
    )
    e = F.col
    bronze = ev.select(
        e("e.meta.id").alias("event_id"),
        e("e.meta.dt").cast("timestamp").alias("event_dt"),
        e("e.meta.stream").alias("stream"),
        F.coalesce(e("e.wiki"), e("e.database")).alias("wiki"),
        F.when(e("e.meta.stream") == "mediawiki.recentchange", e("e.type"))
        .otherwise(F.element_at(F.split(e("e.meta.stream"), "[.]"), -1))
        .alias("event_type"),
        F.coalesce(e("e.namespace"), e("e.page_namespace")).cast("int").alias("namespace"),
        e("e.page_id").alias("page_id"),
        F.coalesce(e("e.title"), e("e.page_title")).alias("page_title"),
        e("e.rev_id").alias("rev_id"),
        e("e.bot").alias("is_bot"),
        e("e.page_is_redirect").alias("is_redirect"),
        F.coalesce(e("e.rev_len"), e("e.length_new")).alias("length_new"),
        e("e.editor_hash").alias("editor_hash"),
        F.concat_ws(":", "topic", "partition", (F.floor(e("offset") / 10000)).cast("string")).alias(
            "source_file"
        ),  # Kafka offsets in blocks of 10k play the role of batch files
        F.current_timestamp().alias("ingested_at"),
    ).where("event_id IS NOT NULL AND event_dt IS NOT NULL AND wiki IS NOT NULL")
    q = (
        bronze.writeStream.format("delta")
        .option("checkpointLocation", str(CKPT / "bronze"))
        .trigger(availableNow=True)
        .start(str(LAKE / "bronze_live_events"))
    )
    q.awaitTermination()
    # the Delta sink reports numOutputRows = -1; the input side counts what this run read
    bronze_rows = sum(int(x.get("numInputRows") or 0) for x in q.recentProgress)

    windows = (
        spark.readStream.format("delta")
        .load(str(LAKE / "bronze_live_events"))
        .withWatermark("event_dt", "10 minutes")
        .dropDuplicatesWithinWatermark(["event_id"])
        .groupBy(F.window("event_dt", "15 minutes"), "wiki")
        .agg(F.count("*").alias("events"), F.sum(F.col("is_bot").cast("int")).alias("bot_events"))
        .select(F.col("window.start").alias("window_start"), "wiki", "events", "bot_events")
    )
    w = (
        windows.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(CKPT / "windows"))
        .trigger(availableNow=True)
        .start(str(LAKE / "stream_live_windows"))
    )
    w.awaitTermination()

    inputs = {
        "bronze_live_events": spark.read.format("delta").load(str(LAKE / "bronze_live_events"))
    }
    dump_tables = ("silver_page_revisions", "silver_sitelinks", "silver_pageviews_daily")
    missing = [n for n in dump_tables if not (FREE / "silver" / n / "_delta_log").exists()]
    only = None
    if missing:  # dump slice not built yet: run only the live steps of the core SQL
        only = {"silver_live_events", "gold_live_activity"}
    else:
        for name in dump_tables:
            inputs[name] = spark.read.format("delta").load(str(FREE / "silver" / name))
    counts = {"bronze_rows_this_run": bronze_rows, "skipped_missing_inputs": missing}
    for table, df in engine.run_spark(spark, inputs, only).items():
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
            str(LAKE / table)
        )
        counts[table] = spark.read.format("delta").load(str(LAKE / table)).count()
    counts["stream_live_windows"] = spark.read.format("delta").load(
        str(LAKE / "stream_live_windows")).count()  # fmt: skip
    counts["bronze_live_events_total"] = inputs["bronze_live_events"].count()
    counts["spark_seconds"] = round(time.monotonic() - t0, 1)
    (REPO / "data" / "spark" / "summary.json").write_text(json.dumps(counts), "utf-8")
    print(json.dumps(counts))
    spark.stop()


if __name__ == "__main__":
    main(*sys.argv[1:])
