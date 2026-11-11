"""The run: one CLI call per scheduled job (Actions, Dagster and Databricks Jobs call this).

`free_job`: catch-up collector -> bronze (delta-rs) -> core SQL (DuckDB) -> dbt (duckdb) ->
Soda -> export -> metadata -> pipeline_metrics. `full_job`: the same collector with the Kafka
sink, upload to the UC volume, Lakeflow pipeline, dbt (databricks), export from Databricks.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
from importlib.metadata import version
from pathlib import Path
from typing import Any

from babel_lag import __version__
from babel_lag.lake import delta
from babel_lag.paths import DATA, EXPORT, ROOT
from babel_lag.telemetry.runlog import Stage, flush_otel

LIVE_STEPS = {"silver_live_events", "gold_live_activity"}


def _v(pkg: str) -> str:
    try:
        return version(pkg)
    except Exception:  # noqa: BLE001
        return "unknown"


def collect(max_seconds: float, kafka: bool = False) -> dict[str, Any]:
    from babel_lag.stream.collector import collect as run_collect

    sink = None
    if kafka:
        from babel_lag.stream.kafka_sink import KafkaSink

        sink = KafkaSink()
    with Stage("collect", "eventstreams-collector", __version__, "collect_live") as st:
        s = asyncio.run(run_collect(mode="catchup", max_seconds=max_seconds, kafka=sink))
        if sink:
            sink.close()
        st.records_in, st.records_out, st.bytes_in, st.bytes_out = (
            s.events + s.unparseable, s.events, s.bytes_in, s.bytes_out)  # fmt: skip
        lags = sorted(s.receive_lag_s)
        st.detail.update(reconnects=s.reconnects, unparseable=s.unparseable,
                         batches=[Path(b).name for b in s.batches],
                         first_dt=s.first_dt, last_dt=s.last_dt,
                         receive_lag_p50_s=lags[len(lags) // 2] if lags else None)  # fmt: skip
    return {"events": s.events, "batches": s.batches}


def ingest_and_rollup(dumps: bool = False) -> None:
    with Stage("bronze", "delta-rs", _v("deltalake"), "ingest_bronze_live") as st:
        res = delta.ingest_landing()
        st.records_in, st.records_out, st.bytes_in = res["rows"] + res["unparseable"], res[
            "rows"], res["bytes"]  # fmt: skip
        st.detail.update(res)
    if dumps:
        with Stage("load_dumps", "delta-rs", _v("deltalake")) as st:
            st.detail.update(delta.load_staging())
            st.records_out = sum(st.detail.values())
    only = None if dumps else LIVE_STEPS
    with Stage("rollup", "duckdb", _v("duckdb"), "rollup_gold_live_activity") as st:
        counts = delta.rollup(only)
        st.records_out = sum(counts.values())
        st.detail.update(counts)


def dbt_build(target: str) -> None:
    exe = shutil.which("dbt") or str(ROOT / ".venv" / "Scripts" / "dbt.exe")
    service = f"dbt-{target}"
    with Stage("dbt_build", service, _v(service)) as st:
        env = {**os.environ, "BABEL_DATA": str(DATA)}
        (DATA / "warehouse").mkdir(parents=True, exist_ok=True)
        cmd = [exe, "build", "--project-dir", "dbt", "--profiles-dir", "dbt", "--target", target]
        p = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True, text=True, check=False)
        rr = ROOT / "dbt" / "target" / "run_results.json"
        results = json.loads(rr.read_text("utf-8"))["results"] if rr.exists() else []
        tests = [r for r in results if r["unique_id"].startswith("test.")]
        st.detail.update(passed=sum(r["status"] == "pass" for r in tests),
                         failed=sum(r["status"] in ("fail", "error") for r in tests),
                         models=sum(r["unique_id"].startswith("model.")
                                    for r in results))  # fmt: skip
        st.records_out = st.detail["models"]
        if p.returncode != 0:
            raise RuntimeError("dbt build failed: " + p.stdout[-800:])
        subprocess.run([exe, "docs", "generate", "--project-dir", "dbt", "--profiles-dir", "dbt",
                        "--target", target], cwd=ROOT, env=env, capture_output=True,
                       check=False)  # fmt: skip


def soda_scan() -> None:
    from babel_lag.quality.soda import scan

    scan()


def export(produced: dict[str, Any]) -> None:
    from babel_lag.export import meta, site
    from babel_lag.telemetry import metrics

    with Stage("export", "babel-export", __version__, "export_languages") as st:
        counts = site.write_all(produced)
        st.records_out = sum(counts.values())
        st.detail.update(counts)
    # the committed metadata is rebuilt only from the real lake (not from a test run)
    summary = meta.build(write=os.environ.get("BABEL_WRITE_METADATA", "1") == "1")
    out = EXPORT / "v1"
    for p in meta.metadata_paths():
        shutil.copy(p, out / f"metadata_{p.name}")
    flush_otel()
    metrics.write(metrics.build(summary, produced), out / "pipeline_metrics.json")


def free_job(produced: dict[str, Any], max_seconds: float = 900, dumps: bool = False) -> None:
    if max_seconds > 0:  # 0: no collection (offline smoke test, or a rollup-only run)
        collect(max_seconds)
    ingest_and_rollup(dumps)
    dbt_build("duckdb")
    if shutil.which("uvx") and os.environ.get("BABEL_SKIP_UVX") != "1":
        soda_scan()
        from babel_lag.quality.contracts import lint

        lint()
    export(produced)


def full_job(produced: dict[str, Any], max_seconds: float = 900) -> None:
    from babel_lag.cloud import lakehouse

    collect(
        max_seconds,
        kafka=os.environ.get("BABEL_FULL_STREAM", "0") == "1"
        or not os.environ.get("GITHUB_ACTIONS"),
    )
    ingest_and_rollup()  # the local lake stays current for metrics and fallback
    lakehouse.sync_and_run()
    dbt_build("databricks")
    export(produced)
