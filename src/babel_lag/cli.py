"""`babel` command line. Scheduled jobs and orchestrators call this CLI; DAGs hold no logic."""

from __future__ import annotations

import json
import shutil

import psutil
import typer

from babel_lag import __version__
from babel_lag.env import load_dotenv
from babel_lag.paths import DATA, ROOT
from babel_lag.policy import load_policy, load_slice, slice_wikis

app = typer.Typer(no_args_is_help=True, help="Babel Lag pipeline.")
dumps_app = typer.Typer(no_args_is_help=True, help="History, pageview and sitelink dumps.")
app.add_typer(dumps_app, name="dumps")


@app.callback()
def _setup() -> None:
    load_dotenv()


@dumps_app.command("slice")
def dumps_slice(
    history: bool = True, pageviews: bool = True, sitelinks: bool = True, redo: bool = False
) -> None:
    """Download, parse and delete the milestone 2 slice dumps (resumable: done files skipped)."""
    from babel_lag.dumps import fetch

    jobs, names = [], []
    if sitelinks:
        names.append(("sitelinks", "wb_items_per_site"))
        jobs.append(fetch.sitelinks_job())
    if history:
        for w in slice_wikis():
            names.append(("history", w))
            jobs.append(fetch.history_job(w))
    if pageviews:
        for d in load_slice()["pageview_days"]:
            names.append(("pageviews", d))
            jobs.append(fetch.pageview_job(d))
    todo = [
        j for j, (src, n) in zip(jobs, names, strict=True)
        if redo or not (fetch.STAGING / src / f"{n}.parquet").exists()
    ]  # fmt: skip
    typer.echo(f"{len(todo)} of {len(jobs)} files to fetch")
    fetch.run_jobs(todo, DATA / "staging" / "fetch_log.jsonl")


@app.command()
def version() -> None:
    """Print the package version."""
    typer.echo(__version__)


@app.command()
def machine(as_json: bool = typer.Option(False, "--json")) -> None:
    """Disk, RAM, CPUs, Docker and the bulk-data disk budget (brief section 1)."""
    policy = load_policy()
    disk = shutil.disk_usage(ROOT.anchor or ROOT)
    mem = psutil.virtual_memory()
    info = {
        "free_disk_gb": round(disk.free / 1e9, 1),
        "disk_budget_gb": round(disk.free * policy["disk_budget_fraction"] / 1e9, 1),
        "ram_total_gb": round(mem.total / 1e9, 1),
        "ram_available_gb": round(mem.available / 1e9, 1),
        "cpus": psutil.cpu_count(logical=True),
        "docker": shutil.which("docker") is not None,
        "data_dir": str(DATA),
    }
    if as_json:
        typer.echo(json.dumps(info, indent=2))
    else:
        for k, v in info.items():
            typer.echo(f"{k:18} {v}")


profile_app = typer.Typer(no_args_is_help=True, help="Profiles: full showcase, free production.")
app.add_typer(profile_app, name="profile")
lake_app = typer.Typer(no_args_is_help=True, help="Delta lake and the core SQL.")
app.add_typer(lake_app, name="lake")
metadata_app = typer.Typer(no_args_is_help=True, help="Contracts, catalog, lineage, glossary.")
app.add_typer(metadata_app, name="metadata")


@profile_app.command("check")
def profile_check(
    run: bool = typer.Option(False, "--run", help="Run the pipeline in the chosen profile."),
    want: str = typer.Option("full", help="Wanted profile: full (falls back) or free."),
    max_seconds: float = typer.Option(900, help="Collector time box."),
    dumps: bool = typer.Option(False, help="Also reload the slice dumps into silver/gold."),
) -> None:
    """Probe full; with --run, run there or fall back to free and record produced_by."""
    from babel_lag import pipeline
    from babel_lag.profile.run import run as run_profile

    full = (lambda p: pipeline.full_job(p, max_seconds)) if run else None
    free = (lambda p: pipeline.free_job(p, max_seconds, dumps)) if run else None
    out = run_profile(full, free, want)
    for p in out.probes:
        typer.echo(f"{p.name:16} {'ok ' if p.ok else 'DOWN'} {p.seconds:6.2f}s {p.detail}")
    typer.echo(f"produced_by={out.produced_by} fallback={out.fallback} reason={out.reason}")


@lake_app.command("rollup")
def lake_rollup(dumps: bool = typer.Option(False, help="Reload staging dumps first.")) -> None:
    """Load landing batches (and optionally dumps) and run the core SQL on DuckDB."""
    from babel_lag import pipeline

    pipeline.ingest_and_rollup(dumps)


@metadata_app.command("build")
def metadata_build(
    gate: float = typer.Option(95.0, help="Fail below this completeness %."),
) -> None:
    """Write contracts/*.odcs.yaml and metadata/*.json; enforce the completeness gate."""
    from babel_lag.export import meta

    res = meta.build()
    comp = res["completeness"]
    typer.echo(f"completeness {comp['documented_pct']}% ({comp['documented']}/{comp['items']}); "
               f"lineage {res['nodes']} nodes, {res['edges']} edges")  # fmt: skip
    if comp["missing"]:
        typer.echo("undocumented: " + ", ".join(comp["missing"][:20]))
    if comp["documented_pct"] < gate:
        raise typer.Exit(1)


cloud_app = typer.Typer(no_args_is_help=True, help="Databricks (full): volume, Lakeflow, UC.")
app.add_typer(cloud_app, name="cloud")
lineage_app = typer.Typer(no_args_is_help=True, help="Marquez and OpenMetadata.")
app.add_typer(lineage_app, name="lineage")
spark_app = typer.Typer(no_args_is_help=True, help="Spark Structured Streaming (compose stream).")
app.add_typer(spark_app, name="spark")


@cloud_app.command("render")
def cloud_render() -> None:
    """Regenerate the Lakeflow pipeline SQL from the core SQL (committed; CI checks it)."""
    from babel_lag.cloud import lakehouse

    typer.echo(lakehouse.write_sql())


@cloud_app.command("run")
def cloud_run(full_refresh: bool = False) -> None:
    """Upload new batches and slice tables, run the Lakeflow pipeline, tag UC tables."""
    from babel_lag.cloud import lakehouse

    out = lakehouse.sync_and_run(full_refresh)
    typer.echo(json.dumps(out, default=str)[:600])


@cloud_app.command("lineage")
def cloud_lineage() -> None:
    """Unity Catalog upstream lineage of the marts (saved to data/evidence/uc_lineage.json)."""
    from babel_lag.cloud import lakehouse

    for t, ups in lakehouse.uc_lineage().items():
        typer.echo(f"{t} <- {', '.join(ups) or '-'}")


@lineage_app.command("replay")
def lineage_replay() -> None:
    """Re-send data/lineage/events.jsonl to Marquez (OPENLINEAGE_URL)."""
    import orjson

    from babel_lag.lineage.emit import events_path, write

    events = [orjson.loads(x) for x in events_path().read_bytes().splitlines() if x]
    bak = events_path().with_suffix(".jsonl.bak")
    events_path().replace(bak)  # write() appends; keep one copy
    failed = write(events)
    bak.unlink()
    typer.echo(f"{len(events)} events sent, {failed} failed")


@lineage_app.command("openmetadata")
def lineage_openmetadata() -> None:
    """Push datasets, columns, owners, domain, glossary and lineage to OpenMetadata."""
    from babel_lag.lineage.openmetadata import push

    typer.echo(push())


@spark_app.command("stream")
def spark_stream() -> None:
    """Kafka -> Delta bronze, watermarked windows and the core SQL on Spark, in Docker."""
    import subprocess

    from babel_lag.telemetry.runlog import Stage

    pkgs = ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
        "org.apache.spark:spark-avro_2.13:4.0.1",
        "io.delta:delta-spark_2.13:4.0.0",
    ])  # fmt: skip
    cmd = ["docker", "compose", "-f", "compose/docker-compose.yml", "--profile", "stream", "run",
           "--rm", "spark", "/opt/spark/bin/spark-submit", "--driver-memory", "1g",
           "--conf", "spark.jars.ivy=/tmp/.ivy2", "--packages", pkgs,
           "src/babel_lag/stream/spark_stream.py"]  # fmt: skip
    with Stage("spark_stream", "spark", "4.0.1", "rollup_silver_live_events") as st:
        p = subprocess.run(cmd, cwd=ROOT, capture_output=True,
                           encoding="utf-8", errors="replace", check=False)  # fmt: skip
        summary = DATA / "spark" / "summary.json"
        if p.returncode != 0 or not summary.exists():
            raise RuntimeError(p.stderr[-1500:])
        st.detail.update(json.loads(summary.read_text("utf-8")))
        st.records_out = st.detail.get("bronze_rows_this_run")
    typer.echo(summary.read_text("utf-8"))


def main() -> None:  # pragma: no cover
    app()


if __name__ == "__main__":  # pragma: no cover
    app()
