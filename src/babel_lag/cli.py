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


def main() -> None:  # pragma: no cover
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
