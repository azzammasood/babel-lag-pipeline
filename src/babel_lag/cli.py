"""`babel` command line. Scheduled jobs and orchestrators call this CLI; DAGs hold no logic."""

from __future__ import annotations

import json
import shutil

import psutil
import typer

from babel_lag import __version__
from babel_lag.paths import DATA, ROOT
from babel_lag.policy import load_policy

app = typer.Typer(no_args_is_help=True, help="Babel Lag pipeline.")


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
