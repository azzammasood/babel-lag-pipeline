"""Soda Core checks over the dbt marts (config/soda/checks.yml).

Soda Core pins an old OpenTelemetry, so it runs in its own environment through `uvx` (no
conflict with the pipeline's dependencies). Results are read from its scan-results file.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess

from babel_lag.paths import CONFIG, DATA
from babel_lag.telemetry.runlog import Stage

SODA = "soda-core-duckdb==3.5.6"


def _v1_copy() -> object:
    """Soda's DuckDB adapter pins duckdb<1.1, which cannot open a DuckDB 2.0 file: copy the marts
    into a file in the v1.0.0 storage format (written by DuckDB 2.0 itself)."""
    import duckdb

    src = DATA / "warehouse" / "babel.duckdb"
    dst = DATA / "warehouse" / "soda_v1.duckdb"
    dst.unlink(missing_ok=True)
    con = duckdb.connect()
    con.execute(f"ATTACH '{src.as_posix()}' AS w (READ_ONLY)")
    con.execute(f"ATTACH '{dst.as_posix()}' AS v1 (STORAGE_VERSION 'v1.0.0')")
    for t in ("mart_language_index", "mart_live_hourly"):
        con.execute(f"CREATE TABLE v1.main.{t} AS SELECT * FROM w.main.{t}")
    con.execute("DETACH v1")
    con.close()
    return dst


def scan() -> dict[str, int]:
    out = DATA / "telemetry" / "soda_results.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "BABEL_WAREHOUSE": str(_v1_copy())}
    with Stage("soda_scan", "soda-core", SODA.split("==")[1]) as st:
        cmd = [shutil.which("uvx") or "uvx", "--python", "3.11", "--from", SODA, "soda", "scan",
               "-d", "babel", "-c", str(CONFIG / "soda" / "configuration.yml"),
               "-srf", str(out), str(CONFIG / "soda" / "checks.yml")]  # fmt: skip
        p = subprocess.run(
            cmd, env=env, capture_output=True, encoding="utf-8", errors="replace", check=False
        )
        checks = json.loads(out.read_text("utf-8")).get("checks", []) if out.exists() else []
        res = {"passed": sum(c.get("outcome") == "pass" for c in checks),
               "failed": sum(c.get("outcome") == "fail" for c in checks)}  # fmt: skip
        st.detail.update(res)
        st.records_out = len(checks)
        if p.returncode not in (0, 1) or not checks:  # 1 = warnings
            raise RuntimeError(f"soda scan exit {p.returncode}: {p.stdout[-400:]}{p.stderr[-400:]}")
        if res["failed"]:
            raise RuntimeError(f"soda: {res['failed']} checks failed")
    return res
