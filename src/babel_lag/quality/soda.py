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


def scan() -> dict[str, int]:
    out = DATA / "telemetry" / "soda_results.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "BABEL_WAREHOUSE": str(DATA / "warehouse" / "babel.duckdb")}
    with Stage("soda_scan", "soda-core", SODA.split("==")[1]) as st:
        cmd = [shutil.which("uvx") or "uvx", "--python", "3.11", "--from", SODA, "soda", "scan",
               "-d", "babel", "-c", str(CONFIG / "soda" / "configuration.yml"),
               "-srf", str(out), str(CONFIG / "soda" / "checks.yml")]  # fmt: skip
        p = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
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
