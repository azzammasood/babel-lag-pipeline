"""ODCS contracts (contracts/*.odcs.yaml) linted by datacontract-cli, run through `uvx` in its
own environment. Each contract is one check; a contract that fails lint is a violation."""

from __future__ import annotations

import os
import shutil
import subprocess

from babel_lag.paths import CONTRACTS
from babel_lag.telemetry.runlog import Stage

DATACONTRACT = "datacontract-cli==1.2.9"


def lint() -> dict[str, int]:
    env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"}
    files = sorted(CONTRACTS.glob("*.odcs.yaml"))
    with Stage("contracts_lint", "datacontract-cli", DATACONTRACT.split("==")[1]) as st:
        bad = []
        for f in files:
            cmd = [shutil.which("uvx") or "uvx", "--python", "3.12", "--from", DATACONTRACT,
                   "datacontract", "lint", str(f)]  # fmt: skip
            p = subprocess.run(
                cmd, env=env, capture_output=True, encoding="utf-8", errors="replace", check=False
            )
            if p.returncode != 0 or "is valid" not in p.stdout:
                bad.append(f.name)
        st.records_in, st.records_out = len(files), len(files) - len(bad)
        st.detail.update(passed=len(files) - len(bad), failed=len(bad), invalid=bad)
    return {"passed": len(files) - len(bad), "failed": len(bad)}
