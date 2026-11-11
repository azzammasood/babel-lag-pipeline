"""`babel profile check`: probe the `full` profile, run the job there when every probe passes,
otherwise (or when the `full` job fails) run it in `free` (brief section 5; Keystone ADR-040).

Probes: the Databricks control plane with the token (missing token, revoked token, deleted
account or a blocked workspace all fail it), Kafka's broker port and the schema registry. The
decision is one `profile` stage in the run log whose detail carries `produced_by`, read by the
export envelope and `pipeline_metrics.json`. The `free` job is production: its failure fails
the run.
"""

from __future__ import annotations

import os
import socket
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field

import httpx

from babel_lag import __version__
from babel_lag.telemetry.runlog import Stage

KAFKA = os.environ.get("BABEL_KAFKA", "127.0.0.1:9092")
REGISTRY = os.environ.get("BABEL_SCHEMA_REGISTRY", "http://127.0.0.1:8081")
TIMEOUT = 5.0


@dataclass
class ProbeResult:
    name: str
    ok: bool
    seconds: float
    detail: str


def _databricks() -> str:
    host, token = os.environ.get("DATABRICKS_HOST", ""), os.environ.get("DATABRICKS_TOKEN", "")
    if not host or not token:
        raise RuntimeError("DATABRICKS_HOST/DATABRICKS_TOKEN not set")
    host = host.replace("https://", "").rstrip("/")
    r = httpx.get(f"https://{host}/api/2.0/sql/warehouses", timeout=TIMEOUT,
                  headers={"Authorization": f"Bearer {token}"})  # fmt: skip
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code}")
    return f"HTTP {r.status_code}"


def _kafka() -> str:
    h, _, p = KAFKA.rpartition(":")
    with socket.create_connection((h, int(p)), timeout=TIMEOUT):
        return "connected"


def _registry() -> str:
    r = httpx.get(f"{REGISTRY}/subjects", timeout=TIMEOUT)
    r.raise_for_status()
    return f"HTTP {r.status_code}"


# `full` = Databricks (reachable from Actions and the owner's machine) plus, where the compose
# `stream` profile runs (BABEL_FULL_STREAM=1, the owner's machine), Kafka and the registry.
PROBES: dict[str, Callable[[], str]] = {"databricks": _databricks}
if os.environ.get("BABEL_FULL_STREAM", "1" if not os.environ.get("GITHUB_ACTIONS") else "0") == "1":
    PROBES |= {"kafka": _kafka, "schema-registry": _registry}


def _run_probe(name: str, fn: Callable[[], str]) -> ProbeResult:
    t0 = time.perf_counter()
    try:
        detail, ok = fn(), True
    except (OSError, httpx.HTTPError, RuntimeError, ValueError) as exc:
        detail, ok = f"{type(exc).__name__}: {exc}"[:200], False
    return ProbeResult(name, ok, round(time.perf_counter() - t0, 3), detail)


def check(probes: dict[str, Callable[[], str]] = PROBES) -> list[ProbeResult]:
    with ThreadPoolExecutor(max_workers=len(probes)) as pool:
        return list(pool.map(lambda kv: _run_probe(*kv), probes.items()))


@dataclass
class Outcome:
    produced_by: str | None
    full_healthy: bool
    reason: str
    probes: list[ProbeResult] = field(default_factory=list)

    @property
    def fallback(self) -> bool:
        return self.produced_by == "free" and self.reason != "requested"

    def as_dict(self) -> dict[str, object]:
        return {"produced_by": self.produced_by, "full_healthy": self.full_healthy,
                "fallback": self.fallback, "reason": self.reason,
                "probes": [asdict(p) for p in self.probes]}  # fmt: skip


Job = Callable[[dict[str, object]], None]


def produced(profile: str, fallback: bool) -> dict[str, object]:
    from babel_lag.lineage.emit import pipeline_run_id

    return {"profile": profile, "fallback": fallback, "run_id": pipeline_run_id(),
            "babel_version": __version__}  # fmt: skip


def run(
    full_job: Job | None,
    free_job: Job | None,
    want: str = "full",
    probes: dict[str, Callable[[], str]] = PROBES,
) -> Outcome:
    """Probe, then run one job in the best profile (jobs None = probe only). Each job gets the
    `produced_by` block its exports must carry."""
    with Stage("profile", "babel-profile", __version__) as st:
        results = check(probes) if want == "full" else []
        down = [p.name for p in results if not p.ok]
        out = Outcome("free", want == "full" and not down, "requested", results)
        if want == "full" and down:
            out.reason = f"full unhealthy: {', '.join(down)} down"
        elif want == "full":
            out.reason = "full healthy"
            if full_job is None:
                out.produced_by = None
            else:
                try:
                    full_job(produced("full", False))
                    out.produced_by = "full"
                except (Exception, SystemExit) as exc:
                    out.reason = f"full job failed: {type(exc).__name__}: {exc}"[:300]
        if out.produced_by == "free":
            if free_job is None:
                out.produced_by = None
            else:
                st.detail.update(out.as_dict())  # recorded even if the free job fails
                free_job(produced("free", out.fallback))
        st.records_in, st.records_out = len(results), sum(p.ok for p in results)
        st.detail.update(out.as_dict())
    return out
