"""Spike: can a token upload micro-batches to a UC Volume and trigger a Lakeflow pipeline?

Usage: uv run python scripts/spike/databricks_volume.py
Collects ~60 s of live events, sanitizes them (no usernames/IPs/comments), uploads two NDJSON
micro-batches, runs the Auto Loader pipeline twice (full, then incremental) and counts rows.
Writes data/spike/databricks_volume.json.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time
from pathlib import Path

import httpx

from babel_lag.cloud.databricks import DatabricksError, Workspace
from babel_lag.env import load_dotenv
from babel_lag.paths import DATA
from babel_lag.policy import user_agent
from babel_lag.stream.sanitize import sanitize

sys.path.insert(0, str(Path(__file__).parent))
from live_rate import BASE, STREAMS, sse  # noqa: E402

SQL = Path("src/babel_lag/cloud/lakeflow/bronze_live.sql")
OUT = DATA / "spike"


def collect(seconds: int) -> list[dict]:
    client = httpx.Client(headers={"User-Agent": user_agent()}, timeout=httpx.Timeout(30, read=60))
    rows, t0 = [], time.monotonic()
    for _, raw in sse(client, BASE + STREAMS, {}):
        rows.append(sanitize(json.loads(raw)))
        if time.monotonic() - t0 > seconds:
            break
    return rows


def main() -> None:
    load_dotenv()
    ws = Workspace.from_env()
    res: dict = {"measured_at": dt.datetime.now(dt.UTC).isoformat()}
    t = time.monotonic()
    me = ws.api("GET", "/api/2.0/preview/scim/v2/Me")
    res["control_plane_ok"] = bool(me)
    whs = ws.api("GET", "/api/2.0/sql/warehouses").get("warehouses") or []
    res["warehouses"] = [
        {"name": w["name"], "state": w["state"], "size": w.get("cluster_size")} for w in whs
    ]
    ws.ensure_schema("babel_landing", "Babel Lag landing zone: sanitized micro-batches (no PII).")
    ws.ensure_schema("babel_bronze", "Babel Lag bronze streaming tables (Lakeflow).")
    vol = ws.ensure_volume("babel_landing", "live", "Sanitized EventStreams NDJSON batches.")
    rows = collect(int(os.environ.get("BABEL_SPIKE_SECONDS", "60")))
    half = len(rows) // 2
    OUT.mkdir(parents=True, exist_ok=True)
    batches = []
    for i, part in enumerate((rows[:half], rows[half:])):
        p = OUT / f"live_batch_{i}.ndjson"
        p.write_text("\n".join(json.dumps(r) for r in part) + "\n", encoding="utf-8")
        batches.append((p, len(part)))
    t_up = time.monotonic()
    ws.put_file(batches[0][0], f"{vol}/spike/{batches[0][0].name}")
    res["upload_s"] = round(time.monotonic() - t_up, 2)
    res["batch_bytes"] = batches[0][0].stat().st_size
    pid = ws.upsert_pipeline(
        "babel-bronze-live", "babel_bronze", SQL, "/Workspace/Shared/babel-lag/bronze_live.sql"
    )
    res["run_full"] = ws.run_pipeline(pid, full_refresh=True)
    wh = whs[0]["id"] if whs else None
    count = "SELECT count(*) FROM workspace.babel_bronze.bronze_live_events"
    try:
        res["warehouse_state_after_start"] = ws.ensure_running(wh) if wh else None
        res["rows_after_batch_0"] = int(ws.sql(count, wh)[0][0])
    except DatabricksError as exc:
        res["sql_error"] = str(exc)[:300]
    ws.put_file(batches[1][0], f"{vol}/spike/{batches[1][0].name}")
    res["run_incremental"] = ws.run_pipeline(pid)
    try:
        res["rows_after_batch_1"] = int(ws.sql(count, wh)[0][0])
    except DatabricksError as exc:
        res["sql_error_2"] = str(exc)[:300]
    res["rows_uploaded"] = [n for _, n in batches]
    res["total_s"] = round(time.monotonic() - t, 1)
    for p, _ in batches:
        p.unlink()
    (OUT / "databricks_volume.json").write_text(json.dumps(res, indent=1), encoding="utf-8")
    print(json.dumps(res))


if __name__ == "__main__":
    main()
