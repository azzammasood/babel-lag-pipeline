"""Databricks Free Edition over its REST API (Keystone ADR-067; Babel Lag ADR-005).

The workspace cannot fetch the streams or dumps (outbound internet is restricted), so the runner
delivers micro-batches and compact facts into a Unity Catalog volume, where Auto Loader in a
Lakeflow Declarative Pipeline picks them up. The token is read from the environment and only ever
sent in the Authorization header; errors carry the URL path and the API's message, never headers.
"""

from __future__ import annotations

import base64
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

TERMINAL = {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"}
STATEMENT_TIMEOUT_S = 900


class DatabricksError(RuntimeError):
    pass


def _host(raw: str) -> str:
    return re.sub(r"^https?://", "", raw.strip()).rstrip("/")


@dataclass
class Workspace:
    host: str
    token: str
    catalog: str = "workspace"  # Free Edition's default catalog
    transport: httpx.BaseTransport | None = None  # tests only

    @classmethod
    def from_env(cls) -> Workspace:
        host, token = os.environ.get("DATABRICKS_HOST", ""), os.environ.get("DATABRICKS_TOKEN", "")
        if not host or not token:
            raise DatabricksError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
        return cls(_host(host), token, os.environ.get("DATABRICKS_CATALOG", "workspace"))

    def _client(self) -> httpx.Client:
        return httpx.Client(
            base_url=f"https://{self.host}",
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=httpx.Timeout(120.0),
            transport=self.transport,
        )

    def api(self, method: str, path: str, **kw: Any) -> Any:
        with self._client() as c:
            r = c.request(method, path, **kw)
        if r.status_code >= 400:
            raise DatabricksError(f"{method} {path}: HTTP {r.status_code}: {r.text[:300]}")
        return r.json() if r.content and "json" in r.headers.get("content-type", "") else None

    # SQL warehouse ------------------------------------------------------------------------
    def warehouse_id(self) -> str:
        found = self.api("GET", "/api/2.0/sql/warehouses").get("warehouses") or []
        if not found:
            raise DatabricksError("no SQL warehouse in the workspace")
        return str(found[0]["id"])

    def warehouse_state(self, warehouse: str) -> str:
        return str(self.api("GET", f"/api/2.0/sql/warehouses/{warehouse}").get("state", "UNKNOWN"))

    def ensure_running(self, warehouse: str, timeout_s: float = 300, poll_s: float = 10) -> str:
        """Start a stopped warehouse and wait for RUNNING; returns the last state seen."""
        state = self.warehouse_state(warehouse)
        if state in ("STOPPED", "STOPPING"):
            self.api("POST", f"/api/2.0/sql/warehouses/{warehouse}/start")
        deadline = time.monotonic() + timeout_s
        while state != "RUNNING" and time.monotonic() < deadline:
            time.sleep(poll_s)
            state = self.warehouse_state(warehouse)
        return state

    def http_path(self) -> str:
        return f"/sql/1.0/warehouses/{self.warehouse_id()}"

    def sql(self, statement: str, warehouse: str | None = None) -> list[list[Any]]:
        """Run one statement on the serverless warehouse; rows of the first result chunk."""
        body = {
            "warehouse_id": warehouse or self.warehouse_id(),
            "statement": statement,
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        }
        res = self.api("POST", "/api/2.0/sql/statements", json=body)
        deadline = time.monotonic() + STATEMENT_TIMEOUT_S
        while res["status"]["state"] not in TERMINAL:
            if time.monotonic() > deadline:
                self.api("POST", f"/api/2.0/sql/statements/{res['statement_id']}/cancel")
                raise DatabricksError(f"statement still running after {STATEMENT_TIMEOUT_S} s")
            time.sleep(3)
            res = self.api("GET", f"/api/2.0/sql/statements/{res['statement_id']}")
        state = res["status"]["state"]
        if state != "SUCCEEDED":
            msg = (res["status"].get("error") or {}).get("message", "")[:400]
            raise DatabricksError(f"statement {state}: {msg}")
        return list((res.get("result") or {}).get("data_array") or [])

    def put_file(self, local: Path, remote: str) -> None:
        with self._client() as c:
            r = c.put(
                f"/api/2.0/fs/files{remote}",
                params={"overwrite": "true"},
                content=local.read_bytes(),
                headers={"Content-Type": "application/octet-stream"},
                timeout=httpx.Timeout(120.0, write=1800.0),  # large slice files on a slow uplink
            )
        if r.status_code >= 400:
            raise DatabricksError(f"PUT {remote}: HTTP {r.status_code}: {r.text[:300]}")

    # Unity Catalog ----------------------------------------------------------------------
    def ensure_schema(self, schema: str, comment: str) -> None:
        try:
            self.api("GET", f"/api/2.1/unity-catalog/schemas/{self.catalog}.{schema}")
        except DatabricksError:
            self.api(
                "POST",
                "/api/2.1/unity-catalog/schemas",
                json={"name": schema, "catalog_name": self.catalog, "comment": comment},
            )

    def ensure_volume(self, schema: str, volume: str, comment: str) -> str:
        full = f"{self.catalog}.{schema}.{volume}"
        try:
            self.api("GET", f"/api/2.1/unity-catalog/volumes/{full}")
        except DatabricksError:
            self.api(
                "POST",
                "/api/2.1/unity-catalog/volumes",
                json={
                    "catalog_name": self.catalog,
                    "schema_name": schema,
                    "name": volume,
                    "volume_type": "MANAGED",
                    "comment": comment,
                },
            )
        return f"/Volumes/{self.catalog}/{schema}/{volume}"

    # Lakeflow ---------------------------------------------------------------------------
    def upsert_pipeline(self, name: str, schema: str, sql: Path, ws_path: str) -> str:
        self.api("POST", "/api/2.0/workspace/mkdirs", json={"path": ws_path.rsplit("/", 1)[0]})
        self.api(
            "POST",
            "/api/2.0/workspace/import",
            json={
                "path": ws_path,
                "format": "SOURCE",
                "language": "SQL",
                "overwrite": True,
                "content": base64.b64encode(sql.read_bytes()).decode(),
            },
        )
        spec = {
            "name": name,
            "serverless": True,
            "catalog": self.catalog,
            "schema": schema,
            "continuous": False,
            "development": True,
            "libraries": [{"file": {"path": ws_path}}],
        }
        found = self.api("GET", "/api/2.0/pipelines", params={"filter": f"name LIKE '{name}'"})
        statuses = (found or {}).get("statuses") or []
        if statuses:
            pid = str(statuses[0]["pipeline_id"])
            self.api("PUT", f"/api/2.0/pipelines/{pid}", json={**spec, "id": pid})
            return pid
        return str(self.api("POST", "/api/2.0/pipelines", json=spec)["pipeline_id"])

    def run_pipeline(self, pid: str, full_refresh: bool = False, timeout_s: int = 1800) -> dict:
        body = {"full_refresh": full_refresh}
        for _ in range(60):  # another update (a scheduled run, the bundle job) may be active
            try:
                upd = self.api("POST", f"/api/2.0/pipelines/{pid}/updates", json=body)
                break
            except DatabricksError as exc:
                if "INVALID_STATE_TRANSITION" not in str(exc):
                    raise
                time.sleep(20)
        else:
            raise DatabricksError(f"pipeline {pid} stayed busy for 20 minutes")
        uid = upd["update_id"]
        t0, state = time.monotonic(), "QUEUED"
        while time.monotonic() - t0 < timeout_s:
            state = self.api("GET", f"/api/2.0/pipelines/{pid}/updates/{uid}")["update"]["state"]
            if state in {"COMPLETED", "FAILED", "CANCELED"}:
                break
            time.sleep(10)
        if state not in {"COMPLETED", "FAILED", "CANCELED"}:  # timed out: spare the quota
            self.api("POST", f"/api/2.0/pipelines/{pid}/stop")
        out: dict[str, Any] = {"state": state, "seconds": round(time.monotonic() - t0, 1)}
        if state != "COMPLETED":
            ev = self.api(
                "GET",
                f"/api/2.0/pipelines/{pid}/events",
                params={"max_results": 25, "filter": "level='ERROR'"},
            )
            out["errors"] = [e.get("message", "")[:300] for e in (ev or {}).get("events", [])][:5]
        return out
