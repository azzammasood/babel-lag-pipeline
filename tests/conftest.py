"""One offline end-to-end run shared by the smoke and privacy tests (speed mode: one run)."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from babel_lag.stream.sanitize import sanitize
from tests import parity_fixtures as fx
from tests.test_privacy import ANON, BLOCK_LOG, RECENTCHANGE, REVISION, USER_PAGE, USER_TALK

ROOT = Path(__file__).resolve().parents[1]


def _raw_live_events() -> list[dict]:
    """Raw events carrying usernames, IPs and comments, as EventStreams would send them."""
    out = []
    base = datetime.now(UTC).replace(microsecond=0) - timedelta(hours=1)  # inside bronze retention
    for i, ev in enumerate([RECENTCHANGE, REVISION, ANON, USER_TALK, USER_PAGE, BLOCK_LOG] * 3):
        dt = (base + timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%SZ")
        meta = {**ev["meta"], "id": f"e{i}", "dt": dt}
        out.append({**ev, "meta": meta, "wiki": ev.get("wiki") or ev.get("database")})
    return out


@pytest.fixture(scope="session")
def e2e(tmp_path_factory) -> Path:
    if shutil.which("dbt") is None and not (ROOT / ".venv" / "Scripts" / "dbt.exe").exists():
        pytest.skip("dbt not installed (uv sync --group warehouse)")
    data = tmp_path_factory.mktemp("data")
    env = {**os.environ, "BABEL_DATA": str(data), "BABEL_EDITOR_SALT": "e2e-salt",
           "BABEL_SKIP_UVX": "1", "BABEL_WRITE_METADATA": "0", "DATABRICKS_TOKEN": ""}  # fmt: skip
    env.pop("OPENLINEAGE_URL", None)
    env.pop("OTEL_EXPORTER_OTLP_ENDPOINT", None)
    os.environ["BABEL_EDITOR_SALT"] = "e2e-salt"
    events = _raw_live_events()
    first = events[0]["meta"]["dt"]
    landing = data / "landing" / "live" / "recentchange" / first[:4] / first[5:7] / first[8:10]
    landing.mkdir(parents=True)
    lines = [json.dumps(sanitize(e, hash_editor=True)) for e in events]
    name = first.replace("-", "").replace(":", "")[:15] + "Z_test-0001.ndjson"
    (landing / name).write_text("\n".join(lines) + "\n", "utf-8")
    inputs = fx.inputs()
    for source, table in (("history", "silver_page_revisions"), ("sitelinks", "silver_sitelinks"),
                          ("pageviews", "silver_pageviews_daily")):  # fmt: skip
        (data / "staging" / source).mkdir(parents=True)
        t: pa.Table = inputs[table]
        if source == "history":  # editor hashes as the parser writes them
            t = t.set_column(t.schema.get_field_index("editor_hash"), "editor_hash",
                             pa.array(["0123456789abcdef"] * t.num_rows))  # fmt: skip
        pq.write_table(t, data / "staging" / source / "fixture.parquet")
    cmd = [sys.executable, "-m", "babel_lag.cli", "profile", "check", "--run", "--want", "free",
           "--max-seconds", "0", "--dumps"]  # fmt: skip
    p = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True, text=True, check=False)
    assert p.returncode == 0, p.stdout[-2000:] + p.stderr[-2000:]
    return data
