"""End-to-end smoke run (offline) and JSON Schema validation of every export (speed mode)."""

from __future__ import annotations

import json
from pathlib import Path

from babel_lag.export.site import validate
from babel_lag.telemetry import metrics


def test_end_to_end_smoke(e2e: Path) -> None:
    out = e2e / "export" / "v1"
    langs = json.loads((out / "languages.json").read_text("utf-8"))
    live = json.loads((out / "live.json").read_text("utf-8"))
    assert langs["produced_by"]["profile"] == "free" and not langs["produced_by"]["fallback"]
    by_wiki = {r["wiki"]: r for r in langs["rows"]}
    assert by_wiki["zuwiki"]["median_lag_days"] == 789.5  # the parity fixture's hand answer
    assert by_wiki["urwiki"]["pageviews"] == 90 and by_wiki["urwiki"]["coverage_pct"] == 100.0
    assert live["row_count"] > 0 and {r["wiki"] for r in live["rows"]} >= {"enwiki", "dewiki"}


def test_every_export_validates(e2e: Path) -> None:
    out = e2e / "export" / "v1"
    for name in ("languages", "live"):
        validate(json.loads((out / f"{name}.json").read_text("utf-8")))
    doc = json.loads((out / "pipeline_metrics.json").read_text("utf-8"))
    metrics.validate(doc)
    assert doc["profile"]["produced_by"] == "free" and doc["cost"]["usd"] == 0
    for name in ("catalog", "lineage", "glossary"):
        assert json.loads((out / f"metadata_{name}.json").read_text("utf-8"))
