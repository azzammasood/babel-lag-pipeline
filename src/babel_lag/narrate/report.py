"""Weekly "Babel report" (brief section 5): an LLM narrates numbers already computed in SQL; it
never computes. Spend cap $0: only OpenRouter models whose id ends in `:free` are accepted.

Validation: every number in the prose must appear in the facts handed to the model (after
rounding to the precision shown); a report with any other number is rejected and not published.
Cached by the hash of the facts, so an unchanged week costs no call. Without OPENROUTER_API_KEY
nothing runs and nothing is published (the report is optional on the site).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from babel_lag.paths import DATA, EXPORT

MODEL = os.environ.get("BABEL_REPORT_MODEL", "meta-llama/llama-3.3-70b-instruct:free")
URL = "https://openrouter.ai/api/v1/chat/completions"
NUM = re.compile(r"(?<![\w.])-?\d[\d,]*(?:\.\d+)?")


def facts() -> dict[str, Any]:
    """The numbers the report may use: per-language rows and the live summary from the exports."""
    langs = json.loads((EXPORT / "v1" / "languages.json").read_text("utf-8"))
    metrics = json.loads((EXPORT / "v1" / "pipeline_metrics.json").read_text("utf-8"))
    rows = sorted(langs["rows"], key=lambda r: -r["items_covered"])
    return {
        "reference_set": rows[0].get("reference_set") if rows else None,
        "languages": [
            {k: r[k] for k in ("language", "items_covered", "coverage_pct", "median_lag_days")}
            for r in rows
        ],  # fmt: skip
        "live_events_total": metrics["totals"]["events"],
        "generated_at": langs["generated_at"][:10],
    }


def _allowed(f: dict[str, Any]) -> set[str]:
    out: set[str] = set()

    def walk(v: Any) -> None:
        if isinstance(v, dict):
            for x in v.values():
                walk(x)
        elif isinstance(v, list):
            for x in v:
                walk(x)
        elif isinstance(v, int | float) and not isinstance(v, bool):
            for form in (str(v), f"{v:,}", str(round(v)), f"{round(v):,}", f"{v:.1f}"):
                out.add(form.replace(",", ""))
        elif isinstance(v, str):
            out.update(n.replace(",", "") for n in NUM.findall(v))

    walk(f)
    return out


def validate(text: str, f: dict[str, Any]) -> list[str]:
    """Numbers in the prose that are not in the facts (empty list = valid)."""
    allowed = _allowed(f)
    return [n for n in NUM.findall(text) if n.replace(",", "") not in allowed]


def write(out: Path = EXPORT / "v1" / "report.json") -> dict[str, Any] | None:
    key = os.environ.get("OPENROUTER_API_KEY")
    if not key:
        print("OPENROUTER_API_KEY not set: no report")
        return None
    if not MODEL.endswith(":free"):
        raise RuntimeError(f"spend cap $0: model {MODEL} is not a free model")
    f = facts()
    digest = hashlib.sha256(json.dumps(f, sort_keys=True).encode()).hexdigest()[:16]
    cache = DATA / "narrate" / f"{digest}.json"
    if cache.exists():
        doc = json.loads(cache.read_text("utf-8"))
    else:
        prompt = (
            "Write a short weekly report (120-180 words, plain prose, no headings) on how "
            "new knowledge reaches Wikipedia language editions. Use ONLY numbers that appear "
            "in the JSON below, exactly as written; do not compute, round differently or add "
            "numbers. Say that lag is measured on Wikipedia only.\n" + json.dumps(f)
        )
        r = httpx.post(URL, timeout=120, headers={"Authorization": f"Bearer {key}"},
                       json={"model": MODEL, "messages": [{"role": "user", "content": prompt}],
                             "temperature": 0.2})  # fmt: skip
        r.raise_for_status()
        text = r.json()["choices"][0]["message"]["content"].strip()
        bad = validate(text, f)
        doc = {"text": text, "model": MODEL, "facts_sha": digest, "invalid_numbers": bad,
               "generated_at": datetime.now(UTC).isoformat()}  # fmt: skip
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(doc), "utf-8")
    if doc["invalid_numbers"]:
        print(f"report rejected: numbers not in the facts: {doc['invalid_numbers'][:10]}")
        return None
    out.write_text(json.dumps(doc, ensure_ascii=False), "utf-8")
    return doc
