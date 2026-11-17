"""Weekly "Babel report" (brief section 5): an LLM narrates numbers already computed in SQL; it
never computes. Spend cap $0: only OpenRouter models whose id ends in `:free` are accepted.

Validation: every number in the prose must appear in the facts handed to the model (after
rounding to the precision shown), every language code must be in the facts, and ranking words
are allowed only in a sentence naming one of the rankings computed here (and only for languages
on it). A report failing any check is rejected, not cached and not published.
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

# Free models only, tried in order (the free list changes; llama-3.3-70b:free was withdrawn).
MODELS = os.environ.get(
    "BABEL_REPORT_MODELS", "google/gemma-4-31b-it:free,nvidia/nemotron-3-ultra-550b-a55b:free"
).split(",")
URL = "https://openrouter.ai/api/v1/chat/completions"
NUM = re.compile(r"(?<![\w.])-?\d[\d,]*(?:\.\d+)?")


MIN_ITEMS = 10_000  # lag rankings only among languages with at least this many covered items
RANKS = {  # list key -> phrase a sentence must contain to rank languages
    "largest_by_covered_items": "largest by covered items",
    "shortest_median_lag": "shortest median lag",
    "longest_median_lag": "longest median lag",
}
RANK_WORD = re.compile(
    r"(?<!at )\b(smallest|largest|shortest|longest|fastest|slowest|highest|lowest|most|least|"
    r"fewest|leads?|leading|top|biggest|best|worst|first|second|third|ranks?|ranked|ahead|"
    r"behind)\b",
    re.IGNORECASE,
)
# numbers in words cannot be checked against the facts, so they are not allowed at all
WORD_NUM = re.compile(
    r"\b(two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|dozens?|hundreds?|"
    r"thousands?|millions?|billions?|half|twice|double|triple)\b",
    re.IGNORECASE,
)
CODE = re.compile(r"\(([a-z][a-z-]*)\)")


def facts() -> dict[str, Any]:
    """What the report may say: rankings computed here from the export, never by the model."""
    langs = json.loads((EXPORT / "v1" / "languages.json").read_text("utf-8"))
    metrics = json.loads((EXPORT / "v1" / "pipeline_metrics.json").read_text("utf-8"))
    keep = ("language", "items_covered", "coverage_pct", "median_lag_days")
    rows = [{k: r[k] for k in keep} for r in langs["rows"]]
    by_items = sorted(rows, key=lambda r: (-r["items_covered"], r["language"]))
    lagged = sorted(
        (r for r in rows if r["items_covered"] >= MIN_ITEMS and r["median_lag_days"] is not None),
        key=lambda r: (r["median_lag_days"], r["language"]),
    )
    return {
        "reference_set": langs["rows"][0].get("reference_set") if rows else None,
        "languages_total": len(rows),
        "min_items_for_lag_ranking": MIN_ITEMS,
        "largest_by_covered_items": by_items[:6],
        "shortest_median_lag": lagged[:3],
        "longest_median_lag": lagged[::-1][:3],
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
    """Problems in the prose (empty list = valid): numbers not in the facts, language codes not
    in the facts, and ranking words outside a sentence that names a published ranking, or that
    rank a language not on that ranking."""
    allowed = _allowed(f)
    bad = [n for n in NUM.findall(text) if n.replace(",", "") not in allowed]
    bad += [f"number in words: {w}" for w in WORD_NUM.findall(text)]
    lists = {k: {r["language"] for r in f[k]} for k in RANKS}
    known = set().union(*lists.values())
    for sent in re.split(r"(?<=[.!?])\s+", text):
        codes = set(CODE.findall(sent)) | {
            c for c in known if re.search(rf"(?<![\w-]){re.escape(c)}(?![\w-])", sent)
        }
        bad += [f"unknown language ({c})" for c in sorted(codes - known)]
        if RANK_WORD.search(sent):
            named = [k for k, phrase in RANKS.items() if phrase in sent.lower()]
            if not codes:
                bad.append(f"ranking without language codes: {sent[:80]}")
            elif not named:
                bad.append(f"ranking without a published list: {sent[:80]}")
            elif codes - set().union(*(lists[k] for k in named)):
                bad.append(f"language not on the named ranking: {sent[:80]}")
    return bad


def _envelope(doc: dict[str, Any], langs: dict[str, Any]) -> dict[str, Any]:
    from babel_lag.export.site import CORRECTIONS_URL, ENVELOPE_VERSION, SOURCES, validate

    env = {
        "envelope_version": ENVELOPE_VERSION,
        "dataset": "report",
        "dataset_version": "v1",
        "generated_at": doc["generated_at"],
        "produced_by": langs["produced_by"],  # the run whose numbers the prose narrates
        "window": {"start": None, "end": None, "periods": []},
        "sources": [SOURCES[s] for s in ("history", "sitelinks", "pageviews", "eventstreams")],
        "privacy": {"level": "language", "people": "aggregate-only"},
        "corrections": CORRECTIONS_URL,
        "row_count": 1,
        "rows": [{"text": doc["text"], "model": doc["model"], "facts_sha": doc["facts_sha"],
                  "facts_date": doc["facts_date"], "ai_generated": True,
                  "numbers_validated": True}],
    }  # fmt: skip
    validate(env)
    return env


def write(out: Path = EXPORT / "v1" / "report.json", tries: int = 3) -> dict[str, Any] | None:
    key = os.environ.get("OPENROUTER_API_KEY")
    if not key:
        print("OPENROUTER_API_KEY not set: no report")
        return None
    if not all(m.endswith(":free") for m in MODELS):
        raise RuntimeError(f"spend cap $0: not all of {MODELS} are free models")
    langs = json.loads((EXPORT / "v1" / "languages.json").read_text("utf-8"))
    f = facts()
    digest = hashlib.sha256(json.dumps(f, sort_keys=True).encode()).hexdigest()[:16]
    cache = DATA / "narrate" / f"{digest}.json"
    doc = json.loads(cache.read_text("utf-8")) if cache.exists() else None
    prompt = (
        "Write a short weekly report (120-180 words, plain prose, no headings) on how "
        "new knowledge reaches Wikipedia language editions. Use ONLY numbers that appear "
        "in the JSON below, exactly as written; do not compute, round differently or add "
        "numbers, and write no dates. coverage_pct is a percentage (write %); counts may use "
        "thousands commas. Name every language by its full English name followed by its code "
        "in brackets, e.g. English (en); if unsure of a name, use the code alone. "
        "To rank languages, use a list "
        "below and say its name in words (largest by covered items, shortest median lag, "
        "longest median lag; the lag lists only include languages with at least "
        "min_items_for_lag_ranking covered items); never rank otherwise. No causes, "
        "explanations or advice. Say that lag is measured on Wikipedia only.\n" + json.dumps(f)
    )
    for attempt in range(tries * len(MODELS) if doc is None else 0):
        model = MODELS[attempt % len(MODELS)]
        r = httpx.post(URL, timeout=180, headers={"Authorization": f"Bearer {key}"},
                       json={"model": model, "messages": [{"role": "user", "content": prompt}],
                             "temperature": 0.2})  # fmt: skip
        if r.status_code != 200:  # rate limit or a withdrawn model: next model
            print(f"attempt {attempt + 1} ({model}): HTTP {r.status_code}")
            continue
        text = r.json()["choices"][0]["message"]["content"].strip()
        bad = validate(text, f)
        if bad:  # rejected prose is never cached or published; try again
            print(f"attempt {attempt + 1} ({model}): rejected: {bad[:5]}")
            continue
        doc = {"text": text, "model": model, "facts_sha": digest, "facts_date": f["generated_at"],
               "generated_at": datetime.now(UTC).isoformat()}  # fmt: skip
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(doc), "utf-8")
        break
    if doc is None:
        print("report rejected: no attempt passed validation; nothing published")
        out.unlink(missing_ok=True)  # never leave an older report behind as if current
        return None
    env = _envelope(doc, langs)
    out.write_text(json.dumps(env, ensure_ascii=False), "utf-8")
    return env
