"""One small fixture test per parser (speed mode)."""

from datetime import date
from pathlib import Path

import pyarrow.parquet as pq

from babel_lag.parsers import history, pageviews, sitelinks

FIX = Path(__file__).parent / "fixtures"


def test_history_parser(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("BABEL_EDITOR_SALT", "test-salt")
    q: list = []
    out = tmp_path / "h.parquet"
    n = history.parse_file(FIX / "history_sample.tsv", out, q,
                           {"source_file": "f", "source_sha256": "0" * 64,
                            "fetched_at": "2026-11-11T00:00:00+00:00"})  # fmt: skip
    rows = pq.read_table(out).to_pylist()
    assert n == len(rows) == 6 and len(q) == 1 and q[0]["line"] == 8
    assert rows[0]["page_id"] == 3 and rows[0]["rev_parent_id"] == 0 and rows[0]["is_bot"] is False
    assert all(isinstance(r[k], bool) for r in rows for k in ("is_reverted", "page_is_deleted"))
    assert rows[2]["is_bot"] and rows[0]["rev_ts"].year == 2003 and rows[0]["rev_ts"].hour == 1
    assert len(rows[0]["editor_hash"]) == 16 and rows[0]["editor_hash"] != rows[1]["editor_hash"]
    assert pq.read_schema(out).names == [*history.SCHEMA.names, "source_file", "source_sha256",
                                         "fetched_at"]  # fmt: skip
    assert "SecretEditorName" not in str(rows) and "203.0.113.42" not in str(rows)


def test_pageview_parser() -> None:
    q: list = []
    with (FIX / "pageviews_sample.txt").open("rb") as fh:
        t = pageviews.parse(
            fh, date(2026, 11, 10), {"ur.wikipedia": "urwiki", "zu.wikipedia": "zuwiki"}, q
        )
    rows = {(r["wiki"], r["page_id"]): r["views"] for r in t.to_pylist()}
    assert rows == {("urwiki", 101): 12, ("urwiki", None): 2, ("zuwiki", 3): 11}
    assert len(q) == 1


def test_sitelinks_parser() -> None:
    q: list = []
    with (FIX / "sitelinks_sample.sql").open("rb") as fh:
        rows = sitelinks.parse(fh, {"urwiki", "zuwiki"}, q).to_pylist()
    assert len(rows) == 5 and not q  # both INSERT layouts
    assert {"item_id": 5, "wiki": "zuwiki", "page_title": "Ikhasi_Elikhulu"} in rows
    assert rows[3]["page_title"] == "O'Brien_\\_test" and rows[-1]["item_id"] == 11
