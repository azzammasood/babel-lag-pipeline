"""One small fixture test per parser (speed mode)."""

from datetime import date
from pathlib import Path

from babel_lag.parsers import history, pageviews, sitelinks

FIX = Path(__file__).parent / "fixtures"


def test_history_parser() -> None:
    q: list = []
    lines = (FIX / "history_sample.tsv").read_text("utf-8").splitlines(keepends=True)
    rows = [r for t in history.parse(lines, q) for r in t.to_pylist()]
    assert len(rows) == 6 and len(q) == 1 and "columns" in q[0]["reason"]
    assert rows[0]["page_id"] == 3 and rows[0]["rev_parent_id"] == 0 and not rows[0]["is_bot"]
    assert rows[2]["is_bot"] and rows[0]["rev_ts"].year == 2003
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
    assert len(rows) == 4 and not q
    assert {"item_id": 5, "wiki": "zuwiki", "page_title": "Ikhasi_Elikhulu"} in rows
    assert rows[-1]["page_title"] == "O'Brien_\\_test"
