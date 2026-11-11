"""Minimal .env loader (no dependency). Real environment variables win over the file."""

from __future__ import annotations

import os

from babel_lag.paths import ROOT


def load_dotenv() -> None:
    path = ROOT / ".env"
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        if value and key.strip() not in os.environ:
            os.environ[key.strip()] = value.strip()
