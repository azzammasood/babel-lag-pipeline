"""Repository paths. Bulk data lives in data/ (git-ignored); override with BABEL_DATA."""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "config"
DATA = Path(os.environ.get("BABEL_DATA", ROOT / "data"))
