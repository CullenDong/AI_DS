"""Shared in-memory state across tool calls in one agent run."""
from __future__ import annotations

import pandas as pd

FRAMES: dict[str, pd.DataFrame] = {}
