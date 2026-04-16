"""Smoke tests for data tools."""
from pathlib import Path

import pandas as pd

from tools import data_tools
from tools.state import FRAMES


def test_load_dataframe_csv(tmp_path: Path):
    p = tmp_path / "t.csv"
    pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}).to_csv(p, index=False)
    msg = data_tools.load_dataframe(str(p), "t")
    assert "loaded 't'" in msg
    assert FRAMES["t"].shape == (3, 2)
