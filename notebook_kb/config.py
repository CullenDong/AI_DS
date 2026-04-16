from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = REPO_ROOT / "configs" / "notebook_kb.yaml"


@dataclass
class Config:
    notebooks_root: Path
    db_path: Path
    model: str
    max_tokens_out: int
    max_notebook_chars: int
    per_cell_char_cap: int

    @classmethod
    def load(cls, path: Path | None = None) -> "Config":
        path = path or DEFAULT_CONFIG_PATH
        raw = yaml.safe_load(path.read_text())
        return cls(
            notebooks_root=_abs(raw["notebooks_root"]),
            db_path=_abs(raw["db_path"]),
            model=raw["model"],
            max_tokens_out=int(raw["max_tokens_out"]),
            max_notebook_chars=int(raw["max_notebook_chars"]),
            per_cell_char_cap=int(raw["per_cell_char_cap"]),
        )


def _abs(p: str | Path) -> Path:
    p = Path(p)
    return p if p.is_absolute() else (REPO_ROOT / p).resolve()
