from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import nbformat


@dataclass
class Cell:
    cell_type: str  # "code" | "markdown"
    source: str


@dataclass
class ParsedNotebook:
    path: Path
    cells: list[Cell]

    @property
    def n_code(self) -> int:
        return sum(1 for c in self.cells if c.cell_type == "code")

    @property
    def n_md(self) -> int:
        return sum(1 for c in self.cells if c.cell_type == "markdown")

    @property
    def code_text(self) -> str:
        return "\n".join(c.source for c in self.cells if c.cell_type == "code")

    def loc(self) -> int:
        return sum(
            1
            for c in self.cells
            if c.cell_type == "code"
            for line in c.source.splitlines()
            if line.strip()
        )


def parse_notebook(path: Path) -> ParsedNotebook:
    nb = nbformat.read(path, as_version=4)
    cells: list[Cell] = []
    for c in nb.cells:
        ctype = c.get("cell_type")
        if ctype not in {"code", "markdown"}:
            continue
        src = c.get("source", "")
        if isinstance(src, list):
            src = "".join(src)
        cells.append(Cell(cell_type=ctype, source=src))
    return ParsedNotebook(path=path, cells=cells)
