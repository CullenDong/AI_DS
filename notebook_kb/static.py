from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field

S3_URI_RE = re.compile(r"s3://[A-Za-z0-9._/\-]+")
FILE_PATH_RE = re.compile(r"[\w./\-]+\.(csv|parquet|tsv|xlsx?|json|jsonl|feather)\b", re.I)
SQL_RE = re.compile(r"\b(SELECT|FROM|JOIN|WHERE|GROUP\s+BY|ORDER\s+BY)\b", re.I)
MAGIC_RE = re.compile(r"^\s*(%{1,2}[A-Za-z_][A-Za-z0-9_]*)", re.M)
ATHENA_RE = re.compile(r"\b(wr\.athena\.\w+|awswrangler\.athena\.\w+)")
REDSHIFT_RE = re.compile(r"boto3\.client\(\s*['\"]redshift[^'\"]*['\"]")


@dataclass
class Signals:
    s3_uris: list[str] = field(default_factory=list)
    file_paths: list[str] = field(default_factory=list)
    sql_snippets: list[str] = field(default_factory=list)
    athena_calls: list[str] = field(default_factory=list)
    redshift_calls: list[str] = field(default_factory=list)
    magics: list[str] = field(default_factory=list)
    fn_defs: list[str] = field(default_factory=list)
    parse_errors: int = 0

    def to_dict(self) -> dict:
        return {
            "s3_uris": self.s3_uris,
            "file_paths": self.file_paths,
            "sql_snippets": self.sql_snippets,
            "athena_calls": self.athena_calls,
            "redshift_calls": self.redshift_calls,
            "magics": self.magics,
            "fn_defs": self.fn_defs,
            "parse_errors": self.parse_errors,
        }


def extract_imports(code: str) -> list[str]:
    imports: set[str] = set()
    try:
        tree = ast.parse(_strip_magics(code))
    except SyntaxError:
        return []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                imports.add(n.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.add(node.module.split(".")[0])
    return sorted(imports)


def extract_signals(code_per_cell: list[str]) -> Signals:
    sig = Signals()
    for cell_src in code_per_cell:
        sig.s3_uris.extend(_dedup_preserve(S3_URI_RE.findall(cell_src)))
        sig.file_paths.extend(m.group(0) for m in FILE_PATH_RE.finditer(cell_src))
        sig.athena_calls.extend(ATHENA_RE.findall(cell_src))
        sig.redshift_calls.extend(REDSHIFT_RE.findall(cell_src))
        sig.magics.extend(MAGIC_RE.findall(cell_src))

        if SQL_RE.search(cell_src):
            for lit in _string_literals(cell_src):
                if SQL_RE.search(lit):
                    sig.sql_snippets.append(lit[:500])

        try:
            tree = ast.parse(_strip_magics(cell_src))
        except SyntaxError:
            sig.parse_errors += 1
            continue
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                sig.fn_defs.append(node.name)

    sig.s3_uris = _dedup_preserve(sig.s3_uris)
    sig.file_paths = _dedup_preserve(sig.file_paths)
    sig.athena_calls = _dedup_preserve(sig.athena_calls)
    sig.redshift_calls = _dedup_preserve(sig.redshift_calls)
    sig.magics = _dedup_preserve(sig.magics)
    sig.fn_defs = _dedup_preserve(sig.fn_defs)
    return sig


def _strip_magics(code: str) -> str:
    # Comment out IPython magics so ast can parse the cell.
    out_lines = []
    for line in code.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("%") or stripped.startswith("!"):
            out_lines.append("# " + line)
        else:
            out_lines.append(line)
    return "\n".join(out_lines)


def _string_literals(code: str) -> list[str]:
    try:
        tree = ast.parse(_strip_magics(code))
    except SyntaxError:
        return []
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            out.append(node.value)
    return out


def _dedup_preserve(items):
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out
