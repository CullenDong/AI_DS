"""
Runs inside the SageMaker Processing container.

Takes --target <filename>, looks for it under /opt/ml/processing/input/code,
and executes it. Supports .py (run directly) and .ipynb (run via papermill).
Outputs are written to /opt/ml/processing/output.
"""

from __future__ import annotations

import argparse
import os
import runpy
import shutil
import subprocess
import sys
from pathlib import Path

INPUT_DIR = Path("/opt/ml/processing/input/code")
DATA_DIR = Path("/opt/ml/processing/input/data")  # optional; present if submit passes data input
OUTPUT_DIR = Path("/opt/ml/processing/output")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True, help="filename under input/code (e.g. my.py or my.ipynb)")
    args, passthrough = parser.parse_known_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    target = INPUT_DIR / args.target
    if not target.exists():
        print(f"ERROR: target not found: {target}", file=sys.stderr)
        print("Contents of input dir:", list(INPUT_DIR.rglob("*")), file=sys.stderr)
        return 2

    if target.suffix == ".py":
        return _run_py(target, passthrough)
    if target.suffix == ".ipynb":
        return _run_ipynb(target, passthrough)

    print(f"ERROR: unsupported file type: {target.suffix}", file=sys.stderr)
    return 2


def _run_py(path: Path, extra_argv: list[str]) -> int:
    # Copy to OUTPUT_DIR so any sibling artifacts written next to the script end up uploaded.
    work = OUTPUT_DIR / "work"
    work.mkdir(parents=True, exist_ok=True)
    dest = work / path.name
    shutil.copy(path, dest)
    os.chdir(work)

    sys.argv = [str(dest)] + extra_argv
    print(f"[run_entry] executing {dest} with argv={sys.argv[1:]}")
    try:
        runpy.run_path(str(dest), run_name="__main__")
    except SystemExit as e:
        return int(e.code or 0)
    return 0


def _run_ipynb(path: Path, extra_argv: list[str]) -> int:
    # papermill is installed via requirements.txt.
    out_path = OUTPUT_DIR / path.name.replace(".ipynb", ".executed.ipynb")
    cmd = [
        sys.executable,
        "-m",
        "papermill",
        str(path),
        str(out_path),
        "--kernel",
        "python3",
        "--log-output",
        "--stdout-file",
        str(OUTPUT_DIR / "stdout.log"),
        "--stderr-file",
        str(OUTPUT_DIR / "stderr.log"),
    ]
    cmd += extra_argv
    print(f"[run_entry] executing papermill: {' '.join(cmd)}")
    return subprocess.call(cmd)


if __name__ == "__main__":
    sys.exit(main())
