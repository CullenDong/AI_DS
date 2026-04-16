"""AI_DS agent entry point.

Usage:
    python -m agent.main "Summarize data/sample.csv"
"""
from __future__ import annotations

import os
import sys

from anthropic import Anthropic

from agent.loop import run_agent


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python -m agent.main '<task description>'", file=sys.stderr)
        return 2

    task = sys.argv[1]
    client = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    run_agent(client, task)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
