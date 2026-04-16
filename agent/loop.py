"""Agent loop: send task to Claude, execute tool calls, feed results back."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from anthropic import Anthropic

from tools import registry

MODEL = "claude-opus-4-7"
MAX_TURNS = 20
PROMPT_PATH = Path(__file__).resolve().parent.parent / "prompts" / "system_prompt.md"


def _load_system_prompt() -> str:
    return PROMPT_PATH.read_text(encoding="utf-8")


def run_agent(client: Anthropic, task: str) -> None:
    system = _load_system_prompt()
    tools = registry.tool_schemas()
    messages: list[dict[str, Any]] = [{"role": "user", "content": task}]

    for _ in range(MAX_TURNS):
        resp = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            system=system,
            tools=tools,
            messages=messages,
        )

        messages.append({"role": "assistant", "content": resp.content})

        if resp.stop_reason != "tool_use":
            for block in resp.content:
                if getattr(block, "type", None) == "text":
                    print(block.text)
            return

        tool_results = []
        for block in resp.content:
            if getattr(block, "type", None) != "tool_use":
                continue
            result = registry.dispatch(block.name, block.input)
            tool_results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result,
                }
            )
        messages.append({"role": "user", "content": tool_results})

    print("[agent] hit MAX_TURNS without stop_reason=end_turn", flush=True)
