from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path

from . import store
from .config import REPO_ROOT, Config
from .parse import parse_notebook

PROMPT_PATH = REPO_ROOT / "prompts" / "notebook_summarize.md"

REQUIRED_FIELDS = (
    "purpose",
    "inputs",
    "outputs",
    "key_steps",
    "data_sources",
    "domain_tags",
    "notable_functions",
    "confidence",
)


@dataclass
class SummarizeReport:
    attempted: int = 0
    ok: int = 0
    low_confidence: int = 0
    errors: int = 0
    tokens_in: int = 0
    tokens_out: int = 0
    cache_read: int = 0
    cache_write: int = 0

    def __str__(self) -> str:
        return (
            f"attempted={self.attempted} ok={self.ok} low={self.low_confidence} "
            f"errors={self.errors} in={self.tokens_in} out={self.tokens_out} "
            f"cache_read={self.cache_read} cache_write={self.cache_write}"
        )


def summarize(
    cfg: Config,
    *,
    limit: int | None = None,
    model: str | None = None,
) -> SummarizeReport:
    model = model or cfg.model
    system_prompt = PROMPT_PATH.read_text()
    from anthropic import Anthropic  # lazy so ingest/show/stats work without the SDK
    client = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    report = SummarizeReport()

    store.init_db(cfg.db_path)
    with store.connect(cfg.db_path) as conn:
        targets = store.notebooks_needing_summary(conn, limit=limit)
        total = len(targets)
        for i, nb in enumerate(targets, 1):
            report.attempted += 1
            try:
                parsed = parse_notebook(Path(nb["path"]))
            except Exception as e:
                print(f"[{i}/{total}] {nb['rel_path']}  parse_error: {e}")
                report.errors += 1
                continue

            nb_text = _render_notebook(
                parsed,
                per_cell_cap=cfg.per_cell_char_cap,
                total_cap=cfg.max_notebook_chars,
            )

            parsed_json, raw, usage, err = _call_with_retry(
                client, model, cfg.max_tokens_out, system_prompt, nb_text
            )
            if usage:
                report.tokens_in += usage.get("input", 0)
                report.tokens_out += usage.get("output", 0)
                report.cache_read += usage.get("cache_read", 0)
                report.cache_write += usage.get("cache_write", 0)

            if parsed_json is None:
                print(f"[{i}/{total}] {nb['rel_path']}  bad_json (stored as low)")
                store.upsert_summary(
                    conn,
                    notebook_id=nb["id"],
                    parsed={},
                    raw_json=raw or "",
                    based_on_sha=nb["sha256"],
                    model=model,
                    tokens_in=usage.get("input", 0) if usage else 0,
                    tokens_out=usage.get("output", 0) if usage else 0,
                    confidence="low",
                )
                report.low_confidence += 1
                continue

            confidence = parsed_json.get("confidence") or "medium"
            store.upsert_summary(
                conn,
                notebook_id=nb["id"],
                parsed=parsed_json,
                raw_json=raw or json.dumps(parsed_json),
                based_on_sha=nb["sha256"],
                model=model,
                tokens_in=usage.get("input", 0) if usage else 0,
                tokens_out=usage.get("output", 0) if usage else 0,
                confidence=confidence,
            )
            report.ok += 1
            print(
                f"[{i}/{total}] {nb['rel_path']}  "
                f"in={usage.get('input', 0) if usage else 0} "
                f"out={usage.get('output', 0) if usage else 0} "
                f"conf={confidence}"
            )

    return report


def _render_notebook(parsed, *, per_cell_cap: int, total_cap: int) -> str:
    parts: list[str] = []
    for cell in parsed.cells:
        tag = "MD" if cell.cell_type == "markdown" else "CODE"
        src = cell.source
        if len(src) > per_cell_cap:
            dropped = src.count("\n", per_cell_cap)
            src = src[:per_cell_cap] + f"\n[... truncated {dropped} lines ...]"
        parts.append(f"# {tag}:\n{src}")
    text = "\n\n".join(parts)

    if len(text) <= total_cap:
        return text

    # Keep all markdown + head/tail of code. Rebuild from cells.
    md_parts = [f"# MD:\n{c.source}" for c in parsed.cells if c.cell_type == "markdown"]
    code_cells = [c for c in parsed.cells if c.cell_type == "code"]
    head = code_cells[: max(3, len(code_cells) // 4)]
    tail = code_cells[-max(3, len(code_cells) // 4):]
    head_text = "\n\n".join(f"# CODE:\n{c.source}" for c in head)
    tail_text = "\n\n".join(f"# CODE:\n{c.source}" for c in tail)
    dropped_count = len(code_cells) - len(head) - len(tail)
    reduced = (
        "\n\n".join(md_parts)
        + "\n\n"
        + head_text
        + f"\n\n# [... {dropped_count} middle code cells truncated ...]\n\n"
        + tail_text
    )
    if len(reduced) > total_cap:
        reduced = reduced[:total_cap] + "\n[... truncated ...]"
    return reduced


def _call_with_retry(client, model, max_tokens, system_prompt, user_text):
    messages = [{"role": "user", "content": user_text}]
    raw, usage, err = _call(client, model, max_tokens, system_prompt, messages)
    if err is None:
        parsed = _parse_json(raw)
        if parsed is not None and _has_required(parsed):
            return parsed, raw, usage, None

    # Retry once with an explicit instruction.
    retry_messages = messages + [
        {"role": "assistant", "content": raw or ""},
        {
            "role": "user",
            "content": "Your previous response was not valid JSON conforming to the schema. Reply with JSON only, no prose, no code fences.",
        },
    ]
    raw2, usage2, err2 = _call(client, model, max_tokens, system_prompt, retry_messages)
    merged_usage = _merge_usage(usage, usage2)
    if err2 is None:
        parsed2 = _parse_json(raw2)
        if parsed2 is not None and _has_required(parsed2):
            return parsed2, raw2, merged_usage, None
    return None, raw2 or raw, merged_usage, err2 or err


def _call(client, model, max_tokens, system_prompt, messages):
    try:
        resp = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=[
                {
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=messages,
        )
    except Exception as e:
        return None, None, e

    text = "".join(
        b.text for b in resp.content if getattr(b, "type", None) == "text"
    )
    usage = {
        "input": getattr(resp.usage, "input_tokens", 0) or 0,
        "output": getattr(resp.usage, "output_tokens", 0) or 0,
        "cache_read": getattr(resp.usage, "cache_read_input_tokens", 0) or 0,
        "cache_write": getattr(resp.usage, "cache_creation_input_tokens", 0) or 0,
    }
    return text, usage, None


def _merge_usage(a, b):
    if not a:
        return b
    if not b:
        return a
    return {k: (a.get(k, 0) + b.get(k, 0)) for k in set(a) | set(b)}


_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.S)


def _parse_json(text: str | None):
    if not text:
        return None
    t = text.strip()
    try:
        return json.loads(t)
    except Exception:
        pass
    m = _JSON_FENCE_RE.search(t)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass
    start = t.find("{")
    end = t.rfind("}")
    if start != -1 and end > start:
        try:
            return json.loads(t[start : end + 1])
        except Exception:
            return None
    return None


def _has_required(obj) -> bool:
    if not isinstance(obj, dict):
        return False
    return all(f in obj for f in REQUIRED_FIELDS)
