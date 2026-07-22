"""Tool registry — schemas sent to Claude and dispatch to Python impls."""
from __future__ import annotations

from typing import Any, Callable

from tools.agent_tools import data_tools, eda_tools, viz_tools

_TOOLS: dict[str, tuple[dict[str, Any], Callable[..., str]]] = {
    "load_dataframe": (
        {
            "name": "load_dataframe",
            "description": "Load a CSV/Parquet/Excel file into a named in-memory DataFrame. Returns shape and dtypes.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "File path relative to project root."},
                    "name": {"type": "string", "description": "Handle used to reference this DataFrame in later tool calls."},
                },
                "required": ["path", "name"],
            },
        },
        data_tools.load_dataframe,
    ),
    "describe": (
        {
            "name": "describe",
            "description": "Return summary statistics, dtypes, and missingness for a loaded DataFrame.",
            "input_schema": {
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "required": ["name"],
            },
        },
        eda_tools.describe,
    ),
    "value_counts": (
        {
            "name": "value_counts",
            "description": "Top value counts for a column.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "column": {"type": "string"},
                    "top": {"type": "integer", "default": 20},
                },
                "required": ["name", "column"],
            },
        },
        eda_tools.value_counts,
    ),
    "correlation": (
        {
            "name": "correlation",
            "description": "Pearson correlation matrix over numeric columns.",
            "input_schema": {
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "required": ["name"],
            },
        },
        eda_tools.correlation,
    ),
    "plot_histogram": (
        {
            "name": "plot_histogram",
            "description": "Save a histogram for a column to outputs/ and return the path.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "column": {"type": "string"},
                    "bins": {"type": "integer", "default": 30},
                },
                "required": ["name", "column"],
            },
        },
        viz_tools.plot_histogram,
    ),
    "query_redshift": (
        {
            "name": "query_redshift",
            "description": (
                "Run a read-only SQL query against Amazon Redshift and return a text preview "
                "(row count, columns, first N rows). Writes are blocked. Use this for exploration; "
                "use load_redshift_to_frame when you need to keep the result for later tool calls."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SELECT-only SQL query."},
                    "database": {"type": "string", "description": "Redshift database name."},
                    "bastion_ip": {
                        "type": "string",
                        "description": "Optional SSH bastion IP. Defaults to REDSHIFT_BASTION_IP env or the built-in default.",
                    },
                    "preview_rows": {"type": "integer", "default": 5},
                },
                "required": ["sql", "database"],
            },
        },
        data_tools.query_redshift,
    ),
    "load_redshift_to_frame": (
        {
            "name": "load_redshift_to_frame",
            "description": (
                "Run a read-only Redshift SQL query and store the full result as a named in-memory "
                "DataFrame for subsequent describe/value_counts/plot tool calls."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SELECT-only SQL query."},
                    "name": {"type": "string", "description": "Handle for the resulting DataFrame."},
                    "database": {"type": "string", "description": "Redshift database name."},
                    "bastion_ip": {"type": "string", "description": "Optional SSH bastion IP."},
                },
                "required": ["sql", "name", "database"],
            },
        },
        data_tools.load_redshift_to_frame,
    ),
}


def tool_schemas() -> list[dict[str, Any]]:
    return [schema for schema, _ in _TOOLS.values()]


def dispatch(name: str, args: dict[str, Any]) -> str:
    if name not in _TOOLS:
        return f"error: unknown tool {name!r}"
    _, fn = _TOOLS[name]
    try:
        return fn(**args)
    except Exception as exc:  # surface to the model so it can recover
        return f"error: {type(exc).__name__}: {exc}"
