"""
Shared JSON parsing utilities for LLM responses.
LLM output often includes markdown fences or wraps arrays in objects —
these helpers handle all the common cases robustly.
"""
import json
import re
from typing import Optional


def parse_json_array(text: str) -> Optional[list]:
    """
    Extract a JSON array from raw LLM output.
    Handles: bare arrays, markdown code fences, dicts wrapping arrays.
    Returns None if no array can be extracted.
    """
    if not text:
        return None
    stripped = text.strip()
    # Strip markdown code fences
    if stripped.startswith("```"):
        stripped = stripped.split("\n", 1)[-1]
        stripped = stripped.rsplit("```", 1)[0].strip()
    # Try direct parse
    try:
        data = json.loads(stripped)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            # Check common wrapper keys first, then any list value
            for key in ("passages", "results", "items", "data", "classifications", "extractions"):
                val = data.get(key)
                if isinstance(val, list):
                    return val
            for val in data.values():
                if isinstance(val, list):
                    return val
        return None
    except json.JSONDecodeError:
        pass
    # Fallback: find first '[' … last ']' in the text
    start = stripped.find("[")
    end = stripped.rfind("]")
    if start != -1 and end > start:
        try:
            data = json.loads(stripped[start:end + 1])
            if isinstance(data, list):
                return data
        except json.JSONDecodeError:
            pass
    return None


def parse_json_object(text: str) -> Optional[dict]:
    """Extract a JSON object from raw LLM output."""
    if not text:
        return None
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        return None
