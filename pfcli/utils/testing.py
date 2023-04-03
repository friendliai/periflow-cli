"""Utils for testing."""

from __future__ import annotations

from functools import reduce
from typing import Any, Dict, List, Optional

import yaml
from typing_extensions import TypeAlias

NestedDict: TypeAlias = Dict[str, Any]


def merge_dict(
    a: NestedDict, b: NestedDict, path: Optional[List[str]] = None
) -> NestedDict:
    if path is None:
        path = []

    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge_dict(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            else:
                raise RuntimeError(f"Conflict at {'.'.join(path + [str(key)])}")
        else:
            a[key] = b[key]
    return a


def merge_dicts(dicts: List[NestedDict]) -> NestedDict:
    d = {}
    d = reduce(merge_dict, [d, *dicts])
    return d


def merge_yaml_strings(strings: List[str]) -> str:
    """Merge two yaml strings into one.

    Args:
        strings (List[str]): YAML strings to merge into one.

    Returns:
        str: A merged YAML string

    """
    dicts = [yaml.safe_load(s) or {} for s in strings]
    d = merge_dicts(dicts)
    return yaml.safe_dump(d)
