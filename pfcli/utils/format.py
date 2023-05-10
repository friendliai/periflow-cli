# Copyright (c) 2022-present, FriendliAI Inc. All rights reserved.

"""PeriFlow CLI Formatting Utilities"""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from typing import NoReturn, Optional

import typer


def datetime_to_pretty_str(past: datetime, long_list: bool = False):
    cur = datetime.now().astimezone()
    delta = cur - past
    if long_list:
        if delta < timedelta(minutes=1):
            return f"{delta.seconds % 60}s ago"
        if delta < timedelta(hours=1):
            return f"{round((delta.seconds % 3600) / 60)}m {delta.seconds % 60}s ago"
        elif delta < timedelta(days=1):
            return f"{delta.seconds // 3600}h {round((delta.seconds % 3600) / 60)}m {delta.seconds % 60}s ago"
        elif delta < timedelta(days=3):
            return (
                f"{delta.days}d {delta.seconds // 3600}h "
                f"{round((delta.seconds % 3600) / 60)}m ago"
            )
        else:
            return past.astimezone(tz=cur.tzinfo).strftime("%Y-%m-%d %H:%M:%S")
    else:
        if delta < timedelta(hours=1):
            return f"{round((delta.seconds % 3600) / 60)} mins ago"
        elif delta < timedelta(days=1):
            return f"{round(delta.seconds / 3600)} hours ago"
        else:
            return f"{delta.days + round(delta.seconds / (3600 * 24))} days ago"


def timedelta_to_pretty_str(delta: timedelta, long_list: bool = False):
    if long_list:
        if delta < timedelta(minutes=1):
            return f"{(delta.seconds % 60)}s"
        if delta < timedelta(hours=1):
            return f"{(delta.seconds % 3600) // 60}m {(delta.seconds % 60)}s"
        elif delta < timedelta(days=1):
            return f"{delta.seconds // 3600}h {(delta.seconds % 3600) // 60}m {(delta.seconds % 60)}s"
        else:
            return (
                f"{delta.days}d {delta.seconds // 3600}h "
                f"{(delta.seconds % 3600) // 60}m {delta.seconds % 60}s"
            )
    else:
        if delta < timedelta(hours=1):
            return f"{round((delta.seconds % 3600) / 60)} mins"
        elif delta < timedelta(days=1):
            return f"{round(delta.seconds / 3600)} hours"
        else:
            return f"{delta.days + round(delta.seconds / (3600 * 24))} days"


def secho_error_and_exit(text: str, color: str = typer.colors.RED) -> NoReturn:
    typer.secho(text, err=True, fg=color)
    raise typer.Exit(1)


def get_remaining_terminal_columns(occupied: int) -> int:
    return os.get_terminal_size().columns - occupied


def utc_to_local(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc).astimezone(tz=None)


def datetime_to_simple_string(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _regex_parse(pattern: str, s: str) -> Optional[str]:
    match = re.search(pattern, s)
    if match:
        return match.group()
    return None


def extract_datetime_part(s: str) -> Optional[str]:
    """Extracts the datetime portion in the format "YYYY-MM-DD--HH" from the input string `s`.

    Args:
        s: A string containing a datetime in the format "YYYY-MM-DD--HH".

    Returns:
        str: A string representing the datetime in the format "YYYY-MM-DD--HH", if found in the input string `s`. If the datetime is not found, None is returned.

    """
    pattern = r"\d{4}-\d{2}-\d{2}--\d{2}"
    return _regex_parse(pattern, s)


def extract_deployment_id_part(s: str) -> Optional[str]:
    """Extracts the deployment ID from the input string `s`.

    Args:
        s: A string containing a deployment ID.

    Returns:
        str: A parsed string. If the deployment ID is not found, None is returned.

    """
    pattern = r"periflow-deployment-\w{8}"
    return _regex_parse(pattern, s)
