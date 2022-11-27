# Copyright (c) 2022-present, FriendliAI Inc. All rights reserved.

"""PeriFlow CLI Formatting Utilities"""

from datetime import datetime, timedelta, timezone
import os

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


def secho_error_and_exit(text: str, color: str = typer.colors.RED):
    typer.secho(text, err=True, fg=color)
    raise typer.Exit(1)


def get_remaining_terminal_columns(occupied: int) -> int:
    return os.get_terminal_size().columns - occupied


def utc_to_local(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc).astimezone(tz=None)


def datetime_to_simple_string(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")
