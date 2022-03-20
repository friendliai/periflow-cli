import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import typer

from pfcli import autoauth

# Variables
periflow_api_server = "https://api-dev.friendli.ai/api/"
periflow_ws_server = "wss://api-ws-dev.friendli.ai/ws/"


def datetime_to_pretty_str(past: Optional[datetime], long_list: bool):
    cur = datetime.now().astimezone()
    delta = cur - past
    if long_list:
        if delta < timedelta(minutes=1):
            return f'{delta.seconds % 60}s ago'
        if delta < timedelta(hours=1):
            return f'{round((delta.seconds % 3600) / 60)}m {delta.seconds % 60}s ago'
        elif delta < timedelta(days=1):
            return f'{delta.seconds // 3600}h {round((delta.seconds % 3600) / 60)}m {delta.seconds % 60}s ago'
        elif delta < timedelta(days=3):
            return f'{delta.days}d {delta.seconds // 3600}h ' \
                   f'{round((delta.seconds % 3600) / 60)}m ago'
        else:
            return past.astimezone(tz=cur.tzinfo).strftime("%Y-%d-%m %H:%M:%S")
    else:
        if delta < timedelta(hours=1):
            return f'{round((delta.seconds % 3600) / 60)} mins ago'
        elif delta < timedelta(days=1):
            return f'{round(delta.seconds / 3600)} hours ago'
        else:
            return f'{delta.days + round(delta.seconds / (3600 * 24))} days ago'


def timedelta_to_pretty_str(start: datetime, finish: datetime, long_list: bool):
    delta = finish - start
    if long_list:
        if delta < timedelta(minutes=1):
            return f'{(delta.seconds % 60)}s'
        if delta < timedelta(hours=1):
            return f'{(delta.seconds % 3600) // 60}m {(delta.seconds % 60)}s'
        elif delta < timedelta(days=1):
            return f'{delta.seconds // 3600}h {(delta.seconds % 3600) // 60}m {(delta.seconds % 60)}s'
        else:
            return f'{delta.days}d {delta.seconds // 3600}h ' \
                   f'{(delta.seconds % 3600) // 60}m {delta.seconds % 60}s'
    else:
        if delta < timedelta(hours=1):
            return f'{round((delta.seconds % 3600) / 60)} mins'
        elif delta < timedelta(days=1):
            return f'{round(delta.seconds / 3600)} hours'
        else:
            return f'{delta.days + round(delta.seconds / (3600 * 24))} days'


def get_uri(path):
    return periflow_api_server + path


def get_wss_uri(path):
    return periflow_ws_server + path


def secho_error_and_exit(text: str, color=typer.colors.RED):
    typer.secho(text, err=True, fg=color)
    raise typer.Exit(1)


def get_group_id() -> int:
    r = autoauth.get(get_uri("user/group/"))
    if r.status_code != 200:
        secho_error_and_exit(f"Cannot acquire group info.")
    groups = r.json()["results"]
    if len(groups) == 0:
        secho_error_and_exit("You are not assigned to any group... Please contact admin")
    if len(groups) > 1:
        secho_error_and_exit(
            "Currently we do not support users with more than two groups... Please contact admin")
    return groups[0]['id']


def get_remaining_terminal_columns(occupied: int) -> int:
    return os.get_terminal_size().columns - occupied


def utc_to_local(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc).astimezone(tz=None)


def datetime_to_simple_string(dt: datetime) -> str:
    return dt.strftime("%Y-%d-%m %H:%M:%S")
