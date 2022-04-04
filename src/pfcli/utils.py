# Copyright (C) 2021 FriendliAI

"""PeriFlow CLI Shared Utilities"""

import os
import zipfile

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from subprocess import CalledProcessError, check_call

import typer

from pfcli.service import (
    CloudType,
    StorageType,
    storage_region_map,
    cloud_region_map
)

# Variables
periflow_api_server = "https://api-dev.friendli.ai/api/"
periflow_ws_server = "wss://api-ws-dev.friendli.ai/ws/"


def datetime_to_pretty_str(past: Optional[datetime], long_list: bool = False):
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
            return past.astimezone(tz=cur.tzinfo).strftime("%Y-%m-%d %H:%M:%S")
    else:
        if delta < timedelta(hours=1):
            return f'{round((delta.seconds % 3600) / 60)} mins ago'
        elif delta < timedelta(days=1):
            return f'{round(delta.seconds / 3600)} hours ago'
        else:
            return f'{delta.days + round(delta.seconds / (3600 * 24))} days ago'


def timedelta_to_pretty_str(start: datetime, finish: datetime, long_list: bool = False):
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


def get_uri(path: str):
    return periflow_api_server + path


def get_wss_uri(path: str):
    return periflow_ws_server + path


def secho_error_and_exit(text: str, color: str = typer.colors.RED):
    typer.secho(text, err=True, fg=color)
    raise typer.Exit(1)


def get_remaining_terminal_columns(occupied: int) -> int:
    return os.get_terminal_size().columns - occupied


def utc_to_local(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc).astimezone(tz=None)


def datetime_to_simple_string(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


@contextmanager
def zip_dir(dir_path: Path, zip_path: Path):
    typer.secho("Compressing training directory...", fg=typer.colors.MAGENTA)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as zip_file:
        for e in dir_path.rglob("*"):
            zip_file.write(e, e.relative_to(dir_path.parent))
    typer.secho("Compressing finished... Now uploading...", fg=typer.colors.MAGENTA)
    try:
        yield zip_path.open("rb")
    finally:
        zip_path.unlink()


def open_editor(path: str, editor: Optional[str] = None):
    default_editor = editor or get_default_editor()
    try:
        check_call([default_editor, path])
    except CalledProcessError:
        typer.secho("", fg=typer.colors.RED)


def get_default_editor() -> str:
    return os.environ.get("PERIFLOW_CLI_EDITOR", "vim")


def validate_storage_region(vendor: StorageType, region: str):
    available_regions = storage_region_map[vendor]
    if region not in available_regions:
        secho_error_and_exit(
            f"'{region}' is not supported region for {vendor}. Please choose another one in {available_regions}."
        )


def validate_cloud_region(vendor: CloudType, region: str):
    available_regions = cloud_region_map[vendor]
    if region not in available_regions:
        secho_error_and_exit(
            f"'{region}' is not supported region for {vendor}. Please choose another one in {available_regions}."
        )
