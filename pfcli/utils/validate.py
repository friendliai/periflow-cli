# Copyright (c) 2022-present, FriendliAI Inc. All rights reserved.

"""PeriFlow CLI Validation Utilities"""

from datetime import datetime
from importlib.metadata import version
from typing import List, Optional

import typer

from pfcli.service import CloudType, cloud_region_map, StorageType, storage_region_map
from pfcli.utils.format import secho_error_and_exit
from pfcli.utils.version import get_latest_cli_version, is_latest_cli_version, PERIFLOW_CLI_NAME


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


def validate_datetime_format(datetime_str: Optional[str]) -> Optional[str]:
    if datetime_str is None:
        return

    try:
        datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S")
    except ValueError as exc:
        raise typer.BadParameter(
            "The datetime format should be {YYYY}-{MM}-{DD}T{HH}:{MM}:{SS}"
        ) from exc
    return datetime_str


def validate_cloud_storage_type(val: StorageType) -> str:
    if val is StorageType.FAI:
        secho_error_and_exit(
            "Checkpoint creation with FAI storage is not supported now."
        )
    return val.value


def validate_parallelism_order(value: str) -> List[str]:
    parallelism_order = value.split(",")
    if {"pp", "dp", "mp"} != set(parallelism_order):
        secho_error_and_exit(
            "Invalid Argument: parallelism_order should contain 'pp', 'dp', 'mp'"
        )
    return parallelism_order


def validate_cli_version() -> None:
    installed_version = version(PERIFLOW_CLI_NAME)
    if not is_latest_cli_version(installed_version):
        latest_version = get_latest_cli_version()
        secho_error_and_exit(
            f"CLI version({installed_version}) is deprecated. "
            f"Please install the latest version({latest_version}) with 'pip install {PERIFLOW_CLI_NAME}=={latest_version} -U --no-cache-dir'."
        )
