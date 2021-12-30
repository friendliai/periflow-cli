from typing import Union, Callable, Tuple, Dict
from pathlib import Path
import os
import functools

import typer
import autoauth


# Variables
periflow_api_server = "https://api-dev.friendli.ai/api/"


def get_uri(path):
    return periflow_api_server + path


def secho_error_and_exit(text: str, color=typer.colors.RED):
    typer.secho(text, err=True, fg=color)
    raise typer.Exit(1)


def get_group_id() -> int:
    r = autoauth.get(get_uri("user/group/"))
    if r.status_code != 200:
        secho_error_and_exit(
            f"Cannot acquire group info. Error Code = {r.status_code} detail = {r.text}")
    groups = r.json()["results"]
    if len(groups) == 0:
        secho_error_and_exit("You are not assigned to any group... Please contact to admin")
    if len(groups) > 1:
        secho_error_and_exit(
            "Currently we do not support users with more than two groups... Please contact admin")
    return groups[0]['id']
