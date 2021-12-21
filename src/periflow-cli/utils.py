import os
import functools

from pathlib import Path
from typing import Callable, List, Union

import typer
import requests

import autoauth


# Variables
periflow_api_server = "https://api-dev.friendli.ai/api/"
credential_path = Path(os.environ["HOME"], ".periflow")
access_token_path = credential_path / "access_token"
refresh_token_path = credential_path / "refresh_token"


def get_uri(path):
    return periflow_api_server + path


def get_token(token_type: str) -> Union[str, None]:
    try:
        if token_type == "access":
            return access_token_path.read_text()
        if token_type == "refresh":
            return refresh_token_path.read_text()
        else:
            raise ValueError("token_type should be one of 'access' or 'refresh'.")
    except FileNotFoundError:
        return None


def update_token(token_type: str, token: str) -> None:
    try:
        credential_path.mkdir(exist_ok=True)
    except (FileNotFoundError, FileExistsError) as e:
        typer.echo(f"Cannot store credential info... {e}", err=True)
        typer.Exit(1)
    if token_type == "access":
        access_token_path.write_text(token)
    elif token_type == "refresh":
        refresh_token_path.write_text(token)


def auto_token_refresh(func: Callable[..., requests.Response]) -> Callable:
    @functools.wraps(func)
    def inner(*args, **kwargs) -> requests.Response:
        r = func(*args, **kwargs)
        if r.status_code == 401 or r.status_code == 403:
            typer.echo("Refresh access token...")
            refresh_token = get_token("refresh")
            if refresh_token is not None:
                refresh_r = requests.post(get_uri("token/refresh/"), data={"refresh": refresh_token})
                if refresh_r.status_code == 200:
                    update_token(token_type="access", token=refresh_r.json()["access"])
                    r = func(*args, **kwargs)
                else:
                    typer.secho("Failed to refresh access token... Please login again", fg=typer.colors.RED)
            else:
                typer.secho("Failed to refresh access token... Please login again", fg=typer.colors.RED)
        return r
    return inner


def get_auth_header() -> dict:
    return {"Authorization": f"Bearer {get_token('access')}"}


def get_group_id() -> int:
    r = autoauth.get(get_uri("user/group/"))
    if r.status_code != 200:
        typer.secho(f"Cannot acquire group info. Error Code = {r.status_code} detail = {r.text}")
        typer.Exit(1)
    groups = r.json()["results"]
    if len(groups) == 0:
        typer.secho("You are not assigned to any group... Please contact to admin",
                    err=True,
                    fg=typer.colors.RED)
        typer.Exit(1)
    if len(groups) > 1:
        typer.secho("Currently we do not support users with more than two groups... Please contact admin",
                    err=True,
                    fg=typer.colors.RED)
        typer.Exit(1)
    return groups[0]['id']
