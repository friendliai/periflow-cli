import functools
import os
from contextlib import contextmanager, asynccontextmanager
from enum import Enum
from pathlib import Path
from typing import Callable, Union

import requests
import typer
import websockets

from pfcli.utils import get_uri, secho_error_and_exit

credential_path = Path(os.environ["HOME"], ".periflow")
access_token_path = credential_path / "access_token"
refresh_token_path = credential_path / "refresh_token"


class TokenType(Enum):
    ACCESS = 1
    REFRESH = 2


def get_auth_header() -> dict:
    return {"Authorization": f"Bearer {get_token(TokenType.ACCESS)}"}


def get_token(token_type: TokenType) -> Union[str, None]:
    try:
        if token_type == TokenType.ACCESS:
            return access_token_path.read_text()
        if token_type == TokenType.REFRESH:
            return refresh_token_path.read_text()
        else:
            secho_error_and_exit("token_type should be one of 'access' or 'refresh'.")
    except FileNotFoundError:
        return None


def update_token(token_type: str, token: str) -> None:
    try:
        credential_path.mkdir(exist_ok=True)
    except (FileNotFoundError, FileExistsError) as e:
        secho_error_and_exit(f"Cannot store credential info... {e}")
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
            refresh_token = get_token(TokenType.REFRESH)
            if refresh_token is not None:
                refresh_r = requests.post(get_uri("token/refresh/"), data={"refresh": refresh_token})
                try:
                    refresh_r.raise_for_status()
                    update_token(token_type="access", token=refresh_r.json()["access"])
                    # We need to restore file offset if we want to transfer file objects
                    if "files" in kwargs:
                        files = kwargs["files"]
                        for file_name, file_tuple in files.items():
                            for element in file_tuple:
                                if hasattr(element, "seek"):
                                    # Restore file offset
                                    element.seek(0)
                    r = func(*args, **kwargs)
                except requests.HTTPError:
                    secho_error_and_exit("Failed to refresh access token... Please login again")
            else:
                secho_error_and_exit("Failed to refresh access token... Please login again")
        return r
    return inner


@auto_token_refresh
def get(*args, **kwargs) -> requests.Response:
    if "headers" in kwargs:
        kwargs["headers"].update(get_auth_header())
    else:
        kwargs["headers"] = get_auth_header()
    r = requests.get(*args, **kwargs)
    return r


@auto_token_refresh
def post(*args, **kwargs) -> requests.Response:
    if "headers" in kwargs:
        kwargs["headers"].update(get_auth_header())
    else:
        kwargs["headers"] = get_auth_header()
    r = requests.post(*args, **kwargs)
    return r


@auto_token_refresh
def patch(*args, **kwargs) -> requests.Response:
    if "headers" in kwargs:
        kwargs["headers"].update(get_auth_header())
    else:
        kwargs["headers"] = get_auth_header()
    r = requests.patch(*args, **kwargs)
    return r


@auto_token_refresh
def delete(*args, **kwargs) -> requests.Response:
    if "headers" in kwargs:
        kwargs["headers"].update(get_auth_header())
    else:
        kwargs["headers"] = get_auth_header()
    r = requests.delete(*args, **kwargs)
    return r


# TODO: [PFT-198] Auto authentication refresh
@asynccontextmanager
async def connect_with_auth(uri: str):
    access_token = get_token(TokenType.ACCESS)
    uri = uri.rstrip('/') + '/'
    uri_with_auth = f'{uri}?token={access_token}'
    async with websockets.connect(uri_with_auth) as conn:
        yield conn
