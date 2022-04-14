# Copyright (C) 2021 FriendliAI

"""PeriFlow Auth Tools"""

import functools
import os
from enum import Enum
from pathlib import Path
from typing import Callable, Union

import requests

from pfcli.utils import get_uri, secho_error_and_exit

credential_path = Path(os.environ["HOME"], ".periflow")
access_token_path = credential_path / "access_token"
refresh_token_path = credential_path / "refresh_token"


class TokenType(str, Enum):
    ACCESS = "ACCESS"
    REFRESH = "REFRESH"


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


def update_token(token_type: TokenType, token: str) -> None:
    try:
        credential_path.mkdir(exist_ok=True)
    except (FileNotFoundError, FileExistsError) as e:
        secho_error_and_exit(f"Cannot store credential info... {e}")
    if token_type == TokenType.ACCESS:
        access_token_path.write_text(token)
    elif token_type == TokenType.REFRESH:
        refresh_token_path.write_text(token)


def auto_token_refresh(func: Callable[..., requests.Response]) -> Callable[..., requests.Response]:
    @functools.wraps(func)
    def inner(*args, **kwargs) -> requests.Response:
        r = func(*args, **kwargs)
        if r.status_code == 401 or r.status_code == 403:
            refresh_token = get_token(TokenType.REFRESH)
            if refresh_token is not None:
                refresh_r = requests.post(get_uri("token/refresh/"), data={"refresh": refresh_token})
                try:
                    refresh_r.raise_for_status()
                except requests.HTTPError:
                    secho_error_and_exit("Failed to refresh access token... Please login again")

                update_token(token_type=TokenType.ACCESS, token=refresh_r.json()["access"])
                # We need to restore file offset if we want to transfer file objects
                if "files" in kwargs:
                    files = kwargs["files"]
                    for file_name, file_tuple in files.items():
                        for element in file_tuple:
                            if hasattr(element, "seek"):
                                # Restore file offset
                                element.seek(0)
                r = func(*args, **kwargs)
                r.raise_for_status()
            else:
                secho_error_and_exit("Failed to refresh access token... Please login again")
        else:
            r.raise_for_status()
        return r
    return inner
