# Copyright (C) 2021 FriendliAI

"""PeriFlow Auth Tools"""

import functools
from enum import Enum
from typing import Callable, Union

import requests

from pfcli.utils.format import secho_error_and_exit
from pfcli.utils.fs import get_periflow_directory
from pfcli.utils.url import get_uri


access_token_path = get_periflow_directory() / "access_token"
refresh_token_path = get_periflow_directory() / "refresh_token"
mfa_token_path = get_periflow_directory() / "mfa_token"


class TokenType(str, Enum):
    ACCESS = "ACCESS"
    REFRESH = "REFRESH"
    MFA = "MFA"


token_path_map = {
    TokenType.ACCESS: access_token_path,
    TokenType.REFRESH: refresh_token_path,
    TokenType.MFA: mfa_token_path,
}


def get_auth_header() -> dict:
    return {"Authorization": f"Bearer {get_token(TokenType.ACCESS)}"}


def get_token(token_type: TokenType) -> Union[str, None]:
    try:
        if token_type == TokenType.ACCESS:
            return access_token_path.read_text()
        if token_type == TokenType.REFRESH:
            return refresh_token_path.read_text()
        if token_type == TokenType.MFA:
            return mfa_token_path.read_text()
        else:
            secho_error_and_exit(
                "token_type should be one of 'access' or 'refresh' or 'mfa'."
            )
    except FileNotFoundError:
        return None


def update_token(token_type: TokenType, token: str) -> None:
    token_path_map[token_type].write_text(token)


def delete_token(token_type: TokenType) -> None:
    token_path_map[token_type].unlink(missing_ok=True)


def clear_tokens() -> None:
    for e in TokenType:
        delete_token(e)


def auto_token_refresh(
    func: Callable[..., requests.Response]
) -> Callable[..., requests.Response]:
    @functools.wraps(func)
    def inner(*args, **kwargs) -> requests.Response:
        r = func(*args, **kwargs)
        if r.status_code == 401 or r.status_code == 403:
            refresh_token = get_token(TokenType.REFRESH)
            if refresh_token is not None:
                refresh_r = requests.post(
                    get_uri("token/refresh/"), data={"refresh_token": refresh_token}
                )
                try:
                    refresh_r.raise_for_status()
                except requests.HTTPError:
                    secho_error_and_exit(
                        "Failed to refresh access token... Please login again"
                    )

                update_token(
                    token_type=TokenType.ACCESS, token=refresh_r.json()["access_token"]
                )
                update_token(
                    token_type=TokenType.REFRESH,
                    token=refresh_r.json()["refresh_token"],
                )
                # We need to restore file offset if we want to transfer file objects
                if "files" in kwargs:
                    files = kwargs["files"]
                    for _, file_tuple in files.items():
                        for element in file_tuple:
                            if hasattr(element, "seek"):
                                # Restore file offset
                                element.seek(0)
                r = func(*args, **kwargs)
                r.raise_for_status()
            else:
                secho_error_and_exit(
                    "Failed to refresh access token... Please login again"
                )
        else:
            r.raise_for_status()
        return r

    return inner
