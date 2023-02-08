# Copyright (c) 2022-present, FriendliAI Inc. All rights reserved.

"""PeriFlow CLI URL Utilities"""

from __future__ import annotations

from urllib.parse import urljoin

periflow_api_server = "https://api-dev.friendli.ai/api/"
periflow_ws_server = "wss://api-ws-dev.friendli.ai/ws/"
periflow_discuss_url = "https://discuss.friendli.ai/"
periflow_mr_server = "https://pfmodelregistry-dev.friendli.ai/"
periflow_serve_server = "https://pfs-dev.friendli.ai/"
periflow_auth_server = "https://pfauth-dev.friendli.ai/"
periflow_meter_server = "https://pfmeter-dev.friendli.ai/"
periflow_observatory_server = "https://pfo-dev.friendli.ai/"


def get_auth_uri(path: str) -> str:
    return urljoin(periflow_auth_server, path)


def get_uri(path: str) -> str:
    return urljoin(periflow_api_server, path)


def get_wss_uri(path: str) -> str:
    return urljoin(periflow_ws_server, path)


def get_pfs_uri(path: str) -> str:
    return urljoin(periflow_serve_server, path)


def get_mr_uri(path: str) -> str:
    return urljoin(periflow_mr_server, path)


def get_meter_uri(path: str) -> str:
    return urljoin(periflow_meter_server, path)


def get_observatory_uri(path: str) -> str:
    return urljoin(periflow_observatory_server, path)
