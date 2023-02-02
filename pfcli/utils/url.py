# Copyright (c) 2022-present, FriendliAI Inc. All rights reserved.

"""PeriFlow CLI URL Utilities"""

from urllib.parse import urljoin

periflow_api_server = "https://api-staging.friendli.ai/api/"
periflow_ws_server = "wss://api-ws-staging.friendli.ai/ws/"
periflow_discuss_url = "https://discuss.friendli.ai/"
periflow_mr_server = "https://pfmodelregistry-staging.friendli.ai/"
periflow_serve_server = "https://pfs-staging.friendli.ai/"
periflow_auth_server = "https://pfauth-staging.friendli.ai/"
periflow_meter_server = "https://pfmeter-staging.friendli.ai/"
periflow_observatory_server = "https://pfo-staging.friendli.ai/"


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
