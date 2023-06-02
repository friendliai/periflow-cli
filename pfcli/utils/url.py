# Copyright (c) 2022-present, FriendliAI Inc. All rights reserved.

"""PeriFlow CLI URL Utilities"""

from __future__ import annotations

from urllib.parse import urljoin

training_url = "https://training.periflow.ai/api/"
training_ws_url = "wss://training-ws.periflow.ai/ws/"
discuss_url = "https://discuss.friendli.ai/"
registry_url = "https://modelregistry.periflow.ai/"
serving_url = "https://serving.periflow.ai/"
auth_url = "https://auth.periflow.ai/"
meter_url = "https://metering.periflow.ai/"
observatory_url = "https://observatory.periflow.ai/"


def get_auth_uri(path: str) -> str:
    return urljoin(auth_url, path)


def get_uri(path: str) -> str:
    return urljoin(training_url, path)


def get_wss_uri(path: str) -> str:
    return urljoin(training_ws_url, path)


def get_pfs_uri(path: str) -> str:
    return urljoin(serving_url, path)


def get_mr_uri(path: str) -> str:
    return urljoin(registry_url, path)


def get_meter_uri(path: str) -> str:
    return urljoin(meter_url, path)


def get_observatory_uri(path: str) -> str:
    return urljoin(observatory_url, path)
