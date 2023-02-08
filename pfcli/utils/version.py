from __future__ import annotations

import json
from importlib.metadata import version
from urllib.request import urlopen

from packaging.version import _BaseVersion
from packaging.version import parse as parse_version

PERIFLOW_CLI_NAME = "periflow-cli"
PYPI_BASE_URL = "https://pypi.org/pypi"


def is_latest_cli_version(ver: str) -> bool:
    version = parse_version(ver)
    latest_version = get_latest_cli_version()

    return version == latest_version


def get_latest_cli_version() -> _BaseVersion:
    pypi_info = json.loads(urlopen(f"{PYPI_BASE_URL}/{PERIFLOW_CLI_NAME}/json").read())
    latest_ver_string = pypi_info["info"]["version"]
    return parse_version(latest_ver_string)


def get_installed_cli_version() -> str:
    return version(PERIFLOW_CLI_NAME)
