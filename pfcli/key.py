# Copyright (c) 2021-present, FriendliAI Inc. All rights reserved.

"""CLI for API Key"""

from __future__ import annotations

import typer
from dateutil.parser import parse

from pfcli.service import ServiceType
from pfcli.service.client import build_client
from pfcli.service.client.user import UserAccessKeyClientService
from pfcli.service.formatter import PanelFormatter, TableFormatter
from pfcli.utils.format import datetime_to_simple_string

app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False,
)

access_key_table = TableFormatter(
    name="Access Keys",
    fields=[
        "id",
        "name",
        "created_at",
    ],
    headers=[
        "ID",
        "Name",
        "Created At",
    ],
)

access_key_panel = PanelFormatter(
    name="Access Key",
    fields=[
        "id",
        "name",
        "created_at",
        "token",
    ],
    headers=[
        "ID",
        "Name",
        "Created At",
        "Access Key",
    ],
)


@app.command()
def list():
    """List all api keys."""
    client: UserAccessKeyClientService = build_client(ServiceType.USER_ACCESS_KEY)
    access_keys = client.list_access_keys()
    for key in access_keys:
        key["created_at"] = datetime_to_simple_string(parse(key["created_at"]))
        if key["expiry_time"]:
            key["expiry_time"] = datetime_to_simple_string(parse(key["expiry_time"]))

    access_key_table.render(access_keys)


@app.command()
def create(name: str = typer.Option(..., "--name", "-n", help="Name of api key.")):
    """Create api key."""
    client: UserAccessKeyClientService = build_client(ServiceType.USER_ACCESS_KEY)
    access_key = client.create_access_key(name=name)
    access_key["created_at"] = datetime_to_simple_string(
        parse(access_key["created_at"])
    )
    if access_key["expiry_time"]:
        access_key["expiry_time"] = datetime_to_simple_string(
            parse(access_key["expiry_time"])
        )

    access_key_panel.render(access_key)

    typer.secho(
        "If you misplace or cannot recall your confidential access key, there is no way to recover it.\n"
        "Your only option is to generate a fresh access key and delete the old one.",
        fg=typer.colors.BLUE,
    )


@app.command()
def delete(access_key_id: str = typer.Argument(..., help="ID of key to delete")):
    """Delete api key."""
    client: UserAccessKeyClientService = build_client(ServiceType.USER_ACCESS_KEY)
    client.delete_access_key(access_key_id)
    typer.secho(f"Success to delete access key {access_key_id}", fg=typer.colors.BLUE)
