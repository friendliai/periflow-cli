# Copyright (C) 2022 FriendliAI

"""PeriFlow Group (Organization) CLI"""

import typer

from pfcli.context import get_current_group_id, set_current_group_id
from pfcli.service import ServiceType
from pfcli.service.client import GroupClientService, UserGroupClientService, build_client
from pfcli.service.formatter import PanelFormatter, TableFormatter
from pfcli.utils import secho_error_and_exit


app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False
)


org_table_formatter = TableFormatter(
    name="Organization",
    fields=['id', 'name'],
    headers=["ID", "Name"]
)
org_panel_formatter = PanelFormatter(
    name="Organization Detail",
    fields=['id', 'name', 'status'],
    headers=['ID', 'Name', 'Statue']
)


@app.command(help="list all organizations")
def list():
    client: UserGroupClientService = build_client(ServiceType.USER_GROUP)
    orgs = client.get_group_info()
    org_table_formatter.render(orgs)


@app.command(help="create a new organization")
def create(
    name: str = typer.Argument(
        ...,
        help='Name of organization to create'
    )
):
    client: GroupClientService = build_client(ServiceType.GROUP)
    org = client.create_group(name=name)
    typer.secho(f"Organization ({name}) is created successfully!", fg=typer.colors.BLUE)
    org_panel_formatter.render(org)


@app.command(help="get current working organization")
def current():
    client: GroupClientService = build_client(ServiceType.GROUP)
    org_id = get_current_group_id()
    org = client.get_group(org_id)
    org_panel_formatter.render(org)


@app.command(help="switch working organization")
def switch(
    name: str = typer.Argument(
        ...,
        help='Name of organization to switch',
    )
):
    client: UserGroupClientService = build_client(ServiceType.USER_GROUP)
    orgs = client.get_group_info()

    org_id = None
    for org in orgs:
        if org['name'] == name:
            org_id = org['id']
            break

    if org_id is None:
        secho_error_and_exit(f"No organization exists with name {name}.")

    set_current_group_id(org_id)
    typer.secho(f"Organization switched to {name}.", fg=typer.colors.BLUE)
