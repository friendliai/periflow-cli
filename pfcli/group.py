# Copyright (C) 2022 FriendliAI

"""PeriFlow Group (Organization) CLI"""

import typer

from pfcli.context import (
    get_current_group_id,
    get_current_project_id,
    project_context_path,
    set_current_group_id
)
from pfcli.service import ServiceType
from pfcli.service.client import GroupClientService, UserGroupClientService, build_client
from pfcli.service.client.project import ProjectClientService
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
    project_client: ProjectClientService = build_client(ServiceType.PROJECT)
    group_client: GroupClientService = build_client(ServiceType.GROUP)
    project_id = get_current_project_id()

    if project_id is not None:
        org_id = project_client.get_project(pf_project_id=project_id)["pf_group_id"]
    else:
        org_id = get_current_group_id()

    if org_id is None:
        secho_error_and_exit("working organization is not set")

    org = group_client.get_group(org_id)
    org_panel_formatter.render(org)


@app.command(help="switch working organization")
def switch(
    name: str = typer.Argument(
        ...,
        help='Name of organization to switch',
    )
):
    project_client: ProjectClientService = build_client(ServiceType.PROJECT)
    user_group_client: UserGroupClientService = build_client(ServiceType.USER_GROUP)
    orgs = user_group_client.get_group_info()

    org_id = None
    for org in orgs:
        if org["name"] == name:
            org_id = org["id"]
            break

    if org_id is None:
        secho_error_and_exit(f"No organization exists with name {name}.")

    project_id = get_current_project_id()
    if project_id is not None:
        project_org_id = project_client.get_project(pf_project_id=project_id)["pf_group_id"]
        if project_org_id != org_id:
            project_context_path.unlink(missing_ok=True)

    set_current_group_id(org_id)
    typer.secho(f"Organization switched to {name}.", fg=typer.colors.BLUE)
