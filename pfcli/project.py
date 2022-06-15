# Copyright (C) 2022 FriendliAI

"""PeriFlow Project CLI"""

from typing import Optional, Union

import typer

from pfcli.context import get_current_project_id, set_current_project_id
from pfcli.service import ServiceType
from pfcli.service.client import (
    GroupProjectClientService,
    ProjectClientService,
    UserGroupProjectClientService,
    build_client
)
from pfcli.service.formatter import PanelFormatter, TableFormatter
from pfcli.utils import secho_error_and_exit


app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False
)
project_table_formatter = TableFormatter(
    name="Project",
    fields=['id', 'name'],
    headers=["ID", "Name"]
)
project_panel_formatter = PanelFormatter(
    name="Project Detail",
    fields=['pf_group_id', 'id', 'name'],
    headers=['Organization ID', 'Project ID', 'Name']
)


@app.command()
def list(
    tail: Optional[int] = typer.Option(
        None,
        "--tail",
        help="The number of project list to view at the tail"
    ),
    head: Optional[int] = typer.Option(
        None,
        "--tail",
        help="The number of project list to view at the head"
    ),
    show_group_project: bool = typer.Option(
        False,
        "--group",
        "-g",
        help="Show all projects in the current group"
    )
):
    """List projects
    """
    client: Union[GroupProjectClientService, UserGroupProjectClientService]
    if show_group_project:
        client = build_client(ServiceType.GROUP_PROJECT)
    else:
        client = build_client(ServiceType.USER_GROUP_PROJECT)

    projects = client.list_projects()

    if tail is not None or head is not None:
        target_project_list = []
        if tail is not None:
            target_project_list.extend(projects[:tail])
        if head is not None:
            target_project_list.extend(projects[-head:])
    else:
        target_project_list = projects

    project_table_formatter.render(target_project_list)


@app.command()
def create(
    name: str = typer.Argument(
        ...,
        help='Name of organization to create'
    )
):
    client: GroupProjectClientService = build_client(ServiceType.GROUP_PROJECT)
    project_detail = client.create_project(name)
    project_panel_formatter.render(project_detail)


@app.command(help="get current working project")
def current():
    client: ProjectClientService = build_client(ServiceType.PROJECT)
    project_id = get_current_project_id()
    project = client.get_project(project_id)
    project_panel_formatter.render(project)


@app.command(help="switch working project")
def switch(
    name: str = typer.Argument(
        ...,
        help='Name of organization to switch',
    )
):
    client: GroupProjectClientService = build_client(ServiceType.GROUP_PROJECT)
    projects = client.list_projects()

    project_id = None
    for project in projects:
        if project['name'] == name:
            project_id = project['id']
            break

    if project_id is None:
        secho_error_and_exit(f"No project exists with name {name}.")

    set_current_project_id(project_id)
    typer.secho(f"Project switched to {name}.", fg=typer.colors.BLUE)
