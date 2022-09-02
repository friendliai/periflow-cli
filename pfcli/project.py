# Copyright (C) 2022 FriendliAI

"""PeriFlow Project CLI"""

import uuid
from typing import List, Optional, Tuple, Union

import typer

from pfcli.context import (
    get_current_project_id,
    set_current_project_id,
    project_context_path,
)
from pfcli.group import _get_current_org, _get_org_user_id_by_name
from pfcli.service import GroupRole, ProjectRole, ServiceType
from pfcli.service.client import (
    GroupProjectClientService,
    ProjectClientService,
    UserClientService,
    UserGroupProjectClientService,
    build_client,
)
from pfcli.service.formatter import PanelFormatter, TableFormatter
from pfcli.utils import secho_error_and_exit


app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False,
)
project_table_formatter = TableFormatter(
    name="Project", fields=["id", "name"], headers=["ID", "Name"]
)
project_panel_formatter = PanelFormatter(
    name="Project Detail",
    fields=["pf_group_id", "id", "name"],
    headers=["Organization ID", "Project ID", "Name"],
)
member_table_formatter = TableFormatter(
    name="Members",
    fields=["id", "username", "name", "email", "access_level"],
    headers=["ID", "Username", "Name", "Email", "Role"],
)


def _find_project_id(projects: List[dict], project_name: str) -> uuid.UUID:
    for project in projects:
        if project["name"] == project_name:
            return uuid.UUID(project["id"])
    secho_error_and_exit(f"No project exists with name {project_name}.")


@app.command(help="list all accessible projects")
def list(
    tail: Optional[int] = typer.Option(
        None, "--tail", help="The number of project list to view at the tail"
    ),
    head: Optional[int] = typer.Option(
        None, "--tail", help="The number of project list to view at the head"
    ),
    show_group_project: bool = typer.Option(
        False, "--group", "-g", help="Show all projects in the current group"
    ),
):
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


@app.command(help="create a new project")
def create(name: str = typer.Argument(..., help="Name of project to create")):
    client: GroupProjectClientService = build_client(ServiceType.GROUP_PROJECT)
    project_detail = client.create_project(name)
    project_panel_formatter.render(project_detail)


@app.command(help="get current working project")
def current():
    client: ProjectClientService = build_client(ServiceType.PROJECT)
    project_id = get_current_project_id()
    if project_id is None:
        secho_error_and_exit("working project is not set")
    project = client.get_project(project_id)
    project_panel_formatter.render(project)


@app.command(help="switch working project")
def switch(
    name: str = typer.Argument(
        ...,
        help="Name of project to switch",
    )
):
    user_group_project_client: UserGroupProjectClientService = build_client(
        ServiceType.USER_GROUP_PROJECT
    )
    project_client: ProjectClientService = build_client(ServiceType.PROJECT)

    project_id = _find_project_id(user_group_project_client.list_projects(), name)
    if project_client.check_project_membership(pf_project_id=project_id):
        set_current_project_id(project_id)
        typer.secho(f"Project switched to {name}.", fg=typer.colors.BLUE)
    else:
        secho_error_and_exit(
            f"You don't have permission to project ({name}). Please contact to the project admin."
        )


@app.command(help="delete project")
def delete(
    name: str = typer.Argument(
        ...,
        help="Name of project to delete",
    )
):
    project_client: ProjectClientService = build_client(ServiceType.PROJECT)
    user_group_project_client: UserGroupProjectClientService = build_client(
        ServiceType.USER_GROUP_PROJECT
    )
    project_id = _find_project_id(user_group_project_client.list_projects(), name)
    project_client.delete_project(pf_project_id=project_id)
    if project_id == get_current_project_id():
        project_context_path.unlink()
    typer.secho(f"Project {name} deleted.", fg=typer.colors.BLUE)


@app.command("add-user", help="add user to project")
def add_user(
    username: str = typer.Argument(
        ...,
        help="Username to add to the current working project",
    ),
    role: ProjectRole = typer.Argument(
        ...,
        help="Project role to assign",
    ),
):
    user_client: UserClientService = build_client(ServiceType.USER)

    org_id, project_id = _check_project_and_get_id()
    user_id = _get_org_user_id_by_name(org_id, username)

    user_client.add_to_project(user_id, project_id, role)
    typer.secho(f"User successfully added to project")


@app.command("set-role", help="set project role for a user")
def set_role(
    username: str = typer.Argument(
        ...,
        help="Username to set project role",
    ),
    role: ProjectRole = typer.Argument(
        ...,
        help="Project role",
    ),
):
    user_client: UserClientService = build_client(ServiceType.USER)

    org_id, project_id = _check_project_and_get_id()
    user_id = _get_org_user_id_by_name(org_id, username)

    user_client.set_project_privilege(user_id, project_id, role)
    typer.secho(
        f"Project role for user ({username}) successfully updated to {role.value}!"
    )


@app.command(help="list up members in the current working project")
def members():
    project_client: ProjectClientService = build_client(ServiceType.PROJECT)

    project_id = get_current_project_id()
    members = project_client.list_users(project_id)
    member_table_formatter.render(members)


def _check_project_and_get_id() -> Tuple[str, str]:
    """Get org_id and project_id if valid"""

    user_client: UserClientService = build_client(ServiceType.USER)

    org = _get_current_org()
    project_id = get_current_project_id()

    if org["privilege_level"] == GroupRole.OWNER:
        return org["id"], project_id

    requester = user_client.get_project_membership(project_id)
    if requester["access_level"] != ProjectRole.ADMIN:
        secho_error_and_exit("Only the admin of the project can add-user/set-role")

    return org["id"], project_id
