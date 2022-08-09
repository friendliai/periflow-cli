# Copyright (C) 2022 FriendliAI

"""PeriFlow Group (Organization) CLI"""

import typer

from pfcli.context import (
    get_current_group_id,
    get_current_project_id,
    project_context_path,
    set_current_group_id
)
from pfcli.service import GroupRole, ServiceType
from pfcli.service.client import (
    GroupClientService,
    ProjectClientService,
    UserClientService,
    UserGroupClientService,
    build_client
)
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
    headers=['ID', 'Name', 'Status']
)
member_table_formatter = TableFormatter(
    name="Members",
    fields=["id", "username", "name", "email", "privilege_level"],
    headers=["ID", "Username", "Name", "Email", "Role"]
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
    group_client: GroupClientService = build_client(ServiceType.GROUP)
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
        if project_client.check_project_membership(pf_project_id=project_id):
            project_org_id = project_client.get_project(pf_project_id=project_id)["pf_group_id"]
            if project_org_id != org_id:
                project_context_path.unlink(missing_ok=True)
        else:
            project_context_path.unlink(missing_ok=True)

    set_current_group_id(org_id)
    typer.secho(f"Organization switched to {name}.", fg=typer.colors.BLUE)


@app.command(help="invite to current working organization")
def invite(
    email: str = typer.Argument(
        ...,
        help='Invitation recipient email address'
    )
):
    group_client: GroupClientService = build_client(ServiceType.GROUP)

    org = _get_current_org()

    if org['privilege_level'] != 'owner':
        secho_error_and_exit("Only the owner of the organization can invite/set-role.")

    group_client.invite_to_group(org['id'], email)
    typer.secho("Invitation Successfully Sent!")


@app.command("accept-invite", help="accept invitation")
def accept_invite(token: str = typer.Option(..., prompt="Enter Verification Code")):
    group_client: GroupClientService = build_client(ServiceType.GROUP)
    group_client.accept_invite(token)
    typer.secho("Verification Success!")


@app.command("set-role", help="set organization role of the user")
def set_role(
    username: str = typer.Argument(
        ...,
        help='Username to set role'
    ),
    role: GroupRole = typer.Argument(
        ...,
        help='Organization role'
    )
):
    user_client: UserClientService = build_client(ServiceType.USER)

    org = _get_current_org()

    if org['privilege_level'] != 'owner':
        secho_error_and_exit("Only the owner of the organization can invite/set-privilege.")

    user_id = _get_org_user_id_by_name(org['id'], username)
    user_client.set_group_privilege(org['id'], user_id, role)
    typer.secho(f"Organization role for user ({username}) successfully updated to {role.value}!")


def _get_org_user_id_by_name(org_id: str, username: str) -> str:
    group_client: GroupClientService = build_client(ServiceType.GROUP)
    users = group_client.get_user(org_id, username)
    for user in users:
        if user['username'] == username:
            return user['id']
    secho_error_and_exit(f"{username} is not a member of this organization.")


def _get_current_org() -> dict:
    user_group_client: UserGroupClientService = build_client(ServiceType.USER_GROUP)

    curr_org_id = get_current_group_id()
    if curr_org_id is None:
        secho_error_and_exit("working organization is not set")

    orgs = user_group_client.get_group_info()
    for org in orgs:
        if org['id'] == str(curr_org_id):
            return org

    # org context may be wrong
    secho_error_and_exit("Failed to identify organization... Please set organization again.")


@app.command(help="list up members in the current working organization")
def members():
    group_client: GroupClientService = build_client(ServiceType.GROUP)
    org_id = get_current_group_id()

    if org_id is None:
        secho_error_and_exit("working organization is not set")

    members = group_client.list_users(org_id)
    member_table_formatter.render(members)
