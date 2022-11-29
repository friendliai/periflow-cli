# Copyright (C) 2022 FriendliAI

"""PeriFlow Group (Organization) CLI"""

from uuid import UUID

import typer

from pfcli.context import get_current_group_id
from pfcli.service import GroupRole, ServiceType
from pfcli.service.client import (
    GroupClientService,
    UserClientService,
    UserGroupClientService,
    build_client,
)
from pfcli.service.formatter import PanelFormatter, TableFormatter
from pfcli.utils.format import secho_error_and_exit


app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False,
)

org_table_formatter = TableFormatter(
    name="Organization", fields=["name", "id"], headers=["Name", "ID"]
)
org_panel_formatter = PanelFormatter(
    name="Organization Detail",
    fields=["id", "name", "status"],
    headers=["ID", "Name", "Status"],
)
member_table_formatter = TableFormatter(
    name="Members",
    fields=["id", "username", "name", "email", "privilege_level"],
    headers=["ID", "Username", "Name", "Email", "Role"],
)


@app.command(help="invite to current working organization")
def invite(email: str = typer.Argument(..., help="Invitation recipient email address")):
    group_client: GroupClientService = build_client(ServiceType.GROUP)

    org = get_current_org()

    if org["privilege_level"] != "owner":
        secho_error_and_exit("Only the owner of the organization can invite/set-role.")

    group_client.invite_to_group(org["id"], email)
    typer.echo("Invitation Successfully Sent!")


@app.command("accept-invite", help="accept invitation")
def accept_invite(
    token: str = typer.Option(..., prompt="Enter email token"),
    key: str = typer.Option(..., prompt="Enter verification key"),
):
    group_client: GroupClientService = build_client(ServiceType.GROUP)
    group_client.accept_invite(token, key)
    typer.echo("Verification Success!")
    typer.echo("Please login again with: ", nl=False)
    typer.secho("pf login", fg=typer.colors.BLUE)


@app.command("set-role", help="set organization role of the user")
def set_role(
    username: str = typer.Argument(..., help="Username to set role"),
    role: GroupRole = typer.Argument(..., help="Organization role"),
):
    user_client: UserClientService = build_client(ServiceType.USER)

    org = get_current_org()

    if org["privilege_level"] != "owner":
        secho_error_and_exit(
            "Only the owner of the organization can invite/set-privilege."
        )

    user_id = _get_org_user_id_by_name(org["id"], username)
    user_client.set_group_privilege(org["id"], user_id, role)
    typer.echo(
        f"Organization role for user ({username}) successfully updated to {role.value}!"
    )


def _get_org_user_id_by_name(org_id: UUID, username: str) -> UUID:
    group_client: GroupClientService = build_client(ServiceType.GROUP)
    users = group_client.get_users(org_id, username)
    for user in users:
        if user["username"] == username:
            return UUID(user["id"])
    secho_error_and_exit(f"{username} is not a member of this organization.")


def get_current_org() -> dict:
    user_group_client: UserGroupClientService = build_client(ServiceType.USER_GROUP)

    curr_org_id = get_current_group_id()
    if curr_org_id is None:
        secho_error_and_exit("Organization is not identified. Please login again.")

    org = user_group_client.get_group_info()
    if org["id"] == str(curr_org_id):
        return org

    # org context may be wrong
    secho_error_and_exit("Failed to identify organization.")


@app.command(help="list up members in the current working organization")
def members():
    group_client: GroupClientService = build_client(ServiceType.GROUP)
    org_id = get_current_group_id()

    if org_id is None:
        secho_error_and_exit("Organization is not identified. Please login again.")

    members = group_client.list_users(org_id)
    member_table_formatter.render(members)
