# Copyright (C) 2021 FriendliAI

"""PeriFlow CLI"""

import requests
from requests import HTTPError, Response

import typer

from pfcli import (
    artifact,
    billing,
    checkpoint,
    credential,
    dataset,
    group,
    job,
    project,
    deployment,
    vm,
)
from pfcli.context import (
    get_current_project_id,
    project_context_path,
    set_current_group_id,
)
from pfcli.service import ServiceType
from pfcli.service.auth import clear_tokens, get_token, TokenType, update_token
from pfcli.service.client import (
    build_client,
    ProjectClientService,
    UserClientService,
    UserGroupClientService,
    UserMFAService,
    UserSignUpService,
)
from pfcli.service.formatter import PanelFormatter
from pfcli.utils.format import secho_error_and_exit
from pfcli.utils.url import get_uri

app = typer.Typer(
    help="Welcome to PeriFlow 🤗",
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False,
)

app.add_typer(credential.app, name="credential", help="Manage credentials")
app.add_typer(checkpoint.app, name="checkpoint", help="Manage checkpoints")
app.add_typer(vm.app, name="vm", help="Manage VMs")
app.add_typer(deployment.app, name="deployment", help="Manage deployments")
app.add_typer(project.app, name="project", help="Manage projects")
app.add_typer(group.app, name="org", help="Manage organizations")
app.add_typer(billing.app, name="billing", help="Manage billing")
app.add_typer(artifact.app, name="artifact", help="Manager artifacts")
app.add_typer(job.app, name="job", help="Manage jobs")
app.add_typer(dataset.app, name="dataset", help="Manage datasets")


user_panel_formatter = PanelFormatter(
    name="My Info",
    fields=["name", "username", "email"],
    headers=["Name", "Username", "Email"],
)


@app.command(help="Sign up to PerfiFlow")
def signup(
    username: str = typer.Option(..., prompt="Enter Username"),
    name: str = typer.Option(..., prompt="Enter Name"),
    email: str = typer.Option(..., prompt="Enter Email"),
    password: str = typer.Option(..., prompt="Enter Password", hide_input=True),
    confirm_password: str = typer.Option(
        ..., prompt="Enter the password again (confirmation)", hide_input=True
    ),
):
    if password != confirm_password:
        secho_error_and_exit("Passwords did not match.")

    client: UserSignUpService = build_client(ServiceType.SIGNUP)
    client.sign_up(username, name, email, password)

    typer.echo(f"\n\nWe just sent a verification code over to {email}")
    typer.run(_verify)


@app.command(help="Show my user info")
def whoami():
    client: UserClientService = build_client(ServiceType.USER)
    info = client.get_current_userinfo()
    user_panel_formatter.render([info])


@app.command(help="Sign in PeriFlow")
def login(
    username: str = typer.Option(..., prompt="Enter Username"),
    password: str = typer.Option(..., prompt="Enter Password", hide_input=True),
):
    r = requests.post(
        get_uri("token/"), data={"username": username, "password": password}
    )
    resp = r.json()
    if "code" in resp and resp["code"] == "mfa_required":
        mfa_token = resp["mfa_token"]
        client: UserMFAService = build_client(ServiceType.MFA)
        # TODO: MFA type currently defaults to totp, need changes when new options are added
        client.initiate_mfa(mfa_type="totp", mfa_token=mfa_token)
        update_token(token_type=TokenType.MFA, token=mfa_token)
        typer.run(_mfa_verify)
    else:
        _handle_login_response(r, False)

    # Save user's organiztion context
    project_client: ProjectClientService = build_client(ServiceType.PROJECT)
    user_group_client: UserGroupClientService = build_client(ServiceType.USER_GROUP)

    try:
        org = user_group_client.get_group_info()
    except IndexError:
        secho_error_and_exit("You are not included in any organization.")
    org_id = org["id"]

    project_id = get_current_project_id()
    if project_id is not None:
        if project_client.check_project_membership(pf_project_id=project_id):
            project_org_id = project_client.get_project(pf_project_id=project_id)[
                "pf_group_id"
            ]
            if project_org_id != org_id:
                project_context_path.unlink(missing_ok=True)
        else:
            project_context_path.unlink(missing_ok=True)
    set_current_group_id(org_id)


@app.command(help="Sign out PeriFlow")
def logout():
    clear_tokens()
    typer.secho("Successfully signed out.", fg=typer.colors.BLUE)


@app.command(help="Change your password")
def passwd(
    old_password: str = typer.Option(
        ..., prompt="Enter your current password", hide_input=True
    ),
    new_password: str = typer.Option(
        ..., prompt="Enter your new password", hide_input=True
    ),
    confirm_password: str = typer.Option(
        ..., prompt="Enter the new password again (confirmation)", hide_input=True
    ),
):
    if old_password == new_password:
        secho_error_and_exit("The current password is the same with the new password.")
    if new_password != confirm_password:
        secho_error_and_exit("Passwords did not match.")
    client: UserClientService = build_client(ServiceType.USER)
    client.change_password(old_password, new_password)

    typer.secho("Password is changed successfully!", fg=typer.colors.BLUE)


def _verify(
    _,
    token: str = typer.Option(..., prompt="Enter email token"),
    key: str = typer.Option(..., prompt="Enter verification key"),
):
    client: UserSignUpService = build_client(ServiceType.SIGNUP)
    client.verify(token, key)

    typer.echo("\n\nVerified!")
    typer.echo("Sign up success! Please sign in.")


def _mfa_verify(_, code: str = typer.Option(..., prompt="Enter MFA Code")):
    mfa_token = get_token(TokenType.MFA)
    # TODO: MFA type currently defaults to totp, need changes when new options are added
    mfa_type = "totp"
    username = f"mfa://{mfa_type}/{mfa_token}"
    r = requests.post(get_uri("token/"), data={"username": username, "password": code})
    _handle_login_response(r, True)


def _handle_login_response(r: Response, mfa: bool):
    try:
        r.raise_for_status()
        update_token(token_type=TokenType.ACCESS, token=r.json()["access_token"])
        update_token(token_type=TokenType.REFRESH, token=r.json()["refresh_token"])

        typer.echo("\n\nLogin success!")
        typer.echo("Welcome back to...")
        typer.echo(" _____          _  _____ _")
        typer.echo("|  __ \___ _ __(_)|  ___| | _____      __")  # type: ignore
        typer.echo("|  ___/ _ \ '__| || |__ | |/ _ \ \ /\ / /")  # type: ignore
        typer.echo("| |  |  __/ |  | ||  __|| | (_) | V  V / ")
        typer.echo("|_|   \___|_|  |_||_|   |_|\___/ \_/\_/  ")  # type: ignore
        typer.echo("\n\n")
    except HTTPError:
        if mfa:
            secho_error_and_exit("Login failed... Invalid MFA Code.")
        else:
            secho_error_and_exit(
                "Login failed... Please check your username and password."
            )
