# Copyright (C) 2021 FriendliAI

"""PeriFlow CLI"""

import requests
from requests import HTTPError

import typer

from pfcli import (
    artifact,
    billing,
    checkpoint,
    credential,
    datastore,
    experiment,
    group,
    job,
    project,
    serve,
    vm,
)
from pfcli.service import ServiceType
from pfcli.service.auth import TokenType, update_token
from pfcli.service.client import UserClientService, UserSignUpService, build_client
from pfcli.service.formatter import PanelFormatter
from pfcli.utils import get_uri, secho_error_and_exit

app = typer.Typer(
    help="Welcome to PeriFlow 🤗",
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False
)
app.add_typer(credential.app, name="credential", help="Manage credentials")
app.add_typer(job.app, name="job", help="Manage jobs")
app.add_typer(checkpoint.app, name="checkpoint", help="Manage checkpoints")
app.add_typer(datastore.app, name="datastore", help="Manage datasets")
app.add_typer(vm.app, name="vm", help="Manage VMs")
app.add_typer(experiment.app, name="experiment", help="Manage experiments")
app.add_typer(serve.app, name="serve", help="Manage serves")
app.add_typer(project.app, name="project", help="Manage projects")
app.add_typer(group.app, name="org", help="Manage organizations")
app.add_typer(artifact.app, name="artifact", help="Manager artifacts")
app.add_typer(billing.app, name="billing", help="Manage billing")


user_panel_formatter = PanelFormatter(
    name="My Info",
    fields=['name', 'username', 'email'],
    headers=["Name", "Username", "Email"]
)

@app.command(help="Sign up to PerfiFlow")
def signup(
    username: str = typer.Option(..., prompt="Enter Username"),
    name: str = typer.Option(..., prompt="Enter Name"),
    email: str = typer.Option(..., prompt="Enter Email"),
    password: str = typer.Option(..., prompt="Enter Password", hide_input=True),
    confirm_password: str = typer.Option(..., prompt="Enter the password again (confirmation)", hide_input=True)
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
    password: str = typer.Option(..., prompt="Enter Password", hide_input=True)
):
    r = requests.post(get_uri("token/"), data={"username": username, "password": password})
    try:
        r.raise_for_status()
        update_token(token_type=TokenType.ACCESS, token=r.json()["access_token"])
        update_token(token_type=TokenType.REFRESH, token=r.json()["refresh_token"])

        typer.echo("\n\nLogin success!")
        typer.echo("Welcome back to...")
        typer.echo(" _____          _  _____ _")
        typer.echo("|  __ \___ _ __(_)|  ___| | _____      __")
        typer.echo("|  ___/ _ \ '__| || |__ | |/ _ \ \ /\ / /")
        typer.echo("| |  |  __/ |  | ||  __|| | (_) | V  V / ")
        typer.echo("|_|   \___|_|  |_||_|   |_|\___/ \_/\_/  ")
        typer.echo("\n\n")
    except HTTPError:
        secho_error_and_exit("Login failed... Please check your username and password.")


@app.command(help="Change your password")
def passwd(
    old_password: str = typer.Option(..., prompt="Enter your current password", hide_input=True),
    new_password: str = typer.Option(..., prompt="Enter your new password", hide_input=True),
    confirm_password: str = typer.Option(..., prompt="Enter the new password again (confirmation)", hide_input=True)
):
    if old_password == new_password:
        secho_error_and_exit("The current password is the same with the new password.")
    if new_password != confirm_password:
        secho_error_and_exit("Passwords did not match.")
    client: UserClientService = build_client(ServiceType.USER)
    client.change_password(old_password, new_password)

    typer.secho("Password is changed successfully!", fg=typer.colors.BLUE)


def _verify(_, token: str = typer.Option(..., prompt="Enter Code")):
    client: UserSignUpService = build_client(ServiceType.SIGNUP)
    client.verify(token)

    typer.echo("\n\nVerified!")
    typer.echo("Sign up success! Please sign in.")
