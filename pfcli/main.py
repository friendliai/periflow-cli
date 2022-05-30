# Copyright (C) 2021 FriendliAI

"""PeriFlow CLI"""

import requests
from requests import HTTPError

import typer

from pfcli import (
    checkpoint,
    experiment,
    credential,
    job,
    datastore,
    vm,
    serve
)
from pfcli.service import ServiceType
from pfcli.service.auth import TokenType, get_current_user_id, get_current_userinfo, update_token
from pfcli.service.client import UserClientService, build_client
from pfcli.service.formatter import PanelFormatter, TableFormatter
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


user_panel_formatter = PanelFormatter(
    name="My Info",
    fields=['name', 'username', 'email'],
    headers=["Name", "Username", "Email"]
)
org_table_formatter = TableFormatter(
    name="Organization",
    fields=['name'],
    headers=["Name"]
)


@app.command(help="Show who am I")
def whoami():
    info = get_current_userinfo()
    user_panel_formatter.render([info])


@app.command(name="org", help="Show what organizations I belong to")
def organization():
    client: UserClientService = build_client(ServiceType.USER, pf_user_id=get_current_user_id())
    orgs = client.get_group_info()
    org_table_formatter.render(orgs)


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
    client: UserClientService = build_client(ServiceType.USER, pf_user_id=get_current_user_id())
    client.change_password(old_password, new_password)

    typer.secho("Password is changed successfully!", fg=typer.colors.BLUE)
