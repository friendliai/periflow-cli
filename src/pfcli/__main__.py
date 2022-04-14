# Copyright (C) 2021 FriendliAI

"""PeriFlow CLI"""

import requests
import tabulate
from requests import HTTPError

import typer

from pfcli import (
    checkpoint,
    experiment,
    credential,
    job,
    datastore,
    vm
)
from pfcli.service import ServiceType
from pfcli.service.auth import TokenType, update_token
from pfcli.service.client import UserGroupClientService, build_client
from pfcli.utils import get_uri, secho_error_and_exit

app = typer.Typer()
app.add_typer(credential.app, name="credential", help="Manage credentials")
app.add_typer(job.app, name="job", help="Manage jobs")
app.add_typer(checkpoint.app, name="checkpoint", help="Manage checkpoints")
app.add_typer(datastore.app, name="datastore", help="Manage datasets")
app.add_typer(vm.app, name="vm", help="Manage VMs")
app.add_typer(experiment.app, name="experiment", help="Manage experiments")


@app.command()
def self():
    client: UserGroupClientService = build_client(ServiceType.USER_GROUP)
    info = client.get_user_info()
    results = [(info["id"], info["username"], info["email"])]
    typer.echo(tabulate.tabulate(results, headers=["id", "username", "email"]))


@app.command()
def group():
    client: UserGroupClientService = build_client(ServiceType.USER_GROUP)
    info =  client.get_group_info()
    results = [[g["name"] for g in info]]
    typer.echo(tabulate.tabulate(results, headers=["name"]))


@app.command()
def login(
    username: str = typer.Option(..., prompt="Enter Username"),
    password: str = typer.Option(..., prompt="Enter Password", hide_input=True)
):
    r = requests.post(get_uri("token/"), data={"username": username, "password": password})
    try:
        r.raise_for_status()
        update_token(token_type=TokenType.ACCESS, token=r.json()["access"])
        update_token(token_type=TokenType.REFRESH, token=r.json()["refresh"])

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


def main():
    app()


if __name__ == "__main__":
    main()
