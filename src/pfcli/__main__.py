import requests
import tabulate
from requests import HTTPError

import typer

from pfcli import checkpoint
from pfcli import credential
from pfcli import job
from pfcli import datastore
from pfcli import vm
from pfcli import autoauth
from pfcli.autoauth import update_token, get_auth_header
from pfcli.utils import get_uri, secho_error_and_exit

app = typer.Typer()
app.add_typer(credential.app, name="credential")
app.add_typer(job.app, name="job")
app.add_typer(checkpoint.app, name="checkpoint")
app.add_typer(datastore.app, name="datastore")
app.add_typer(vm.app, name="vm")


@app.command()
def self():
    r = autoauth.get(get_uri("user/self/"), headers=get_auth_header())
    try:
        r.raise_for_status()
        results = [[r.json()["username"], r.json()["email"]]]
        typer.echo(tabulate.tabulate(results, headers=["username", "email"]))
    except HTTPError:
        secho_error_and_exit(f"Error Code = {r.status_code}, Detail = {r.json()['detail']}")


@app.command()
def group():
    r = autoauth.get(get_uri("user/group/"), headers=get_auth_header())
    try:
        r.raise_for_status()
        results = [[g["name"]] for g in r.json()["results"]]
        typer.echo(tabulate.tabulate(results, headers=["group_name"]))
    except HTTPError:
        secho_error_and_exit(f"Error Code = {r.status_code}, Detail = {r.json()['detail']}")


@app.command()
def login(username: str = typer.Option(..., prompt="Enter Username"),
          password: str = typer.Option(..., prompt="Enter Password", hide_input=True)):
    r = requests.post(get_uri("token/"), data={"username": username, "password": password})
    try:
        r.raise_for_status()
        update_token(token_type="access", token=r.json()["access"])
        update_token(token_type="refresh", token=r.json()["refresh"])
        typer.echo("Login success!")
    except HTTPError:
        secho_error_and_exit("Login failed... Please check your username and password.")


def main():
    app()


if __name__ == "__main__":
    main()
