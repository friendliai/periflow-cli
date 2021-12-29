import typer
import requests
import tabulate

import autoauth
from autoauth import update_token, get_auth_header
from utils import get_uri

import checkpoint
import credential
import datastore

app = typer.Typer()
app.add_typer(credential.app, name="credential")
app.add_typer(checkpoint.app, name="checkpoint")
app.add_typer(datastore.app, name="datastore")


@app.command()
def self():
    r = autoauth.get(get_uri("user/self/"), headers=get_auth_header())
    if r.status_code == 200:
        results = [[r.json()["id"], r.json()["username"], r.json()["email"]]]
        typer.echo(tabulate.tabulate(results, headers=["id", "username", "email"]))
    else:
        typer.secho(f"Error Code = {r.status_code}, Detail = {r.json()['detail']}", err=True,
                    fg=typer.colors.RED)


@app.command()
def group():
    r = autoauth.get(get_uri("user/group/"), headers=get_auth_header())
    if r.status_code == 200:
        results = [[g["id"], g["name"]] for g in r.json()["results"]]
        typer.echo(tabulate.tabulate(results, headers=["id", "group_name"]))
    else:
        typer.secho(f"Error Code = {r.status_code}, Detail = {r.json()['detail']}", err=True,
                    fg=typer.colors.RED)


@app.command()
def login(username: str = typer.Option(..., prompt="Enter Username"),
          password: str = typer.Option(..., prompt="Enter Password", hide_input=True)):
    r = requests.post(get_uri("token/"), data={"username": username, "password": password})
    if r.status_code == 200:
        update_token(token_type="access", token=r.json()["access"])
        update_token(token_type="refresh", token=r.json()["refresh"])
        typer.echo("Login success!")
    else:
        typer.secho(f"Login failed... Please check your username and password. Response = {r.text}",
                    err=True,
                    fg=typer.colors.RED)


if __name__ == '__main__':
    app()
