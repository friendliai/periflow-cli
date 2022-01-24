import requests
import tabulate
from requests import HTTPError


from pfcli import app
from autoauth import update_token, get_auth_header
from utils import get_uri, secho_error_and_exit
import autoauth


@app.command()
def self():
    r = autoauth.get(get_uri("user/self/"), headers=get_auth_header())
    try:
        r.raise_for_status()
        results = [[r.json()["id"], r.json()["username"], r.json()["email"]]]
        typer.echo(tabulate.tabulate(results, headers=["id", "username", "email"]))
    except HTTPError:
        secho_error_and_exit(f"Error Code = {r.status_code}, Detail = {r.json()['detail']}")


@app.command()
def group():
    r = autoauth.get(get_uri("user/group/"), headers=get_auth_header())
    try:
        r.raise_for_status()
        results = [[g["id"], g["name"]] for g in r.json()["results"]]
        typer.echo(tabulate.tabulate(results, headers=["id", "group_name"]))
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


if __name__ == '__main__':
    app()
