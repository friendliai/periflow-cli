from typing import List, Optional

import requests
import typer

from utils import get_uri, auto_token_refresh, get_auth_header

import autoauth

app = typer.Typer()


@app.command("list", help="List all credentials that belong to the group.")
def checkpoint_list(file: List[str] = typer.Option(...),
                    category: Optional[str] = typer.Option(None),
                    cursor: Optional[str] = typer.Option(None),
                    limit: Optional[int] = typer.Option(None)):
    print("list", file, category)
    return
    response = autoauth.get(get_uri("user/group"))
    if response.status_code != 200:
        typer.echo(response.text)
        typer.Exit(1)

    groups = response.json()["results"]
    request_data = {}

    if category is not None:
        request_data.update({"category": category})
    if cursor is not None:
        request_data.update({"cursor": cursor})
    if limit is not None:
        request_data.update({"limit": limit})

    checkpoints = []
    for group in groups:
        request_data.update({"group_id": group["id"]})
        response = autoauth.get(get_uri("checkpoint/"), json=request_data)
        if response.status_code != 200:
            typer.echo(response.text)
            typer.Exit(1)
        checkpoints += response.json()["results"]

    typer.echo(checkpoints)


@auto_token_refresh
def try_create():
    pass


@app.command("create", help="Create the credential.")
def checkpoint_create():
    print("create")


@auto_token_refresh
def try_update():
    pass


@app.command("update", help="Update the existing credential.")
def checkpoint_update():
    print("update")


@auto_token_refresh
def try_delete():
    pass


@app.command("delete", help="Delete the existing credential.")
def checkpoint_delete():
    print("delete")


if __name__ == '__main__':
    app()
