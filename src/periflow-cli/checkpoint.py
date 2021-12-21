"""CLI for Checkpoint
"""
from typing import Optional

import tabulate
import typer

import autoauth

from utils import get_group_id, get_uri

app = typer.Typer()


@app.command("list", help="List all credentials that belong to the group.")
def checkpoint_list(category: Optional[str] = typer.Option(None),
                    cursor: Optional[str] = typer.Option(None),
                    limit: Optional[int] = typer.Option(None)):
    """List all checkpoints that belong to the user's group
    """
    group_id = get_group_id()
    request_data = {}

    if category is not None:
        request_data.update({"category": category})
    if cursor is not None:
        request_data.update({"cursor": cursor})
    if limit is not None:
        request_data.update({"limit": limit})

    request_data.update({"group_id": group_id})
    response = autoauth.get(get_uri("checkpoint/"), json=request_data)
    if response.status_code != 200:
        typer.secho(f"Cannot retrieve checkpoints. Error code = {response.status_code} detail = {response.text}")
        typer.Exit(1)
    checkpoints = response.json()["results"]
    next_cursor = response.json()["next_cursor"]

    headers = ["id", "category", "vendor", "storage_name", "iteration", "created_at"]
    results = []
    for checkpoint in checkpoints:
        results.append([checkpoint[header] for header in headers])

    typer.echo(f"Cursor: {next_cursor}")
    typer.echo(tabulate.tabulate(results, headers=headers))


@app.command("view")
def checkpoint_detail(checkpoint_id: str = typer.Option(...)):
    response = autoauth.get(get_uri(f"checkpoint/{checkpoint_id}/"))
    if response.status_code != 200:
        typer.secho(f"Cannot retrieve checkpoint. Error code = {response.status_code} detail = {response.text}")
        typer.Exit(1)

    checkpoint_json = response.json()
    typer.echo(f"id: {checkpoint_json['id']}")
    typer.echo(f"category: {checkpoint_json['category']}")
    typer.echo(f"vendor: {checkpoint_json['vendor']}")



# FIXME (TB): which is better design?
# 1. getting files as positional argument
#   pf checkpoint file1 file2 ... fileN --category .. --cursor .. --limit ..
# 2. getting files as repetitive keyword arguments
#   pf checkpoint --files file1 --files file2 ... --files fileN --category ..
@app.command("create", help="Create the credential.")
def checkpoint_create():
    print("create")


@app.command("update", help="Update the existing credential.")
def checkpoint_update():
    print("update")



@app.command("delete", help="Delete the existing credential.")
def checkpoint_delete():
    print("delete")


if __name__ == '__main__':
    app()
