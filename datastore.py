import typer
import yaml
from pathlib import Path
from typing import Optional

from utils import get_uri
import autoauth
import tabulate

app = typer.Typer()


@app.command()
def list():
    groups = autoauth.get(get_uri("user/group/")).json()["results"]
    if len(groups) == 0:
        typer.secho("You are not assigned to any group... Please contact to admin", err=True, fg=typer.colors.RED)
        typer.Exit(1)
    for group in groups:
        typer.secho(f"Group Name: {group['name']}", fg=typer.colors.MAGENTA)
        results = [["id", "name", "vendor", "storage_name"]]
        datastores = autoauth.get(get_uri(f"group/{group['id']}/datastore/")).json()
        for datastore in datastores:
            results.append([datastore["id"], datastore["name"], datastore["vendor"], datastore["storage_name"]])
        typer.echo(tabulate.tabulate(results, headers="firstrow"))


@app.command()
def create(name: str = typer.Option(...),
           vendor: str = typer.Option(...),
           storage_name: str = typer.Option(...),
           credential_id: str = typer.Option(...)):
    groups = autoauth.get(get_uri("user/group/")).json()["results"]
    if len(groups) == 0:
        typer.secho("You are not assigned to any group... Please contact to admin",
                    err=True,
                    fg=typer.colors.RED)
        typer.Exit(1)

    group_id = groups[0]["id"]

    request_json = {
        "name": name,
        "vendor": vendor,
        "storage_name": storage_name,
        "credential_id": credential_id
    }

    r = autoauth.post(get_uri(f"group/{group_id}/datastore/"), json=request_json)
    if r.status_code == 201:
        results = [
            ["id", "name", "vendor", "storage_name"],
            [
                r.json()["id"],
                r.json()["name"],
                r.json()["vendor"],
                r.json()["storage_name"]
            ]]
        typer.echo(tabulate.tabulate(results, headers="firstrow"))
    else:
        typer.secho(r.text, err=True, color=typer.colors.RED)


@app.command()
def update(datastore_id: str = typer.Option(...),
           name: str = typer.Option(None),
           vendor: str = typer.Option(None),
           storage_name: str = typer.Option(None),
           credential_id: str = typer.Option(None)):
    groups = autoauth.get(get_uri("user/group/")).json()["results"]
    if len(groups) == 0:
        typer.secho("You are not assigned to any group... Please contact to admin",
                    err=True,
                    fg=typer.colors.RED)
        typer.Exit(1)

    group_id = groups[0]["id"]

    request_json = {}
    if name is not None:
        request_json.update({"name": name})
    if vendor is not None:
        request_json.update({"vendor": vendor})
    if storage_name is not None:
        request_json.update({"storage_name": storage_name})
    if credential_id is not None:
        request_json.update({"credential_id": credential_id})
    if not request_json:
        typer.secho("You need to specify at least one properties to update datastore",
                    err=True,
                    fg=typer.colors.RED)
    r = autoauth.patch(get_uri(f"group/{group_id}/datastore/{datastore_id}/"), json=request_json)
    if r.status_code == 200:
        results = [
            ["id", "name", "vendor", "storage_name"],
            [
                r.json()["id"],
                r.json()["name"],
                r.json()["vendor"],
                r.json()["storage_name"]
            ]]
        typer.echo(tabulate.tabulate(results, headers="firstrow"))
    else:
        typer.secho(r.text, err=True, color=typer.colors.RED)


@app.command()
def delete(datastore_id: str = typer.Option(...)):
    groups = autoauth.get(get_uri("user/group/")).json()["results"]
    if len(groups) == 0:
        typer.secho("You are not assigned to any group... Please contact to admin",
                    err=True,
                    fg=typer.colors.RED)
        typer.Exit(1)
    if len(groups) > 1:
        typer.secho("Currently we do not support users with more than two groups... Please contact admin",
                    err=True,
                    fg=typer.colors.RED)
        typer.Exit(1)

    group_id = groups[0]["id"]

    r = autoauth.get(get_uri(f"group/{group_id}/datastore/{datastore_id}"))
    if r.status_code == 200:
        typer.secho("Datastore to be deleted", fg=typer.colors.MAGENTA)
        results = [
            ["id", "name", "vendor", "storage_name"],
            [
                r.json()["id"],
                r.json()["name"],
                r.json()["vendor"],
                r.json()["storage_name"]
            ]]
        typer.echo(tabulate.tabulate(results, headers="firstrow"))
    else:
        typer.secho(r.text, err=True, color=typer.colors.RED)
        typer.Exit(1)
    datastore_delete = typer.confirm("Are you sure want to delete the datastore? This cannot be undone")
    if not datastore_delete:
        typer.Exit(1)

    r = autoauth.delete(get_uri(f"group/{group_id}/datastore/{datastore_id}"))
    if r.status_code == 204:
        typer.echo(f"Successfully deleted datastore.")
    else:
        typer.secho(r.text, err=True, color=typer.colors.RED)


if __name__ == '__main__':
    app()
