from typing import Optional

import tabulate
import typer
from requests import HTTPError

import autoauth
from utils import get_uri, get_group_id, secho_error_and_exit

app = typer.Typer()


@app.command()
def list():
    group_id = get_group_id()
    results = [["id", "name", "vendor", "storage_name"]]
    datastores = autoauth.get(get_uri(f"group/{group_id}/datastore/")).json()

    for datastore in datastores:
        results.append([datastore["id"], datastore["name"], datastore["vendor"], datastore["storage_name"]])
    typer.echo(tabulate.tabulate(results, headers="firstrow"))


@app.command()
def create(name: str = typer.Option(...),
           vendor: str = typer.Option(...),
           storage_name: str = typer.Option(...),
           credential_id: str = typer.Option(...)):

    group_id = get_group_id()

    request_json = {
        "name": name,
        "vendor": vendor,
        "storage_name": storage_name,
        "credential_id": credential_id
    }

    r = autoauth.post(get_uri(f"group/{group_id}/datastore/"), json=request_json)
    try:
        r.raise_for_status()
        results = [
            ["id", "name", "vendor", "storage_name"],
            [
                r.json()["id"],
                r.json()["name"],
                r.json()["vendor"],
                r.json()["storage_name"]
            ]]
        typer.echo(tabulate.tabulate(results, headers="firstrow"))
    except HTTPError:
        secho_error_and_exit(f"Datastore create failed... Error Code = {r.status_code}, Detail = {r.text}")


@app.command()
def update(datastore_id: str = typer.Option(...),
           name: Optional[str] = typer.Option(None),
           vendor: Optional[str] = typer.Option(None),
           storage_name: Optional[str] = typer.Option(None),
           credential_id: Optional[str] = typer.Option(None)):

    group_id = get_group_id()

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
        secho_error_and_exit("You need to specify at least one properties to update datastore")

    r = autoauth.patch(get_uri(f"group/{group_id}/datastore/{datastore_id}/"), json=request_json)
    try:
        r.raise_for_status()
        results = [
            ["id", "name", "vendor", "storage_name"],
            [
                r.json()["id"],
                r.json()["name"],
                r.json()["vendor"],
                r.json()["storage_name"]
            ]]
        typer.echo(tabulate.tabulate(results, headers="firstrow"))
    except HTTPError:
        secho_error_and_exit(f"Datastore update failed... Error Code = {r.status_code}, Detail = {r.text}")


@app.command()
def delete(datastore_id: str = typer.Option(...)):

    group_id = get_group_id()

    r = autoauth.get(get_uri(f"group/{group_id}/datastore/{datastore_id}"))
    try:
        r.raise_for_status()
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
    except HTTPError:
        secho_error_and_exit(f"Datastore delete failed... Error Code = {r.status_code}, Detail = {r.text}")
    datastore_delete = typer.confirm("Are you sure want to delete the datastore? This cannot be undone")
    if not datastore_delete:
        typer.Exit(1)

    r = autoauth.delete(get_uri(f"group/{group_id}/datastore/{datastore_id}"))
    try:
        r.raise_for_status()
        typer.echo(f"Successfully deleted datastore.")
    except HTTPError:
        secho_error_and_exit(f"Failed to delete datastore... Error Code = {r.status_code}, Detail = {r.text}")


if __name__ == '__main__':
    app()
