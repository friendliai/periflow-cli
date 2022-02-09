from typing import Optional

import tabulate
import typer
from requests import HTTPError

from pfcli import autoauth
from pfcli.utils import get_uri, get_group_id, secho_error_and_exit, cred_id_by_name

app = typer.Typer()


def datastore_id_by_name(datastore_name: str):
    group_id = get_group_id()
    r = autoauth.get(get_uri(f"group/{group_id}/datastore/"))

    try:
        r.raise_for_status()
    except HTTPError:
        secho_error_and_exit(f"Datastore listing failed... Error Code = {r.status_code}, Detail = {r.text}")
    datastores = r.json()
    try:
        datastore_id = next(datastore["id"] for datastore in datastores if datastore["name"] == datastore_name)
        return datastore_id
    except:
        secho_error_and_exit(f"Cannot find a datastore with such name")


@app.command()
def list():
    group_id = get_group_id()
    results = [["name", "vendor", "storage_name", "region"]]
    datastores = autoauth.get(get_uri(f"group/{group_id}/datastore/")).json()

    for datastore in datastores:
        results.append([datastore["name"], datastore["vendor"], datastore["storage_name"], datastore["region"]])
    typer.echo(tabulate.tabulate(results, headers="firstrow"))


@app.command()
def create(name: str = typer.Option(...),
           vendor: str = typer.Option(...),
           storage_name: str = typer.Option(...),
           credential_name: str = typer.Option(...),
           credential_type: str = typer.Option(...),
           region: str = typer.Option(...)):

    group_id = get_group_id()
    cred_id = cred_id_by_name(credential_name, credential_type)

    request_json = {
        "name": name,
        "vendor": vendor,
        "storage_name": storage_name,
        "credential_id": cred_id,
        "region": region
    }

    r = autoauth.post(get_uri(f"group/{group_id}/datastore/"), json=request_json)
    try:
        r.raise_for_status()
        results = [
            ["name", "vendor", "storage_name", "region"],
            [
                r.json()["name"],
                r.json()["vendor"],
                r.json()["storage_name"],
                r.json()["region"]
            ]]
        typer.echo(tabulate.tabulate(results, headers="firstrow"))
    except HTTPError:
        secho_error_and_exit(f"Datastore create failed... Error Code = {r.status_code}, Detail = {r.text}")


@app.command()
def update(datastore_name: str = typer.Option(...),
           new_name: Optional[str] = typer.Option(None),
           vendor: Optional[str] = typer.Option(None),
           storage_name: Optional[str] = typer.Option(None),
           region: Optional[str] = typer.Option(None),
           credential_name: Optional[str] = typer.Option(None),
           credential_type: Optional[str] = typer.Option(None)):

    group_id = get_group_id()
    datastore_id = datastore_id_by_name(datastore_name)

    request_json = {}
    if new_name is not None:
        request_json.update({"name": new_name})
    if vendor is not None:
        request_json.update({"vendor": vendor})
    if storage_name is not None:
        request_json.update({"storage_name": storage_name})
    if region is not None:
        request_json.update({"region": region})
    if (credential_name is not None) and (credential_type is not None):
        credential_id = cred_id_by_name(credential_name, credential_type)
        request_json.update({"credential_id": credential_id})
    elif credential_name is not None or credential_type is not None:
        secho_error_and_exit("You need to specify both credential name and type to update your credential")
    if not request_json:
        secho_error_and_exit("You need to specify at least one properties to update datastore")

    r = autoauth.patch(get_uri(f"group/{group_id}/datastore/{datastore_id}/"), json=request_json)
    try:
        r.raise_for_status()
        results = [
            ["name", "vendor", "storage_name", "region"],
            [
                r.json()["name"],
                r.json()["vendor"],
                r.json()["storage_name"],
                r.json()["region"]
            ]]
        typer.echo(tabulate.tabulate(results, headers="firstrow"))
    except HTTPError:
        secho_error_and_exit(f"Datastore update failed... Error Code = {r.status_code}, Detail = {r.text}")


@app.command()
def delete(datastore_name: str = typer.Option(...)):

    group_id = get_group_id()
    datastore_id = datastore_id_by_name(datastore_name)

    r = autoauth.get(get_uri(f"group/{group_id}/datastore/{datastore_id}"))
    try:
        r.raise_for_status()
        typer.secho("Datastore to be deleted", fg=typer.colors.MAGENTA)
        results = [
            ["name", "vendor", "storage_name", "region"],
            [
                r.json()["name"],
                r.json()["vendor"],
                r.json()["storage_name"],
                r.json()["region"]
            ]]
        typer.echo(tabulate.tabulate(results, headers="firstrow"))
    except HTTPError:
        secho_error_and_exit(f"Datastore delete failed... Error Code = {r.status_code}, Detail = {r.text}")
    datastore_delete = typer.confirm("Are you sure want to delete the datastore? This cannot be undone")
    if not datastore_delete:
        raise typer.Exit(1)

    r = autoauth.delete(get_uri(f"group/{group_id}/datastore/{datastore_id}"))
    try:
        r.raise_for_status()
        typer.echo(f"Successfully deleted datastore.")
    except HTTPError:
        secho_error_and_exit(f"Failed to delete datastore... Error Code = {r.status_code}, Detail = {r.text}")


if __name__ == '__main__':
    app()
