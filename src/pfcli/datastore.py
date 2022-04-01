# Copyright (C) 2021 FriendliAI

"""PeriFlow Datastore CLi"""

from typing import Optional, List, Dict

import tabulate
import typer
import yaml
from requests import HTTPError

from pfcli.service import ServiceType, auth
from pfcli.service.client import DataClientService, GroupDataClientService, build_client
from pfcli.service.config import build_data_configurator
from pfcli.utils import get_uri, get_group_id, secho_error_and_exit

app = typer.Typer()


def _print_datastores(datastores: List[Dict]):
    headers = ["id", "name", "vendor", "storage_name", "metadata"]
    results = [
        (
            d["id"],
            d["name"],
            d["vendor"],
            d["storage_name"],
            yaml.dump(d["metadata"], indent=2) if bool(d["metadata"]) else "N/A"
        ) for d in datastores
    ]
    typer.echo(tabulate.tabulate(results, headers=headers))


@app.command()
def list():
    client: GroupDataClientService = build_client(ServiceType.GROUP_DATA)
    datastores = client.list_datastores()
    _print_datastores(datastores)


@app.command()
def view(
    name: str = typer.Option(
        ...,
        '--name',
        '-n',
        help="ID or name of datastore to see detail info."
    )
):
    group_client: GroupDataClientService = build_client(ServiceType.GROUP_DATA)
    client: DataClientService = build_client(ServiceType.DATA)

    datastore_id = group_client.get_id_by_name(name)
    datastore = client.get_datastore(datastore_id)
    _print_datastores([datastore])


@app.command()
def create():
    job_type = typer.prompt(
        "What kind job would you like to create a datastore for?\n",
        "Options: 'predefined', 'custom'",
        prompt_suffix="\n>>"
    )
    configurator = build_data_configurator(job_type)
    name, vendor, region, storage_name, credential_id, metadata = configurator.render()

    client: GroupDataClientService = build_client(ServiceType.GROUP_DATA)
    datastore = client.create_datastore(name, vendor, region, storage_name, credential_id, metadata)

    typer.secho("Datastore created successfully!", fg=typer.colors.BLUE)
    _print_datastores([datastore])


@app.command()
def update(
    datastore_id: str = typer.Option(...),
    name: Optional[str] = typer.Option(None),
    vendor: Optional[str] = typer.Option(None),
    storage_name: Optional[str] = typer.Option(None),
    credential_id: Optional[str] = typer.Option(None)
):

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

    r = auth.patch(get_uri(f"group/{group_id}/datastore/{datastore_id}/"), json=request_json)
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
        secho_error_and_exit(f"Datastore update failed...")


@app.command()
def delete(datastore_id: str = typer.Option(...)):

    group_id = get_group_id()

    r = auth.get(get_uri(f"group/{group_id}/datastore/{datastore_id}"))
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
        secho_error_and_exit(f"Datastore delete failed...")
    datastore_delete = typer.confirm("Are you sure want to delete the datastore? This cannot be undone")
    if not datastore_delete:
        typer.Exit(1)

    r = auth.delete(get_uri(f"group/{group_id}/datastore/{datastore_id}"))
    try:
        r.raise_for_status()
        typer.echo(f"Successfully deleted datastore.")
    except HTTPError:
        secho_error_and_exit(f"Datastore delete failed...")


if __name__ == '__main__':
    app()
