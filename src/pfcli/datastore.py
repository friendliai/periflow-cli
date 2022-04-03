# Copyright (C) 2021 FriendliAI

"""PeriFlow Datastore CLI"""

from typing import Optional, List, Dict
from click import Choice

import tabulate
import typer
import yaml

from pfcli.service import CloudType, JobType, ServiceType
from pfcli.service.client import DataClientService, GroupDataClientService, build_client
from pfcli.service.config import build_data_configurator
from pfcli.utils import secho_error_and_exit

app = typer.Typer()


def _print_datastores(datastores: List[Dict], show_detail: bool = False):
    headers = ["id", "name", "cloud", "storage_name"]
    if show_detail:
        headers.append("metadata")
    results = []
    for d in datastores:
        info = [d["id"], d["name"], d["vendor"], d["storage_name"]]
        if show_detail:
            info.append(yaml.dump(d["metadata"], indent=2) if bool(d["metadata"]) else "N/A")
        results.append(info)
    typer.echo(tabulate.tabulate(results, headers=headers))


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    interactive: bool = typer.Option(
        False,
        '--interactive',
        help="Use interactive mode."
    )
):
    supported_command = ('create',)
    if not interactive:
        return

    if ctx.invoked_subcommand not in supported_command:
        secho_error_and_exit(f"'--interactive' option is not supported for '{ctx.invoked_subcommand}' command.")

    if ctx.invoked_subcommand == 'create':
        job_type = typer.prompt(
            "What kind job would you like to create a datastore for?\n",
            type=Choice([e.value for e in JobType]),
            prompt_suffix="\n>>"
        )
        configurator = build_data_configurator(job_type)
        name, cloud, region, storage_name, credential_id, metadata = configurator.render()

        client: GroupDataClientService = build_client(ServiceType.GROUP_DATA)
        datastore = client.create_datastore(name, cloud, region, storage_name, credential_id, metadata)

        typer.secho("Datastore created successfully!", fg=typer.colors.BLUE)

    _print_datastores([datastore])
    exit(0)


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
    _print_datastores([datastore], show_detail=True)


@app.command()
def create(
    name: Optional[str] = typer.Option(
        ...,
        '--name',
        '-n',
        help="Name of your datastore to create."
    ),
    cloud: Optional[CloudType] = typer.Option(
        ...,
        '--cloud',
        '-c',
        help="Name of cloud storage vendor where your dataset is uploaded."
    ),
    region: Optional[str] = typer.Option(
        ...,
        '--region',
        '-r',
        help="Cloud storage region where your dataset is uploaded."
    ),
    storage_name: Optional[str] = typer.Option(
        ...,
        '--storage-name',
        '-s',
        help="The name of cloud storage where your dataset is uploaded."
    ),
    credential_id: Optional[str] = typer.Option(
        ...,
        '--credential-id',
        '-i',
        help="Credential UUID to access the cloud storage."
    ),
    metadata_file: Optional[typer.FileText] = typer.Option(
        None,
        '--metadata-file',
        '-f',
        help="Path to file containing the metadata describing your dataset."
    ),
):
    metadata = {}
    if metadata_file is not None:
        try:
            metadata = yaml.safe_load(metadata_file)
        except yaml.YAMLError as exc:
            secho_error_and_exit(f"Error occurred while parsing metadata file... {exc}")

    client: GroupDataClientService = build_client(ServiceType.GROUP_DATA)
    datastore = client.create_datastore(name, cloud, region, storage_name, credential_id, metadata)

    typer.secho(f"Datastore ({name}) is created successfully!", fg=typer.colors.BLUE)
    _print_datastores([datastore])


@app.command()
def update(
    target_name: str = typer.Option(
        ...,
        '--taget-name-',
        '-n',
        help='The name of datastore to update.'
    ),
    new_name: Optional[str] = typer.Option(
        None,
        '--new-name',
        '-nn',
        help='The new name of datastore.'
    ),
    cloud: Optional[CloudType] = typer.Option(
        None,
        '--cloud',
        '-c',
        help="Name of cloud storage vendor where your dataset is uploaded."
    ),
    region: Optional[str] = typer.Option(
        None,
        '--region',
        '-r',
        help="Cloud storage region where your dataset is uploaded."
    ),
    storage_name: Optional[str] = typer.Option(
        None,
        '--storage-name',
        '-s',
        help="The name of cloud storage where your dataset is uploaded."
    ),
    credential_id: Optional[str] = typer.Option(
        None,
        '--credential-id',
        '-i',
        help="Credential UUID to access the cloud storage."
    ),
    metadata_file: Optional[typer.FileText] = typer.Option(
        None,
        '--metadata-file',
        '-f',
        help="Path to file containing the metadata describing your dataset."
    ),
):
    group_client: GroupDataClientService = build_client(ServiceType.GROUP_DATA)
    datastore_id = group_client.get_id_by_name(target_name)

    metadata = None
    if metadata_file is not None:
        try:
            metadata = yaml.safe_load(metadata_file)
            metadata = metadata or {}
        except yaml.YAMLError as exc:
            secho_error_and_exit(f"Error occurred while parsing metadata file... {exc}")

    client: DataClientService = build_client(ServiceType.DATA)
    datastore = client.update_datastore(
        datastore_id,
        name=new_name,
        vendor=cloud,
        region=region,
        storage_name=storage_name,
        credential_id=credential_id,
        metadata=metadata
    )

    typer.secho("Datastore is updated successfully!", fg=typer.colors.BLUE)
    _print_datastores([datastore])


@app.command()
def delete(
    name: str = typer.Option(
        ...,
        '--name',
        '-n',
        help="Name of datastore to delete.",
    ),
    force: bool = typer.Option(
        False,
        '--force',
        '-f',
        help="Forcefully delete datastore without confirmation prompt."
    )
):
    if not force:
        do_delete = typer.confirm("Are your sure to delete datastore?")
        if not do_delete:
            raise typer.Abort()

    client: DataClientService = build_client(ServiceType.DATA)
    group_client: GroupDataClientService = build_client(ServiceType.GROUP_DATA)

    datastore_id = group_client.get_id_by_name(name)
    if datastore_id is None:
        secho_error_and_exit(f"Datastore ({name}) is not found.")

    client.delete_datastore(datastore_id)

    typer.secho(f"Datastore ({name}) deleted successfully!", fg=typer.colors.BLUE)


if __name__ == '__main__':
    app()
