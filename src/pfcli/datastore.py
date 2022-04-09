# Copyright (C) 2021 FriendliAI

"""PeriFlow Datastore CLI"""

from typing import Optional
from click import Choice

import typer
import yaml

from pfcli.service import (
    StorageType,
    JobType,
    ServiceType,
    cred_type_map,
    cred_type_map_inv,
)
from pfcli.service.client import (
    CredentialClientService,
    DataClientService,
    GroupDataClientService,
    build_client,
)
from pfcli.service.cloud import build_storage_helper
from pfcli.service.config import build_data_configurator
from pfcli.service.formatter import TableFormatter
from pfcli.utils import secho_error_and_exit, validate_storage_region

app = typer.Typer()

formatter = TableFormatter(
    fields=['id', 'name', 'vendor', 'storage_name'],
    headers=['id', 'name', 'cloud', 'storage name'],
    extra_fields=['metadata'],
    extra_headers=['metadata']
)


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

    datastore['metadata'] = yaml.dump(datastore['metadata'], indent=2) if datastore['metadata'] else "N/A"
    typer.echo(formatter.render([datastore], show_detail=True))
    exit(0)


@app.command()
def list():
    client: GroupDataClientService = build_client(ServiceType.GROUP_DATA)
    datastores = client.list_datastores()
    typer.echo(formatter.render(datastores))


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
    datastore['metadata'] = yaml.dump(datastore['metadata'], indent=2) if datastore['metadata'] else "N/A"
    typer.echo(formatter.render([datastore], show_detail=True))


@app.command()
def create(
    name: str = typer.Option(
        ...,
        '--name',
        '-n',
        help="Name of your datastore to create."
    ),
    cloud: Optional[StorageType] = typer.Option(
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
    )
):
    validate_storage_region(cloud, region)

    credential_client: CredentialClientService = build_client(ServiceType.CREDENTIAL)
    credential = credential_client.get_credential(credential_id)
    if credential["type"] != cred_type_map[cloud]:
        secho_error_and_exit(
            f"Credential type and cloud vendor mismatch: {cred_type_map_inv[credential['type']]} and {cloud}."
        )

    storage_helper = build_storage_helper(cloud, credential['value'])
    files = storage_helper.list_storage_files(storage_name)

    metadata = {}
    if metadata_file is not None:
        try:
            metadata = yaml.safe_load(metadata_file)
        except yaml.YAMLError as exc:
            secho_error_and_exit(f"Error occurred while parsing metadata file... {exc}")

    client: GroupDataClientService = build_client(ServiceType.GROUP_DATA)
    datastore = client.create_datastore(name, cloud, region, storage_name, credential_id, metadata, files, True)

    typer.secho(f"Datastore ({name}) is created successfully!", fg=typer.colors.BLUE)
    datastore['metadata'] = yaml.dump(datastore['metadata'], indent=2) if datastore['metadata'] else "N/A"
    typer.echo(formatter.render([datastore], show_detail=True))


@app.command()
def update(
    target_name: str = typer.Option(
        ...,
        '--taget-name',
        '-n',
        help='The name of datastore to update.'
    ),
    new_name: Optional[str] = typer.Option(
        None,
        '--new-name',
        '-nn',
        help='The new name of datastore.'
    ),
    cloud: Optional[StorageType] = typer.Option(
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
        metadata=metadata,
        active=True
    )

    typer.secho("Datastore is updated successfully!", fg=typer.colors.BLUE)
    datastore['metadata'] = yaml.dump(datastore['metadata'], indent=2) if datastore['metadata'] else "N/A"
    typer.echo(formatter.render([datastore], show_detail=True))


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
