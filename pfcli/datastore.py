# Copyright (C) 2021 FriendliAI

"""PeriFlow Datastore CLI"""

import json
from pathlib import Path
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
from pfcli.service.formatter import (
    JSONFormatter,
    PanelFormatter,
    TableFormatter,
    TreeFormatter,
)
from pfcli.utils import (
    secho_error_and_exit,
    upload_files,
    get_file_info,
)

app = typer.Typer()

table = TableFormatter(
    name="Datastore",
    fields=['name', 'vendor', 'region', 'storage_name', 'active'],
    headers=['Name', 'Cloud', 'Region', 'Storage Name', 'Active'],
)
table.add_substitution_rule("True", "✔️")
table.add_substitution_rule("False", "x")
table.apply_styling("Active", style="cyan")

panel = PanelFormatter(
    name="Overview",
    fields=['name', 'vendor', 'region', 'storage_name', 'active'],
    headers=['Name', 'Cloud', 'Region', 'Storage Name', 'Active'],
)

json_formatter = JSONFormatter(name="Metadata")
tree_formatter = TreeFormatter(name="Files")


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
            prompt_suffix="\n>> "
        )
        configurator = build_data_configurator(job_type)
        name, cloud, region, storage_name, credential_id, metadata, files = configurator.render()

        client: GroupDataClientService = build_client(ServiceType.GROUP_DATA)
        datastore = client.create_datastore(name, cloud, region, storage_name, credential_id, metadata, files, True)

        typer.secho("Datastore created successfully!", fg=typer.colors.BLUE)

    panel.render([datastore], show_detail=True)
    exit(0)


@app.command()
def list():
    client: GroupDataClientService = build_client(ServiceType.GROUP_DATA)
    datastores = client.list_datastores()
    table.render(datastores)


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
    panel.render([datastore], show_detail=True)
    tree_formatter.render(datastore['files'])
    json_formatter.render(datastore['metadata'])


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
    ),
):
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
    typer.echo(table.render([datastore], show_detail=True))


@app.command()
def upload(
    name: str = typer.Option(
        ...,
        '--name',
        '-n',
        help="Name of your datastore to upload objects. If not exists, a new datastore will be created."
    ),
    source_path: Path = typer.Option(
        ...,
        '--source-path',
        '-p',
        help="Path to source file or directory to upload."
    ),
    metadata_file: Optional[typer.FileText] = typer.Option(
        None,
        '--metadata-file',
        '-f',
        help="Path to file containing the metadata describing your dataset."
    ),
):
    client: DataClientService = build_client(ServiceType.DATA)
    group_client: GroupDataClientService = build_client(ServiceType.GROUP_DATA)

    datastore_id = group_client.get_id_by_name(name)
    metadata = None
    if datastore_id is None:
        typer.echo(f"Creating datastore ({name})...")
        metadata = {}
        if metadata_file is not None:
            try:
                metadata = yaml.safe_load(metadata_file)
            except yaml.YAMLError as exc:
                secho_error_and_exit(f"Error occurred while parsing metadata file... {exc}")

        datastore = group_client.create_datastore(name, StorageType.FAI, '', '', None, metadata, [], False)
        typer.secho(f"Datastore ({name}) is created successfully.", fg=typer.colors.BLUE)
        datastore_id = datastore['id']

    typer.echo(f"Start uploading objects to datastore ({name})...")
    url_dicts = client.get_upload_urls(datastore_id, source_path)
    upload_files(url_dicts)

    datastore = client.update_datastore(
        datastore_id,
        files=[
            get_file_info(url_info['path']) for url_info in url_dicts
        ],
        metadata=metadata,
        active=True
    )
    typer.secho(f"Objects are uploaded to datastore ({name}) successfully!", fg=typer.colors.BLUE)
    typer.echo(table.render([datastore], show_detail=True))


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
    typer.echo(table.render([datastore], show_detail=True))


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
