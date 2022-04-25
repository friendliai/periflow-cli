# Copyright (C) 2021 FriendliAI

"""PeriFlow Datastore CLI"""

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
    storage_type_map_inv,
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

app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False
)

table_formatter = TableFormatter(
    name="Datastore",
    fields=['name', 'vendor', 'region', 'storage_name', 'active'],
    headers=['Name', 'Cloud', 'Region', 'Storage Name', 'Active'],
)
table_formatter.add_substitution_rule("True", "✓")
table_formatter.add_substitution_rule("False", "x")
table_formatter.apply_styling("Active", style="cyan")

panel_formatter = PanelFormatter(
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

    datastore['vendor'] = storage_type_map_inv[datastore['vendor']].value
    panel_formatter.render([datastore], show_detail=True)
    exit(0)


@app.command()
def list():
    """List datasets in datastore.
    """
    client: GroupDataClientService = build_client(ServiceType.GROUP_DATA)
    datastores = client.list_datastores()
    for datastore in datastores:
        datastore['vendor'] = storage_type_map_inv[datastore['vendor']].value
    table_formatter.render(datastores)


@app.command()
def view(
    name: str = typer.Argument(
        ...,
        help="ID or name of datastore to see detail info."
    )
):
    """View the detail of a dataset.
    """
    group_client: GroupDataClientService = build_client(ServiceType.GROUP_DATA)
    client: DataClientService = build_client(ServiceType.DATA)

    datastore_id = group_client.get_id_by_name(name)
    datastore = client.get_datastore(datastore_id)
    datastore['vendor'] = storage_type_map_inv[datastore['vendor']].value
    panel_formatter.render([datastore], show_detail=True)
    tree_formatter.render(datastore['files'])
    json_formatter.render(datastore['metadata'])


@app.command()
def link(
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
             "The metadata should be written in YAML format."
    )
):
    """Link user's own cloud storage to PeriFlow datastore.
    """
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
    datastore['vendor'] = storage_type_map_inv[datastore['vendor']].value

    typer.secho(f"Datastore ({name}) is created successfully!", fg=typer.colors.BLUE)
    panel_formatter.render([datastore], show_detail=True)
    tree_formatter.render(datastore['files'])
    json_formatter.render(datastore['metadata'])


@app.command()
def upload(
    name: str = typer.Argument(
        ...,
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
             "The metadata should be written in YAML format."
    )
):
    """Create a dataset by uploading dataset files in my local file system.
    The created dataset will have "fai" cloud type.
    """
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
    datastore['vendor'] = storage_type_map_inv[datastore['vendor']].value

    typer.secho(f"Objects are uploaded to datastore ({name}) successfully!", fg=typer.colors.BLUE)
    panel_formatter.render([datastore], show_detail=True)
    tree_formatter.render(datastore['files'])
    json_formatter.render(datastore['metadata'])


@app.command()
def edit(
    name: str = typer.Argument(
        ...,
        help='The name of datastore to update.'
    ),
    new_name: Optional[str] = typer.Option(
        None,
        '--name',
        '-n',
        help='The new name of datastore.'
    ),
    metadata_file: Optional[typer.FileText] = typer.Option(
        None,
        '--metadata-file',
        '-f',
        help="Path to file containing the metadata describing your dataset. "
             "The metadata should be written in YAML format."
    )
):
    """Edit metadata of dataset.
    """
    group_client: GroupDataClientService = build_client(ServiceType.GROUP_DATA)
    datastore_id = group_client.get_id_by_name(name)

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
        metadata=metadata,
        active=True
    )
    datastore['vendor'] = storage_type_map_inv[datastore['vendor']].value

    typer.secho("Datastore is updated successfully!", fg=typer.colors.BLUE)
    panel_formatter.render([datastore], show_detail=True)
    tree_formatter.render(datastore['files'])
    json_formatter.render(datastore['metadata'])


@app.command()
def delete(
    name: str = typer.Argument(
        ...,
        help="Name of datastore to delete.",
    ),
    force: bool = typer.Option(
        False,
        '--force',
        '-f',
        help="Forcefully delete datastore without confirmation prompt."
    )
):
    """Delete dataset from datastore.
    If the dataset was linked from user's cloud storage by `pf datastore link` command, then the storage will not be
    deleted and only the DB record is deleted. If the dataset was uploaded by `pf datastore upload` command, the storage
    will be deleted along with the DB record.
    """
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
