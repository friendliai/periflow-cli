# Copyright (C) 2021 FriendliAI

"""PeriFlow Dataset CLI"""

from pathlib import Path
from typing import Optional
from uuid import UUID

from click import Choice
from rich.text import Text
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
    ProjectDataClientService,
    build_client,
)
from pfcli.service.client.data import FileSizeType, expand_paths
from pfcli.service.cloud import build_storage_helper
from pfcli.service.config import build_data_configurator
from pfcli.service.formatter import (
    JSONFormatter,
    PanelFormatter,
    TableFormatter,
    TreeFormatter,
)
from pfcli.utils.format import secho_error_and_exit
from pfcli.utils.fs import get_file_info
from pfcli.utils.validate import validate_cloud_storage_type

app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False,
)

table_formatter = TableFormatter(
    name="Datasets",
    fields=["name", "vendor", "region", "storage_name", "active"],
    headers=["Name", "Cloud", "Region", "Storage Name", "Active"],
)
table_formatter.add_substitution_rule("True", Text("Y", style="green"))
table_formatter.add_substitution_rule("False", Text("N", style="red"))
table_formatter.add_substitution_rule("", "-")

panel_formatter = PanelFormatter(
    name="Overview",
    fields=["name", "vendor", "region", "storage_name", "active"],
    headers=["Name", "Cloud", "Region", "Storage Name", "Active"],
)
panel_formatter.add_substitution_rule("True", Text("Y", style="green"))
panel_formatter.add_substitution_rule("False", Text("N", style="red"))
panel_formatter.add_substitution_rule("", "-")

json_formatter = JSONFormatter(name="Metadata")
tree_formatter = TreeFormatter(name="Files")


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    interactive: bool = typer.Option(
        False, "--interactive", help="Use interactive mode."
    ),
):
    supported_command = ("create",)
    if not interactive:
        return

    if ctx.invoked_subcommand not in supported_command:
        secho_error_and_exit(
            f"'--interactive' option is not supported for '{ctx.invoked_subcommand}' command."
        )

    if ctx.invoked_subcommand == "create":
        job_type = typer.prompt(
            "What kind job would you like to create a dataset for?\n",
            type=Choice([e.value for e in JobType]),
            prompt_suffix="\n>> ",
        )
        configurator = build_data_configurator(job_type)
        (
            name,
            cloud,
            region,
            storage_name,
            credential_id,
            metadata,
            files,
        ) = configurator.render()

        client: ProjectDataClientService = build_client(ServiceType.PROJECT_DATA)
        dataset = client.create_dataset(
            name, cloud, region, storage_name, credential_id, metadata, files, True
        )

        typer.secho("Dataset is created successfully!", fg=typer.colors.BLUE)

        dataset["vendor"] = storage_type_map_inv[dataset["vendor"]].value
        panel_formatter.render([dataset], show_detail=True)
        exit(0)


@app.command()
def list():
    """List datasets in dataset."""
    client: ProjectDataClientService = build_client(ServiceType.PROJECT_DATA)
    datasets = client.list_datasets()
    for dataset in datasets:
        dataset["vendor"] = storage_type_map_inv[dataset["vendor"]].value
    table_formatter.render(datasets)


@app.command()
def view(
    name: str = typer.Argument(..., help="ID or name of dataset to see detail info.")
):
    """View the detail of a dataset."""
    project_client: ProjectDataClientService = build_client(ServiceType.PROJECT_DATA)
    client: DataClientService = build_client(ServiceType.DATA)

    dataset_id = project_client.get_id_by_name(name)
    if dataset_id is None:
        secho_error_and_exit(f"Dataset with name ({name}) is not found.")
    dataset = client.get_dataset(dataset_id)  # type: ignore
    dataset["vendor"] = storage_type_map_inv[dataset["vendor"]].value
    panel_formatter.render([dataset], show_detail=True)
    tree_formatter.render(dataset["files"])
    json_formatter.render(dataset["metadata"])


@app.command()
def create(
    name: str = typer.Option(
        ..., "--name", "-n", help="Name of your dataset to create."
    ),
    cloud_storage: StorageType = typer.Option(
        ...,
        "--cloud-storage",
        "-c",
        help="The cloud storage vendor where your dataset is uploaded.",
        callback=validate_cloud_storage_type,
    ),
    region: str = typer.Option(
        ...,
        "--region",
        "-r",
        help="Cloud storage region where your dataset is uploaded.",
    ),
    storage_name: str = typer.Option(
        ...,
        "--storage-name",
        "-s",
        help="The name of cloud storage where your dataset is uploaded.",
    ),
    storage_path: Optional[str] = typer.Option(
        None,
        "--storage-path",
        "-p",
        help="File or direcotry path of cloud storage. The root of the storage will be used by default.",
    ),
    credential_id: UUID = typer.Option(
        ...,
        "--credential-id",
        "-i",
        help="Credential UUID to access the cloud storage.",
    ),
    metadata_file: Optional[typer.FileText] = typer.Option(
        None,
        "--metadata-file",
        "-f",
        help="Path to file containing the metadata describing your dataset."
        "The metadata should be written in YAML format.",
    ),
):
    """Link user's own cloud storage to PeriFlow dataset.
    Use `pf dataset --interactive create` command to create a dataset with interactive prompt.
    """
    credential_client: CredentialClientService = build_client(ServiceType.CREDENTIAL)
    credential = credential_client.get_credential(credential_id)
    if credential["type"] != cred_type_map[cloud_storage.value]:
        secho_error_and_exit(
            "Credential type and cloud vendor mismatch: "
            f"{cred_type_map_inv[credential['type']]} and {cloud_storage.value}."
        )

    storage_helper = build_storage_helper(cloud_storage, credential["value"])
    if storage_path is not None:
        storage_path = storage_path.strip("/")
    files = storage_helper.list_storage_files(storage_name, storage_path)
    if storage_path is not None:
        storage_name = f"{storage_name}/{storage_path}"

    metadata = {}
    if metadata_file is not None:
        try:
            metadata = yaml.safe_load(metadata_file)
        except yaml.YAMLError as exc:
            secho_error_and_exit(f"Error occurred while parsing metadata file... {exc}")

    client: ProjectDataClientService = build_client(ServiceType.PROJECT_DATA)
    dataset = client.create_dataset(
        name=name,
        vendor=cloud_storage,
        region=region,
        storage_name=storage_name,
        credential_id=credential_id,
        metadata=metadata,
        files=files,
        active=True,
    )
    dataset["vendor"] = storage_type_map_inv[dataset["vendor"]].value

    typer.secho(f"Dataset ({name}) is created successfully!", fg=typer.colors.BLUE)
    panel_formatter.render([dataset], show_detail=True)
    tree_formatter.render(dataset["files"])
    json_formatter.render(dataset["metadata"])


@app.command()
def upload(
    name: str = typer.Option(
        ..., "--name", "-n", help="Name of your dataset to upload objects."
    ),
    source_path: str = typer.Option(
        ..., "--source-path", "-p", help="Path to source file or directory to upload."
    ),
    metadata_file: Optional[typer.FileText] = typer.Option(
        None,
        "--metadata-file",
        "-f",
        help="Path to file containing the metadata describing your dataset."
        "The metadata should be written in YAML format.",
    ),
):
    """Create a dataset by uploading dataset files in my local file system.
    The created dataset will have "fai" cloud type.
    """
    client: DataClientService = build_client(ServiceType.DATA)
    project_client: ProjectDataClientService = build_client(ServiceType.PROJECT_DATA)
    expand = source_path.endswith("/")
    src_path: Path = Path(source_path)

    dataset_id = project_client.get_id_by_name(name)
    if dataset_id is not None:
        secho_error_and_exit(f"The dataset with the same name ({name}) already exists.")

    typer.echo(f"Creating dataset ({name})...")
    metadata = {}
    if metadata_file is not None:
        try:
            metadata = yaml.safe_load(metadata_file)
        except yaml.YAMLError as exc:
            secho_error_and_exit(f"Error occurred while parsing metadata file... {exc}")

    dataset = project_client.create_dataset(
        name=name,
        vendor=StorageType.FAI,
        region="",
        storage_name="",
        credential_id=None,
        metadata=metadata,
        files=[],
        active=False,
    )
    dataset_id = dataset["id"]

    try:
        typer.echo(f"Start uploading objects to dataset ({name})...")
        spu_targets = expand_paths(src_path, expand, FileSizeType.SMALL)
        mpu_targets = expand_paths(src_path, expand, FileSizeType.LARGE)
        spu_url_dicts = (
            client.get_spu_urls(dataset_id=dataset_id, paths=spu_targets)
            if len(spu_targets) > 0
            else []
        )
        mpu_url_dicts = (
            client.get_mpu_urls(
                dataset_id=dataset_id,
                paths=mpu_targets,
                src_path=src_path.name if expand else None,
            )
            if len(mpu_targets) > 0
            else []
        )

        client.upload_files(dataset_id, spu_url_dicts, mpu_url_dicts, src_path, expand)

        files = [
            get_file_info(url_info["path"], src_path, expand)
            for url_info in spu_url_dicts
        ]
        files.extend(
            [
                get_file_info(url_info["path"], src_path, expand)
                for url_info in mpu_url_dicts
            ]
        )
        dataset = client.update_dataset(
            dataset_id, files=files, metadata=metadata, active=True
        )
    except Exception as exc:
        client.delete_dataset(dataset_id)
        raise exc

    dataset["vendor"] = storage_type_map_inv[dataset["vendor"]].value

    typer.secho(
        f"Objects are uploaded to dataset ({name}) successfully!",
        fg=typer.colors.BLUE,
    )
    panel_formatter.render([dataset], show_detail=True)
    tree_formatter.render(dataset["files"])
    json_formatter.render(dataset["metadata"])


@app.command()
def edit(
    name: str = typer.Argument(..., help="The name of dataset to update."),
    new_name: Optional[str] = typer.Option(
        None, "--name", "-n", help="The new name of dataset."
    ),
    metadata_file: Optional[typer.FileText] = typer.Option(
        None,
        "--metadata-file",
        "-f",
        help="Path to file containing the metadata describing your dataset. "
        "The metadata should be written in YAML format.",
    ),
):
    """Edit metadata of dataset."""
    project_client: ProjectDataClientService = build_client(ServiceType.PROJECT_DATA)
    dataset_id = project_client.get_id_by_name(name)
    if dataset_id is None:
        secho_error_and_exit(f"Dataset with name ({name}) is not found.")

    metadata = None
    if metadata_file is not None:
        try:
            metadata = yaml.safe_load(metadata_file)
            metadata = metadata or {}
        except yaml.YAMLError as exc:
            secho_error_and_exit(f"Error occurred while parsing metadata file... {exc}")

    client: DataClientService = build_client(ServiceType.DATA)
    dataset = client.update_dataset(
        dataset_id, name=new_name, metadata=metadata, active=True
    )
    dataset["vendor"] = storage_type_map_inv[dataset["vendor"]].value

    typer.secho("Dataset is updated successfully!", fg=typer.colors.BLUE)
    panel_formatter.render([dataset], show_detail=True)
    tree_formatter.render(dataset["files"])
    json_formatter.render(dataset["metadata"])


@app.command()
def delete(
    name: str = typer.Argument(
        ...,
        help="Name of dataset to delete.",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Forcefully delete dataset without confirmation prompt.",
    ),
):
    """Delete dataset from dataset.
    If the dataset was linked from user's cloud storage by `pf dataset link` command, then the storage will not be
    deleted and only the DB record is deleted. If the dataset was uploaded by `pf dataset upload` command, the storage
    will be deleted along with the DB record.
    """
    if not force:
        do_delete = typer.confirm("Are your sure to delete dataset?")
        if not do_delete:
            raise typer.Abort()

    client: DataClientService = build_client(ServiceType.DATA)
    project_client: ProjectDataClientService = build_client(ServiceType.PROJECT_DATA)

    dataset_id = project_client.get_id_by_name(name)
    if dataset_id is None:
        secho_error_and_exit(f"Dataset ({name}) is not found.")

    client.delete_dataset(dataset_id)

    typer.secho(f"Dataset ({name}) deleted successfully!", fg=typer.colors.BLUE)
