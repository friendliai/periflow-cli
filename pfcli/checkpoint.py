# Copyright (C) 2021 FriendliAI

"""CLI for Checkpoint"""

import os
from click import secho
from dateutil.parser import parse
from typing import Optional, List
from uuid import UUID

import typer

from pfcli.service import (
    CheckpointCategory,
    CredType,
    ModelFormCategory,
    ServiceType,
    StorageType,
    cred_type_map,
    cred_type_map_inv,
    storage_type_map_inv,
)
from pfcli.service.client import (
    CheckpointClientService,
    CredentialClientService,
    GroupProjectCheckpointClientService,
    build_client,
)
from pfcli.service.cloud import build_storage_helper
from pfcli.service.formatter import PanelFormatter, TableFormatter, TreeFormatter
from pfcli.utils.format import datetime_to_pretty_str, secho_error_and_exit
from pfcli.utils.fs import download_file
from pfcli.utils.validate import validate_cloud_storage_type, validate_parallelism_order


app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False,
)

table_formatter = TableFormatter(
    name="Checkpoints",
    fields=[
        "id",
        "name",
        "model_category",
        "forms[0].vendor",
        "iteration",
        "created_at",
    ],
    headers=[
        "ID",
        "Name",
        "Category",
        "Cloud",
        "Iteration",
        "Created At",
    ],
)
panel_formatter = PanelFormatter(
    name="Overview",
    fields=[
        "id",
        "name",
        "model_category",
        "forms[0].vendor",
        "forms[0].storage_name",
        "iteration",
        "forms[0].form_category",
        "created_at",
    ],
    headers=[
        "ID",
        "Name",
        "Category",
        "Cloud",
        "Storage Name",
        "Iteration",
        "Format",
        "Created At",
    ],
)
tree_formatter = TreeFormatter(name="Files")

model_info_panel = PanelFormatter(
    name="Model Info",
    fields=[
        "head_size",
        "num_heads",
        "num_layers",
        "max_length",
        "vocab_size",
    ],
    headers=[
        "Head Size",
        "#Heads",
        "#Layers",
        "Max Length",
        "Vocab Size",
    ]
)

@app.command()
def list(
    category: Optional[CheckpointCategory] = typer.Option(
        None,
        "--category",
        "-c",
        help="Category of checkpoints. One of 'user_provided' and 'job_generated'.",
    )
):
    """List all checkpoints that belong to the user's organization."""
    client: GroupProjectCheckpointClientService = build_client(
        ServiceType.GROUP_PROJECT_CHECKPOINT
    )
    checkpoints = client.list_checkpoints(category)
    for ckpt in checkpoints:
        for form in ckpt["forms"]:
            form["vendor"] = storage_type_map_inv[form["vendor"]].value
        ckpt["created_at"] = datetime_to_pretty_str(parse(ckpt["created_at"]))

    table_formatter.render(checkpoints)


@app.command()
def view(
    checkpoint_id: UUID = typer.Argument(
        ..., help="UUID of checkpoint to inspect detail."
    )
):
    """Show details of a checkpoint."""
    client: CheckpointClientService = build_client(ServiceType.CHECKPOINT)
    ckpt = client.get_checkpoint(checkpoint_id)
    for form in ckpt["forms"]:
        form["vendor"] = storage_type_map_inv[form["vendor"]].value
    ckpt["created_at"] = datetime_to_pretty_str(parse(ckpt["created_at"]))

    panel_formatter.render([ckpt])

    # serving model info.
    if "attributes" in ckpt and "head_size" in ckpt["attributes"]:
        model_info_panel.render(ckpt["attributes"])

    tree_formatter.render(ckpt["forms"][0]["files"])


@app.command()
def create(
    name: str = typer.Option(
        ..., "--name", "-n", help="Name of your checkpoint to create."
    ),
    format: ModelFormCategory = typer.Option(
        ModelFormCategory.ETC,
        "-m",
        "--format",
        help="The format of your checkpoint",
    ),
    cloud_storage: StorageType = typer.Option(
        ...,
        "--cloud-storage",
        "-c",
        help="The cloud storage vendor where the checkpoint is uploaded.",
        callback=validate_cloud_storage_type,
    ),
    region: str = typer.Option(
        ...,
        "--region",
        "-r",
        help="The cloud storage region where the checkpoint is uploaded.",
    ),
    storage_name: str = typer.Option(
        ...,
        "--storage-name",
        "-s",
        help="The name cloud storage where the checkpoint is uploaded.",
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
        help="UUID of crendential to access cloud storage.",
    ),
    iteration: int = typer.Option(
        ..., "--iteration", help="The iteration number of the checkpoint."
    ),
    # advanced arguments
    dp_degree: int = typer.Option(
        1, "--dp-degree", help="Data parallelism degree of the model checkpoint."
    ),
    pp_degree: int = typer.Option(
        1,
        "--pp-degree",
        help="Pipelined model parallelism degree of the model checkpoint.",
    ),
    mp_degree: int = typer.Option(
        1, "--mp-degree", help="Tensor parallelism degree of the model checkpoint."
    ),
    parallelism_order: str = typer.Option(
        "pp,dp,mp",
        "--parallelism-order",
        callback=validate_parallelism_order,
        help="Order of device allocation in distributed training.",
    ),
):
    """Create a checkpoint object by registering user's cloud storage to PeriFlow."""
    dist_config = {
        "pp_degree": pp_degree,
        "dp_degree": dp_degree,
        "mp_degree": mp_degree,
        "dp_mode": "allreduce",
        "parallelism_order": parallelism_order,
    }

    credential_client: CredentialClientService = build_client(ServiceType.CREDENTIAL)
    credential = credential_client.get_credential(credential_id)
    if credential["type"] != cred_type_map[CredType(cloud_storage.value)]:
        secho_error_and_exit(
            "Credential type and cloud vendor mismatch: "
            f"{cred_type_map_inv[credential['type']]} and {cloud_storage.value}."
        )

    storage_helper = build_storage_helper(
        cloud_storage, credential_json=credential["value"]
    )
    if storage_path is not None:
        storage_path = storage_path.strip("/")
    files = storage_helper.list_storage_files(storage_name, storage_path)
    if storage_path is not None:
        storage_name = f"{storage_name}/{storage_path}"

    checkpoint_client: GroupProjectCheckpointClientService = build_client(
        ServiceType.GROUP_PROJECT_CHECKPOINT
    )
    ckpt = checkpoint_client.create_checkpoint(
        name=name,
        model_form_category=format,
        vendor=cloud_storage,
        region=region,
        credential_id=credential_id,
        iteration=iteration,
        storage_name=storage_name,
        files=files,
        dist_config=dist_config,
        data_config={},
        job_setting_config=None,  # TODO: make configurable
    )
    for form in ckpt["forms"]:
        form["vendor"] = storage_type_map_inv[form["vendor"]].value
    ckpt["created_at"] = datetime_to_pretty_str(parse(ckpt["created_at"]))

    panel_formatter.render([ckpt])
    tree_formatter.render(ckpt["files"])


@app.command()
def delete(
    checkpoint_id: UUID = typer.Argument(..., help="UUID of checkpoint to delete."),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Forcefully delete checkpoint without confirmation prompt.",
    ),
):
    """Delete the existing checkpoint."""
    if not force:
        do_delete = typer.confirm("Are you sure to delete checkpoint?")
        if not do_delete:
            raise typer.Abort()

    client: CheckpointClientService = build_client(ServiceType.CHECKPOINT)
    client.delete_checkpoint(checkpoint_id)

    typer.secho("Checkpoint is deleted successfully!", fg=typer.colors.BLUE)


@app.command()
def download(
    checkpoint_id: UUID = typer.Argument(..., help="UUID of checkpoint to download."),
    save_directory: Optional[str] = typer.Option(
        None,
        "--destination",
        "-d",
        help="Destination path to directory to save checkpoint files.",
    ),
):
    """Download checkpoint files to local storage."""
    if save_directory is not None and not os.path.isdir(save_directory):
        secho_error_and_exit(f"Directory {save_directory} is not found.")

    save_directory = save_directory or os.getcwd()

    client: CheckpointClientService = build_client(ServiceType.CHECKPOINT)
    files = client.get_checkpoint_download_urls(checkpoint_id)

    for i, file in enumerate(files):
        typer.secho(f"Downloading files {i + 1}/{len(files)}...")
        download_file(
            file["download_url"], out=os.path.join(save_directory, file["name"])
        )
