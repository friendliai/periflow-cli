# Copyright (C) 2021 FriendliAI

"""CLI for Checkpoint"""

from pathlib import Path
from typing import Optional, List

import tabulate
import typer

from pfcli.service import CheckpointCategory, ServiceType, VendorType
from pfcli.service.client import (
    CheckpointClientService,
    CredentialClientService,
    GroupCheckpointClinetService,
    build_client,
)
from pfcli.service.cloud import CloudStorageHelper
from pfcli.utils import secho_error_and_exit

app = typer.Typer()


def _echo_checkpoint_detail(checkpoint_json: dict):
    typer.echo(f"id: {checkpoint_json['id']}")
    typer.echo(f"category: {checkpoint_json['category']}")
    typer.echo(f"vendor: {checkpoint_json['vendor']}")
    typer.echo(f"iteration: {checkpoint_json['iteration']}")
    typer.echo(f"created_at: {checkpoint_json['created_at']}")
    typer.echo("files:")
    headers = ["name", "path", "mtime", "size"]
    results = []
    for file in checkpoint_json['files']:
        results.append([file[header] for header in headers])
    headers[2] = "modified time"
    typer.echo(tabulate.tabulate(results, headers=headers))


def _validate_parallelism_order(value: str) -> List[str]:
    parallelism_order = value.split(",")
    if {"pp", "dp", "mp"} != set(parallelism_order):
        secho_error_and_exit("Invalid Argument: parallelism_order should contain 'pp', 'dp', 'mp'")
    return parallelism_order


@app.command("list")
def checkpoint_list(
    category: Optional[CheckpointCategory] = typer.Option(
        None,
        "--category",
        "-c",
        help="Category of checkpoints. One of 'user_provided' and 'job_generated'."
    )
):
    """List all checkpoints that belong to the user's group
    """
    client: GroupCheckpointClinetService = build_client(ServiceType.GROUP_CHECKPOINT)
    checkpoints = client.list_checkpoints(category)

    headers = ["id", "category", "vendor", "storage_name", "iteration", "created_at"]
    results = []
    for checkpoint in checkpoints:
        results.append([checkpoint[header] for header in headers])

    typer.echo(tabulate.tabulate(results, headers=headers))


@app.command("view")
def checkpoint_detail(
    checkpoint_id: str = typer.Option(
        ...,
        "--checkpoint-id",
        "-i",
        help="UUID of checkpoint to inspect detail."
    )
):
    """Show details of the given checkpoint_id
    """
    client: CheckpointClientService = build_client(ServiceType.CHECKPOINT)
    info = client.get_checkpoint(checkpoint_id)
    _echo_checkpoint_detail(info)


@app.command("create")
def checkpoint_create(
    file_or_dir: Path = typer.Option(
        ...,
        "--file-or-dir",
        "-f",
        help="File or direcotry path of cloud storage."
    ),
    iteration: int = typer.Option(
        ...,
        "--iteration",
        "-i",
        help="The iteration number of the checkpoint."
    ),
    vendor: VendorType = typer.Option(
        ...,
        "--vendor",
        "-v",
        help="The cloud storage vendor type where the checkpoint is uploaded."
    ),
    storage_name: str = typer.Option(
        ...,
        "--storage-name",
        "-s",
        help="The name cloud storage where the checkpoint is uploaded."
    ),
    credential_id: str = typer.Option(
        ...,
        "--credential-id",
        "-c",
        help="UUID of crendential to access cloud storage."
    ),
    # advanced arguments
    pp_degree: int = typer.Option(
        1,
        "--pp-degree",
        "-pp",
        help="Pipelined model parallelism degree of the model checkpoint."
    ),
    dp_degree: int = typer.Option(
        1,
        "--dp-degree",
        "-dp",
        help="Data parallelism degree of the model checkpoint."
    ),
    mp_degree: int = typer.Option(
        1,
        "--mp-degree",
        "-mp",
        help="Tensor parallelism degree of the model checkpoint."
    ),
    parallelism_order: str = typer.Option(
        'pp,dp,mp',
        '--parallelism-order',
        '-po',
        callback=_validate_parallelism_order,
        help="Order of device allocation in distributed training."
    )
):
    """Create the checkpoint.
    """
    dist_config = {
        "pp_degree": pp_degree,
        "dp_degree": dp_degree,
        "mp_degree": mp_degree,
        "dp_mode": "allreduce",
        "parallelism_order": parallelism_order
    }

    credential_client: CredentialClientService = build_client(ServiceType.CREDENTIAL)
    credential = credential_client.get_credential(credential_id)
    if credential["type"] != vendor:
        secho_error_and_exit(f"Credential type and vendor mismatch: {credential['type']} and {vendor}")

    storage_helper = CloudStorageHelper(file_or_dir, credential["value"], vendor, storage_name)
    files = storage_helper.get_checkpoint_file_list()

    checkpoint_client: GroupCheckpointClinetService = build_client(ServiceType.GROUP_CHECKPOINT)
    info = checkpoint_client.create_checkpoint(
        vendor=vendor,
        iteration=iteration,
        storage_name=storage_name,
        dist_config=dist_config,
        credential_id=credential_id,
        files=files
    )
    _echo_checkpoint_detail(info)


@app.command("update")
def checkpoint_update(
    checkpoint_id: str = typer.Option(
        ...,
        "--checkpoint-id",
        help="UUID of checkpoint to update."
    ),
    file_or_dir: Optional[Path] = typer.Option(
        ...,
        "--file-or-dir",
        "-f",
        help="File or direcotry path of cloud storage."
    ),
    iteration: Optional[int] = typer.Option(
        ...,
        "--iteration",
        "-i",
        help="The iteration number of the checkpoint."
    ),
    vendor: Optional[VendorType] = typer.Option(
        ...,
        "--vendor",
        "-v",
        help="The cloud storage vendor type where the checkpoint is uploaded."
    ),
    storage_name: Optional[str] = typer.Option(
        ...,
        "--storage-name",
        "-s",
        help="The name cloud storage where the checkpoint is uploaded."
    ),
    credential_id: Optional[str] = typer.Option(
        ...,
        "--credential-id",
        "-c",
        help="UUID of crendential to access cloud storage."
    ),
    # advanced arguments
    pp_degree: Optional[int] = typer.Option(
        1,
        "--pp-degree",
        "-pp",
        help="Pipelined model parallelism degree of the model checkpoint."
    ),
    dp_degree: Optional[int] = typer.Option(
        1,
        "--dp-degree",
        "-dp",
        help="Data parallelism degree of the model checkpoint."
    ),
    mp_degree: Optional[int] = typer.Option(
        1,
        "--mp-degree",
        "-mp",
        help="Tensor parallelism degree of the model checkpoint."
    ),
    parallelism_order: Optional[str] = typer.Option(
        'pp,dp,mp',
        '--parallelism-order',
        '-po',
        callback=_validate_parallelism_order,
        help="Order of device allocation in distributed training."
    )
):
    """Update the existing checkpoint.
    """
    dist_config = {
        "pp_degree": pp_degree,
        "dp_degree": dp_degree,
        "mp_degree": mp_degree,
        "dp_mode": "allreduce",
        "parallelism_order": parallelism_order
    }

    checkpoint_client: CheckpointClientService = build_client(ServiceType.CHECKPOINT)
    prev_info = checkpoint_client.get_checkpoint(checkpoint_id)

    if credential_id is not None:
        credential_client: CredentialClientService = build_client(ServiceType.CREDENTIAL)
        credential = credential_client.get_credential(credential_id)
        vendor = vendor or prev_info['vendor']
        if credential["type"] != vendor:
            secho_error_and_exit(f"Credential type and vendor mismatch: {credential['type']} and {vendor}")

        if file_or_dir is not None:
            storage_name = storage_name or prev_info['storage_name']
            storage_helper = CloudStorageHelper(file_or_dir, credential["value"], vendor, storage_name)
            files = storage_helper.get_checkpoint_file_list()

    info = checkpoint_client.update_checkpoint(
        checkpoint_id,
        vendor=vendor,
        iteration=iteration,
        storage_name=storage_name,
        dist_config=dist_config,
        credential_id=credential_id,
        files=files
    )
    _echo_checkpoint_detail(info)


@app.command("delete")
def checkpoint_delete(
    checkpoint_id: str = typer.Option(...)
):
    """Delete the existing checkpoint.
    """
    client: CheckpointClientService = build_client(ServiceType.CHECKPOINT)
    client.delete_checkpoint(checkpoint_id)


if __name__ == '__main__':
    app()
