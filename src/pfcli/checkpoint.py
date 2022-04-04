# Copyright (C) 2021 FriendliAI

"""CLI for Checkpoint"""

from pathlib import Path
from typing import Optional, List

import typer

from pfcli.service import CheckpointCategory, ServiceType, StorageType
from pfcli.service.client import (
    CheckpointClientService,
    CredentialClientService,
    GroupCheckpointClinetService,
    build_client,
)
from pfcli.service.cloud import CloudStorageHelper
from pfcli.service.formatter import TableFormatter
from pfcli.utils import secho_error_and_exit


app = typer.Typer()

ckpt_formatter = TableFormatter(
    fields=['id', 'category', 'vendor', 'storage_name', 'iteration', 'created_at'],
    headers=['id', 'category', 'cloud', 'storage name', 'iteration', 'created at']
)
file_formatter = TableFormatter(
    fields=['name', 'path', 'mtime', 'size'],
    headers=['name', 'path', 'mtime', 'size']
)


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

    typer.echo(ckpt_formatter.render(checkpoints))


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

    typer.echo(ckpt_formatter.render([info], in_list=True))
    typer.echo("\nFILES\n")
    typer.echo(file_formatter.render(info['files']))


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
    cloud: StorageType = typer.Option(
        ...,
        "--cloud",
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
    if credential["type"] != cloud:
        secho_error_and_exit(f"Credential type and cloud vendor mismatch: {credential['type']} and {cloud}")

    storage_helper = CloudStorageHelper(file_or_dir, credential["value"], cloud, storage_name)
    files = storage_helper.get_checkpoint_file_list()

    checkpoint_client: GroupCheckpointClinetService = build_client(ServiceType.GROUP_CHECKPOINT)
    info = checkpoint_client.create_checkpoint(
        vendor=cloud,
        iteration=iteration,
        storage_name=storage_name,
        dist_config=dist_config,
        credential_id=credential_id,
        files=files
    )

    typer.echo(ckpt_formatter.render([info], in_list=True))
    typer.echo("\nFILES\n")
    typer.echo(file_formatter.render(info['files']))


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
    cloud: Optional[StorageType] = typer.Option(
        ...,
        "--cloud",
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
        cloud = cloud or prev_info['vendor']
        if credential["type"] != cloud:
            secho_error_and_exit(f"Credential type and cloud vendor mismatch: {credential['type']} and {cloud}")

        if file_or_dir is not None:
            storage_name = storage_name or prev_info['storage_name']
            storage_helper = CloudStorageHelper(file_or_dir, credential["value"], cloud, storage_name)
            files = storage_helper.get_checkpoint_file_list()

    info = checkpoint_client.update_checkpoint(
        checkpoint_id,
        vendor=cloud,
        iteration=iteration,
        storage_name=storage_name,
        dist_config=dist_config,
        credential_id=credential_id,
        files=files
    )

    typer.echo(ckpt_formatter.render([info], in_list=True))
    typer.echo("\nFILES\n")
    typer.echo(file_formatter.render(info['files']))


@app.command("delete")
def checkpoint_delete(
    checkpoint_id: str = typer.Option(
        ...,
        '--checkpoint-id',
        '-d',
        help="UUID of checkpoint to delete."
    ),
    force: bool = typer.Option(
        False,
        '--force',
        '-f',
        help="Forcefully delete checkpoint without confirmation prompt."
    )
):
    """Delete the existing checkpoint.
    """
    if not force:
        do_delete = typer.confirm("Are you sure to delete checkpoint?")
        if not do_delete:
            raise typer.Abort()

    client: CheckpointClientService = build_client(ServiceType.CHECKPOINT)
    client.delete_checkpoint(checkpoint_id)

    typer.secho("Checkpoint is deleted successfully!", fg=typer.colors.BLUE)


if __name__ == '__main__':
    app()
