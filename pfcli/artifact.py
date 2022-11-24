# Copyright (C) 2022 FriendliAI

"""CLI for artifact"""

import os
from pathlib import Path
from typing import Optional

import typer

from pfcli.service import ServiceType
from pfcli.service.client import build_client
from pfcli.service.client.job import ProjectJobArtifactClientService
from pfcli.utils.fs import download_file
from pfcli.utils.format import secho_error_and_exit


app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False,
)


@app.command()
def download(
    job_id: int = typer.Argument(..., help="ID of a job to download artifact"),
    save_directory: Optional[Path] = typer.Option(
        None, "--destination", "-d", help="Destination path to save artifact files."
    ),
):
    """download artifact"""
    if save_directory is not None and not save_directory.is_dir():
        secho_error_and_exit(
            f"{save_directory.name} already exist, but not a directory!"
        )

    save_directory = save_directory or Path(os.getcwd())

    client: ProjectJobArtifactClientService = build_client(
        ServiceType.PROJECT_JOB_ARTIFACT, job_id=job_id
    )
    all_artifacts = client.list_artifacts()
    for i, artifact in enumerate(all_artifacts):
        artifact_id = artifact["id"]
        response = client.get_artifact_download_url(artifact_id)
        url = response["url"]
        name = artifact["name"]
        typer.secho(f"Downloading files {i + 1}/{len(all_artifacts)}...")
        download_file(url, out=str(save_directory / name))
