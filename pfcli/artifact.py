# Copyright (C) 2022 FriendliAI

"""CLI for artifact"""

import os
from pathlib import Path
from typing import Optional

import typer

from pfcli.service import ServiceType
from pfcli.service.client import build_client
from pfcli.service.client.job import JobArtifactClientService
from pfcli.utils import download_file, secho_error_and_exit


app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False
)


@app.command()
def download(
    job_id: int = typer.Argument(
        ...,
        help="ID of a job to download artifact"
    ),
    save_path: Optional[Path] = typer.Option(
        None,
        '--destination',
        '-d',
        help='Destination path to save artifact zip file.'
    )
):
    """download artifact
    """
    if save_path is not None and save_path.exists():
        secho_error_and_exit(f"{save_path.name} already exist!")

    if save_path is None:
        save_path = Path(os.getcwd()) / f"{job_id}-artifacts.zip"

    client: JobArtifactClientService = build_client(ServiceType.JOB_ARTIFACT, job_id=job_id)
    response = client.get_artifact_download_urls()
    url = response["url"]
    download_file(url, out=str(save_path))
