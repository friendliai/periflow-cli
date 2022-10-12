# Copyright (C) 2021 FriendliAI

"""CLI for Deployment"""

from dateutil.parser import parse
from typing import Optional

import typer

from pfcli.service import (
    ServiceType,
    GpuType,
)
from pfcli.service.client import (
    DeploymentClientService,
    build_client,
)
from pfcli.service.formatter import PanelFormatter, TableFormatter
from pfcli.utils import datetime_to_pretty_str


app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False,
)

deployment_panel = PanelFormatter(
    name="Overview",
    fields=["id", "name", "status", "vm", "gpu_type", "num_gpus", "start", "endpoint"],
    headers=["ID", "Name", "Status", "VM", "Device", "Device Cnt", "Start", "Endpoint"],
    extra_fields=["error"],
    extra_headers=["error"],
)

deployment_table = TableFormatter(
    name="Deployments",
    fields=[
        "id",
        "name",
        "status",
        "vm",
        "gpu_type",
        "num_gpus",
        "start",
    ],
    headers=["ID", "Name", "Status", "VM", "Device", "Device Cnt", "Start"],
    extra_fields=["error"],
    extra_headers=["error"],
)

deployment_panel.add_substitution_rule("waiting", "[bold]waiting")
deployment_panel.add_substitution_rule("enqueued", "[bold cyan]enqueued")
deployment_panel.add_substitution_rule("running", "[bold blue]running")
deployment_panel.add_substitution_rule("success", "[bold green]success")
deployment_panel.add_substitution_rule("failed", "[bold red]failed")
deployment_panel.add_substitution_rule("terminated", "[bold yellow]terminated")
deployment_panel.add_substitution_rule("terminating", "[bold magenta]terminating")
deployment_panel.add_substitution_rule("cancelling", "[bold magenta]cancelling")

deployment_panel.add_substitution_rule("waiting", "[bold]waiting")
deployment_panel.add_substitution_rule("enqueued", "[bold cyan]enqueued")
deployment_panel.add_substitution_rule("running", "[bold blue]running")
deployment_panel.add_substitution_rule("success", "[bold green]success")
deployment_panel.add_substitution_rule("failed", "[bold red]failed")
deployment_panel.add_substitution_rule("terminated", "[bold yellow]terminated")
deployment_panel.add_substitution_rule("terminating", "[bold magenta]terminating")
deployment_panel.add_substitution_rule("cancelling", "[bold magenta]cancelling")


@app.command()
def list(
    tail: Optional[int] = typer.Option(
        None, "--tail", help="The number of deployment list to view at the tail"
    ),
    head: Optional[int] = typer.Option(
        None, "--head", help="The number of deployment list to view at the head"
    ),
):
    """List all deployments,"""
    client: DeploymentClientService = build_client(ServiceType.DEPLOYMENT)
    deployments = client.list_deployments()

    for deployment in deployments:
        started_at = deployment.get("start")
        if started_at is not None:
            start = datetime_to_pretty_str(parse(started_at))
        else:
            start = None
        deployment["start"] = start

    if tail is not None or head is not None:
        target_deployment_list = []
        if tail:
            target_deployment_list.extend(deployments[:tail])
        if head:
            target_deployment_list.extend(deployments[-head:])
    else:
        target_deployment_list = deployments

    deployment_table.render(target_deployment_list)


@app.command()
def stop(deployment_id: str = typer.Argument(..., help="ID of deployment to stop")):
    """Delete deployment."""
    client: DeploymentClientService = build_client(ServiceType.DEPLOYMENT)
    client.delete_deployment(deployment_id)


@app.command()
def view(deployment_id: str = typer.Argument(..., help="deployment id to inspect detail.")):
    """Show details of a deployment."""
    client: DeploymentClientService = build_client(ServiceType.DEPLOYMENT)
    deployment = client.get_deployment(deployment_id)

    started_at = deployment.get("start")
    if started_at is not None:
        start = datetime_to_pretty_str(parse(started_at))
    else:
        start = None
    deployment["start"] = start

    deployment_panel.render([deployment])


@app.command()
def create(
    checkpoint_id: str = typer.Option(
        ..., "--checkpoint-id", "-c", help="Checkpoint id to deploy."
    ),
    name: str = typer.Option(..., "--name", "-n", help="The name of deployment deployment."),
    gpu_type: GpuType = typer.Option(
        ..., "--gpu-type", "-g", help="The GPU type where the deployment is deployed."
    ),
    num_sessions: int = typer.Option(
        ..., "--num-sessions", "-s", help="The number of sessions of deployment deployment."
    ),
):
    """Create a deployment object by using model checkpoint."""
    request_data = {
        "name": name,
        "model_id": checkpoint_id,
        "gpu_type": gpu_type,
        "num_sessions": num_sessions,
    }
    client: DeploymentClientService = build_client(ServiceType.DEPLOYMENT)
    deployment = client.create_deployment(request_data)

    typer.secho(
        f"Deployment ({deployment['id']}) started successfully. Use 'pf serivce view <id>' to see the deployment details.\n"
        f"Run 'curl {deployment['endpoint']}' for inference request",
        fg=typer.colors.BLUE,
    )
