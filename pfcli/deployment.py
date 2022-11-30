# Copyright (C) 2021 FriendliAI

"""CLI for Deployment"""

from dateutil.parser import parse
from typing import Optional

import ruamel.yaml
import typer
import yaml
from uuid import UUID
import datetime

from pfcli.service import (
    DeploymentType,
    ServiceType,
    GpuType,
    CloudType,
    EngineType,
)
from pfcli.service.client import (
    DeploymentClientService,
    build_client,
)
from pfcli.context import get_current_project_id
from pfcli.service.client.deployment import (
    DeploymentMetricsClientService,
    PFSProjectUsageClientService,
)
from pfcli.service.config import build_deployment_configurator
from pfcli.service.formatter import PanelFormatter, TableFormatter
from pfcli.utils.format import (
    datetime_to_pretty_str,
    secho_error_and_exit,
)
from pfcli.utils.prompt import get_default_editor, open_editor


app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False,
)
template_app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False,
)

app.add_typer(template_app, name="template", help="Manage deployment templates.")

deployment_panel = PanelFormatter(
    name="Deployment Overview",
    fields=[
        "id",
        "config.name",
        "config.deployment_type",
        "status",
        "vms",
        "config.gpu_type",
        "config.total_gpus",
        "start",
        "endpoint",
    ],
    headers=[
        "ID",
        "Name",
        "Type",
        "Status",
        "VM Type",
        "GPU Type",
        "#GPUs",
        "Start",
        "Endpoint",
    ],
    extra_fields=["error"],
    extra_headers=["error"],
)

deployment_table = TableFormatter(
    name="Deployments",
    fields=[
        "id",
        "config.name",
        "status",
        "vms",
        "config.gpu_type",
        "config.total_gpus",
        "start",
    ],
    headers=["ID", "Name", "Status", "VM Type", "GPU Type", "#GPUs", "Start"],
    extra_fields=["error"],
    extra_headers=["error"],
)

deployment_metrics_table = TableFormatter(
    name="Deployment Metrics",
    fields=[
        "id",
        "latency",
        "throughput",
        "time_window",
    ],
    headers=["ID", "Latency(ms)", "Throughput(req/s)", "Time Window(sec)"],
    extra_fields=["error"],
    extra_headers=["error"],
)

deployment_usage_table = TableFormatter(
    name="Deployment Usage",
    fields=[
        "id",
        "type",
        "duration",
    ],
    headers=["ID", "Type", "Total Usage (days, HH:MM:SS)"],
)

deployment_panel.add_substitution_rule("waiting", "[bold]waiting")
deployment_panel.add_substitution_rule("enqueued", "[bold cyan]enqueued")
deployment_panel.add_substitution_rule("running", "[bold blue]running")
deployment_panel.add_substitution_rule("success", "[bold green]success")
deployment_panel.add_substitution_rule("failed", "[bold red]failed")
deployment_panel.add_substitution_rule("terminated", "[bold yellow]terminated")
deployment_panel.add_substitution_rule("terminating", "[bold magenta]terminating")
deployment_panel.add_substitution_rule("cancelling", "[bold magenta]cancelling")

deployment_table.add_substitution_rule("waiting", "[bold]waiting")
deployment_table.add_substitution_rule("enqueued", "[bold cyan]enqueued")
deployment_table.add_substitution_rule("running", "[bold blue]running")
deployment_table.add_substitution_rule("success", "[bold green]success")
deployment_table.add_substitution_rule("failed", "[bold red]failed")
deployment_table.add_substitution_rule("terminated", "[bold yellow]terminated")
deployment_table.add_substitution_rule("terminating", "[bold magenta]terminating")
deployment_table.add_substitution_rule("cancelling", "[bold magenta]cancelling")

deployment_metrics_table.add_substitution_rule("waiting", "[bold]waiting")
deployment_metrics_table.add_substitution_rule("enqueued", "[bold cyan]enqueued")
deployment_metrics_table.add_substitution_rule("running", "[bold blue]running")
deployment_metrics_table.add_substitution_rule("success", "[bold green]success")
deployment_metrics_table.add_substitution_rule("failed", "[bold red]failed")
deployment_metrics_table.add_substitution_rule("terminated", "[bold yellow]terminated")
deployment_metrics_table.add_substitution_rule(
    "terminating", "[bold magenta]terminating"
)
deployment_metrics_table.add_substitution_rule("cancelling", "[bold magenta]cancelling")


@app.command()
def list(
    tail: Optional[int] = typer.Option(
        None, "--tail", help="The number of deployment list to view at the tail"
    ),
    head: Optional[int] = typer.Option(
        None, "--head", help="The number of deployment list to view at the head"
    ),
):
    """List all deployments."""
    project_id = get_current_project_id()
    if project_id is None:
        secho_error_and_exit("Failed to identify project... Please set project again.")

    client: DeploymentClientService = build_client(ServiceType.DEPLOYMENT)
    deployments = client.list_deployments(str(project_id))["deployments"]

    for deployment in deployments:
        started_at = deployment.get("start")
        if started_at is not None:
            start = datetime_to_pretty_str(parse(started_at))
        else:
            start = None
        deployment["start"] = start
        deployment["vms"] = (
            deployment["vms"][0]["name"] if deployment["vms"] else "None"
        )

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
def delete(deployment_id: str = typer.Argument(..., help="ID of deployment to delete")):
    """Delete deployment."""
    client: DeploymentClientService = build_client(ServiceType.DEPLOYMENT)
    client.delete_deployment(deployment_id)
    typer.secho(
        f"Deleted Deployment ({deployment_id}) successfully.",
        fg=typer.colors.BLUE,
    )


@app.command()
def view(
    deployment_id: str = typer.Argument(..., help="deployment id to inspect detail.")
):
    """Show details of a deployment."""
    client: DeploymentClientService = build_client(ServiceType.DEPLOYMENT)
    deployment = client.get_deployment(deployment_id)

    started_at = deployment.get("start")
    if started_at is not None:
        start = datetime_to_pretty_str(parse(started_at))
    else:
        start = None
    deployment["start"] = start
    deployment["vms"] = deployment["vms"][0]["name"] if deployment["vms"] else "None"
    deployment_panel.render([deployment])


@app.command()
def metrics(
    deployment_id: str = typer.Argument(..., help="Deployment id to inspect detail."),
    time_window: int = typer.Option(
        60, "--time-window", "-t", help="Time window of metrics in seconds."
    ),
):
    """Show metrics of a deployment."""
    metrics_client: DeploymentMetricsClientService = build_client(
        ServiceType.DEPLOYMENT_METRICS, deployment_id=deployment_id
    )
    metrics = metrics_client.get_metrics(
        deployment_id=deployment_id, time_window=time_window
    )
    metrics["id"] = metrics["deployment_id"]
    # ns => ms
    metrics["latency"] = "{:.3f}".format(metrics["latency"] / 1000000) if "latency" in metrics else None
    metrics["throughput"] = "{:.3f}".format(metrics["throughput"]) if "throughput" in metrics else None
    deployment_metrics_table.render([metrics])


@app.command()
def usage():
    """Show total usage of deployments in project."""
    client: PFSProjectUsageClientService = build_client(ServiceType.PFS_PROJECT_USAGE)
    usages = client.get_deployment_usage()

    deployments = [
        {"id": id, "type": info["deployment_type"] , "duration": datetime.timedelta(seconds=int(info["duration"]))}
        for id, info in usages.items()
    ]
    deployment_usage_table.render(deployments)

@app.command()
def create(
    checkpoint_id: str = typer.Option(
        ..., "--checkpoint-id", "-id", help="Checkpoint id to deploy."
    ),
    deployment_name: str = typer.Option(
        ..., "--name", "-n", help="The name of deployment. "
    ),
    deployment_type: DeploymentType = typer.Option(
        ..., "--type", "-t", help="Type of deployment(dev/prod)."
    ),
    gpu_type: GpuType = typer.Option(
        ..., "--gpu-type", "-g", help="The GPU type where the deployment is deployed."
    ),
    cloud: CloudType = typer.Option(
        ..., "--cloud", "-c", help="Type of cloud(aws, azure, gcp)."
    ),
    region: str = typer.Option(..., "--region", "-r", help="Region of cloud."),
    engine: Optional[EngineType] = typer.Option(
        EngineType.ORCA, "--engine", "-e", help="Type of engine(orca or triton)."
    ),
    config_file: typer.FileText = typer.Option(
        ..., "--config-file", "-f", help="Path to configuration file."
    ),
):
    """Create a deployment object by using model checkpoint."""
    project_id = get_current_project_id()
    if project_id is None:
        secho_error_and_exit("Failed to identify project... Please set project again.")

    if engine is not EngineType.ORCA:
        secho_error_and_exit("Only ORCA is supported!")

    try:
        UUID(checkpoint_id)
    except ValueError:
        secho_error_and_exit("Checkpoint ID should be in UUID format.")

    try:
        config: dict = yaml.safe_load(config_file)
    except yaml.YAMLError as e:
        secho_error_and_exit(f"Error occurred while parsing engine config file... {e}")

    request_data = {
        "project_id": str(project_id),
        "model_id": checkpoint_id,
        "deployment_type": deployment_type,
        "name": deployment_name,
        "gpu_type": gpu_type,
        "cloud": cloud,
        "region": region,
        **config,
    }
    client: DeploymentClientService = build_client(ServiceType.DEPLOYMENT)
    deployment = client.create_deployment(request_data)

    typer.secho(
        f"Deployment ({deployment['id']}) started successfully. Use 'pf deployment view {deployment['id']}' to see the deployment details.\n"
        f"Send inference requests to '{deployment['endpoint']}'.",
        fg=typer.colors.BLUE,
    )


@template_app.command("create")
def template_create(
    save_path: typer.FileTextWrite = typer.Option(
        ..., "--save-path", "-s", help="Path to save job YAML configruation file."
    )
):
    """Create a deployment engine configuration YAML file."""
    configurator = build_deployment_configurator(EngineType.ORCA)
    yaml_str = configurator.render()

    yaml = ruamel.yaml.YAML()
    code = yaml.load(yaml_str)
    yaml.dump(code, save_path)

    continue_edit = typer.confirm(
        f"Do you want to open an editor to configure the job YAML file? (default editor: {get_default_editor()})",
        prompt_suffix="\n>> ",
    )
    if continue_edit:
        open_editor(save_path.name)
