# Copyright (C) 2021 FriendliAI

"""CLI for Deployment"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from uuid import UUID

import ruamel.yaml
import typer
import yaml
from dateutil.parser import parse

from pfcli.context import get_current_project_id
from pfcli.service import (
    CloudType,
    DeploymentSecurityLevel,
    DeploymentType,
    EngineType,
    GpuType,
    ServiceType,
)
from pfcli.service.client import (
    DeploymentClientService,
    DeploymentEventClientService,
    DeploymentLogClientService,
    DeploymentMetricsClientService,
    PFSProjectUsageClientService,
    build_client,
)
from pfcli.service.config import build_deployment_configurator
from pfcli.service.formatter import PanelFormatter, TableFormatter
from pfcli.utils.format import (
    datetime_to_pretty_str,
    datetime_to_simple_string,
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
        "ready_replicas",
        "vms",
        "config.vm.gpu_type",
        "config.total_gpus",
        "start",
        "security_level",
        "config.infrequest_log",
        "endpoint",
    ],
    headers=[
        "ID",
        "Name",
        "Type",
        "Status",
        "#Ready",
        "VM Type",
        "GPU Type",
        "#GPUs",
        "Start",
        "Security Level",
        "Logging",
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
        "ready_replicas",
        "vms",
        "config.vm.gpu_type",
        "config.total_gpus",
        "start",
    ],
    headers=["ID", "Name", "Status", "#Ready", "VM Type", "GPU Type", "#GPUs", "Start"],
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
        "cloud",
        "vm",
        "created_at",
        "finished_at",
        "gpu_type",
        "duration",
    ],
    headers=[
        "ID",
        "Type",
        "Cloud",
        "VM",
        "Created At",
        "Finished At",
        "GPU",
        "Total Usage (days, HH:MM:SS)",
    ],
)

deployment_event_table = TableFormatter(
    name="Deployment Event",
    fields=[
        "id",
        "type",
        "description",
        "created_at",
    ],
    headers=["ID", "Type", "Description", "Timestamp"],
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
    include_terminated: bool = typer.Option(
        False,
        "--include-terminated",
        help="Show all deployments in my project including terminated deployments",
    ),
    limit: int = typer.Option(20, "--limit", help="The number of deployments to view"),
    from_oldest: bool = typer.Option(
        False, "--from-oldest", help="Show oldest deployments first"
    ),
):
    """List all deployments."""
    project_id = get_current_project_id()
    if project_id is None:
        secho_error_and_exit("Failed to identify project... Please set project again.")

    client: DeploymentClientService = build_client(ServiceType.DEPLOYMENT)
    deployments = client.list_deployments(str(project_id), False)["deployments"]
    if include_terminated:
        deployments += client.list_deployments(str(project_id), True)["deployments"]

    deployments.sort(key=lambda x: x["start"], reverse=not from_oldest)
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

    target_deployment_list = deployments[:limit]
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
    deployment["security_level"] = (
        DeploymentSecurityLevel.PROTECTED.value
        if deployment["config"]["infrequest_perm_check"]
        else DeploymentSecurityLevel.PUBLIC.value
    )
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
    if metrics["latency"]:
        # ns => ms
        metrics["latency"] = (
            "{:.3f}".format(metrics["latency"] / 1000000)
            if "latency" in metrics
            else None
        )
    if metrics["throughput"]:
        metrics["throughput"] = (
            "{:.3f}".format(metrics["throughput"]) if "throughput" in metrics else None
        )
    deployment_metrics_table.render([metrics])


@app.command()
def usage(
    year: int = typer.Argument(...),
    month: int = typer.Argument(...),
    day: Optional[int] = typer.Argument(None),
):
    """Show total usage of deployments in project in a month or a day."""
    client: PFSProjectUsageClientService = build_client(ServiceType.PFS_PROJECT_USAGE)
    try:
        start_date = datetime(year, month, day if day else 1, tzinfo=timezone.utc)
    except ValueError:
        secho_error_and_exit(f"Invalid date({year}-{month}{f'-{day}' if day else ''})")
    if day:
        end_date = start_date + timedelta(days=1)
    else:
        end_date = datetime(
            year + int(month == 12),
            (month + 1) if month < 12 else 1,
            1,
            tzinfo=timezone.utc,
        )
    usages = client.get_usage(start_date, end_date)
    deployments = [
        {
            "id": id,
            "type": info["deployment_type"],
            "cloud": info["cloud"].upper() if "cloud" in info else None,
            "vm": info["vm"]["name"] if info.get("vm") else None,
            "gpu_type": info["vm"]["gpu_type"].upper() if info.get("vm") else None,
            "created_at": datetime_to_simple_string(parse(info["created_at"])),
            "finished_at": datetime_to_simple_string(parse(info["finished_at"]))
            if info["finished_at"]
            else "-",
            "duration": timedelta(seconds=int(info["duration"])),
        }
        for id, info in usages.items()
        if int(info["duration"]) != 0
    ]
    deployment_usage_table.render(deployments)


@app.command()
def log(
    deployment_id: str = typer.Argument(..., help="deployment id to get log."),
    replica_index: int = typer.Argument(
        0, help="replica index of deployment to get log."
    ),
):
    """Show deployments log."""
    client: DeploymentLogClientService = build_client(
        ServiceType.DEPLOYMENT_LOG, deployment_id=deployment_id
    )
    log = client.get_deployment_log(
        deployment_id=deployment_id, replica_index=replica_index
    )
    for line in log:
        typer.echo(line["data"])


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
    num_replicas: int = typer.Option(
        1, "--replicas", "-rp", help="Number of replicas to run deployment."
    ),
    security_level: DeploymentSecurityLevel = typer.Option(
        DeploymentSecurityLevel.PUBLIC,
        "--security-level",
        help="Security level of deployment endpoints",
    ),
    logging: bool = typer.Option(
        False,
        "--logging",
        "-l",
        help="Logging inference requests or not.",
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
        config: Dict[str, Any] = yaml.safe_load(config_file)
    except yaml.YAMLError as e:
        secho_error_and_exit(f"Error occurred while parsing engine config file... {e}")

    total_gpus = 1
    if engine == EngineType.ORCA:
        try:
            orca_config: dict = config["orca_config"]
        except KeyError:
            secho_error_and_exit(
                "The config file does not include the `orca_config` key as root."
            )

        num_devices = orca_config.get("num_devices", 1)
        num_workers = orca_config.get("num_workers", 1)
        num_sessions = orca_config.get("num_sessions", 1)
        total_gpus = num_devices * num_workers * num_sessions

    request_data = {
        "project_id": str(project_id),
        "model_id": checkpoint_id,
        "deployment_type": deployment_type,
        "name": deployment_name,
        "vm": {"gpu_type": gpu_type},
        "cloud": cloud,
        "region": region,
        "total_gpus": total_gpus,
        "num_replicas": num_replicas,
        "infrequest_perm_check": True
        if security_level == DeploymentSecurityLevel.PROTECTED
        else False,
        "infrequest_log": True if logging else False,
        **config,
    }
    client: DeploymentClientService = build_client(ServiceType.DEPLOYMENT)
    deployment = client.create_deployment(request_data)

    typer.secho(
        f"Deployment ({deployment['id']}) started successfully. Use 'pf deployment view {deployment['id']}' to see the deployment details.\n"
        f"Send inference requests to '{deployment['endpoint']}'.",
        fg=typer.colors.BLUE,
    )


@app.command()
def update(
    deployment_id: str = typer.Argument(..., help="Deployment id to update."),
    replicas: int = typer.Option(
        ..., "--replicas", "-r", help="Number of replicas to scale deployment."
    ),
):
    """Update deployment.
    # TODO: Add more update options.
    """
    client: DeploymentClientService = build_client(ServiceType.DEPLOYMENT)
    if replicas < 0:
        secho_error_and_exit(
            "The replicas argument should be equal to or greater than 0."
        )

    client.scale_deployment(deployment_id=deployment_id, replicas=replicas)
    typer.secho(
        f"Deployment ({deployment_id}) scale to {replicas}.",
        fg=typer.colors.BLUE,
    )


@app.command()
def event(
    deployment_id: str = typer.Argument(..., help="Deployment id to get events."),
):
    """Get deployment events."""
    client: DeploymentEventClientService = build_client(
        ServiceType.DEPLOYMENT_EVENT, deployment_id=deployment_id
    )

    events = client.get_event(deployment_id=deployment_id)
    for event in events:
        event["id"] = f"periflow-deployment-{event['namespace']}"
        event["created_at"] = datetime_to_simple_string(parse(event["created_at"]))
    deployment_event_table.render(events)


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
