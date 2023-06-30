# Copyright (C) 2021 FriendliAI

"""CLI for Deployment"""

from __future__ import annotations

import ast
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from uuid import UUID

import ruamel.yaml
import typer
import yaml
from dateutil.parser import parse
from dateutil.tz import tzlocal
from tqdm import tqdm

from pfcli.configurator.deployment import (
    DRCConfigurator,
    build_deployment_interactive_configurator,
)
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
from pfcli.service.client.deployment import DeploymentReqRespClientService
from pfcli.service.client.file import FileClientService, GroupProjectFileClientService
from pfcli.service.client.user import UserGroupProjectClientService
from pfcli.service.formatter import PanelFormatter, TableFormatter
from pfcli.utils.format import (
    datetime_to_pretty_str,
    datetime_to_simple_string,
    extract_datetime_part,
    extract_deployment_id_part,
    secho_error_and_exit,
)
from pfcli.utils.fs import download_file, upload_file
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
        "deployment_id",
        "config.name",
        "description",
        "config.deployment_type",
        "config.model_id",
        "status",
        "ready_replicas",
        "config.scaler_config.min_replica_count",
        "config.scaler_config.max_replica_count",
        "vms",
        "config.vm.gpu_type",
        "config.total_gpus",
        "start",
        "end",
        "security_level",
        "config.infrequest_log",
        "endpoint",
    ],
    headers=[
        "ID",
        "Name",
        "Description",
        "Type",
        "Ckpt ID",
        "Status",
        "#Ready",
        "Min Replicas",
        "Max Replicas",
        "VM Type",
        "GPU Type",
        "#GPUs",
        "Start",
        "End",
        "Security Level",
        "Logging",
        "Endpoint",
    ],
    extra_fields=["error"],
    extra_headers=["error"],
    substitute_exact_match_only=False,
)

deployment_table = TableFormatter(
    name="Deployments",
    fields=[
        "deployment_id",
        "config.name",
        "description",
        "status",
        "ready_replicas",
        "vms",
        "config.vm.gpu_type",
        "config.total_gpus",
        "start",
    ],
    headers=[
        "ID",
        "Name",
        "Description",
        "Status",
        "#Ready",
        "VM Type",
        "GPU Type",
        "#GPUs",
        "Start",
    ],
    extra_fields=["error"],
    extra_headers=["error"],
    substitute_exact_match_only=False,
)

deployment_org_table = TableFormatter(
    name="Deployments",
    fields=[
        "deployment_id",
        "config.name",
        "description",
        "status",
        "ready_replicas",
        "vms",
        "config.vm.gpu_type",
        "config.total_gpus",
        "start",
        "config.project_id",
        "project_name",
    ],
    headers=[
        "ID",
        "Name",
        "Description",
        "Status",
        "#Ready",
        "VM Type",
        "GPU Type",
        "#GPUs",
        "Start",
        "Project ID",
        "Project Name",
    ],
    extra_fields=["error"],
    extra_headers=["error"],
    substitute_exact_match_only=False,
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

deployment_panel.add_substitution_rule(
    "Initializing", "[bold yellow]Initializing[/bold yellow]"
)
deployment_panel.add_substitution_rule("Healthy", "[bold green]Healthy[/bold green]")
deployment_panel.add_substitution_rule("Unhealthy", "[bold red]Unhealthy[/bold red]")
deployment_panel.add_substitution_rule(
    "Stopping", "[bold magenta]Stopping[/bold magenta]"
)
deployment_panel.add_substitution_rule("Terminated", "[bold]Terminated[/bold]")

deployment_table.add_substitution_rule(
    "Initializing", "[bold yellow]Initializing[/bold yellow]"
)
deployment_table.add_substitution_rule("Healthy", "[bold green]Healthy[/bold green]")
deployment_table.add_substitution_rule("Unhealthy", "[bold red]Unhealthy[/bold red]")
deployment_table.add_substitution_rule(
    "Stopping", "[bold magenta]Stopping[/bold magenta]"
)
deployment_table.add_substitution_rule("Terminated", "[bold]Terminated[/bold]")

deployment_org_table.add_substitution_rule(
    "Initializing", "[bold yellow]Initializing[/bold yellow]"
)
deployment_org_table.add_substitution_rule(
    "Healthy", "[bold green]Healthy[/bold green]"
)
deployment_org_table.add_substitution_rule(
    "Unhealthy", "[bold red]Unhealthy[/bold red]"
)
deployment_org_table.add_substitution_rule(
    "Stopping", "[bold magenta]Stopping[/bold magenta]"
)
deployment_org_table.add_substitution_rule("Terminated", "[bold]Terminated[/bold]")


def get_deployment_id_from_namespace(namespace: str):
    """Get deployment id from namespace."""
    return f"periflow-deployment-{namespace}"


@app.command()
def list(
    include_terminated: bool = typer.Option(
        False,
        "--include-terminated",
        help="Show all deployments in my project including terminated deployments. "
        "The active deployments are shown above the terminated ones.",
    ),
    limit: int = typer.Option(20, "--limit", help="The number of deployments to view"),
    from_oldest: bool = typer.Option(
        False, "--from-oldest", help="Show oldest deployments first"
    ),
    org: bool = typer.Option(False, "--org", help="Show all deployments in org"),
):
    """List all deployments."""
    project_id = get_current_project_id()
    if project_id is None:
        secho_error_and_exit("Failed to identify project... Please set project again.")

    client: DeploymentClientService = build_client(ServiceType.DEPLOYMENT)
    deployments = client.list_deployments(
        project_id=str(project_id) if not org else None,
        archived=False,
        limit=limit,
        from_oldest=from_oldest,
    )
    num_active_deployments = len(deployments)
    if include_terminated and limit > num_active_deployments:
        deployments += client.list_deployments(
            project_id=str(project_id) if not org else None,
            archived=True,
            limit=limit - num_active_deployments,
            from_oldest=from_oldest,
        )

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
        deployment["deployment_id"] = get_deployment_id_from_namespace(
            deployment["namespace"]
        )

    table = deployment_table
    if org:
        table = deployment_org_table
        project_client: UserGroupProjectClientService = build_client(
            ServiceType.USER_GROUP_PROJECT
        )
        projects = project_client.list_projects()
        project_map = {project["id"]: project["name"] for project in projects}
        for deployment in deployments:
            deployment["project_name"] = project_map[deployment["config"]["project_id"]]

    table.render(deployments)


@app.command()
def delete(deployment_id: str = typer.Argument(..., help="ID of deployment to delete")):
    """Delete deployment."""
    client: DeploymentClientService = build_client(ServiceType.DEPLOYMENT)
    client.delete_deployment(deployment_id)
    typer.secho(
        f"Deleted Deployment ({deployment_id}) successfully.",
        fg=typer.colors.GREEN,
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
    end = deployment.get("end")
    end = datetime_to_pretty_str(parse(end)) if end is not None else None
    deployment["start"] = start
    deployment["end"] = end
    deployment["vms"] = deployment["vms"][0]["name"] if deployment["vms"] else "None"
    deployment["security_level"] = (
        DeploymentSecurityLevel.PROTECTED.value
        if deployment["config"]["infrequest_perm_check"]
        else DeploymentSecurityLevel.PUBLIC.value
    )
    deployment["deployment_id"] = get_deployment_id_from_namespace(
        deployment["namespace"]
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
    description: Optional[str] = typer.Option(
        None, "--description", "-d", help="Deployment description."
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
    default_request_config_file: Optional[typer.FileText] = typer.Option(
        None,
        "--default-request-config-file",
        "-drc",
        help="Path to default request config",
    ),
    min_replicas: int = typer.Option(
        1,
        "--min-replicas",
        "-min",
        help="Number of minimum replicas.",
    ),
    max_replicas: int = typer.Option(
        1,
        "--max-replicas",
        "-max",
        help="Number of maximum replicas.",
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

    if min_replicas > max_replicas:
        secho_error_and_exit("min_replicas should be less than max_replicas.")

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

    if default_request_config_file:
        configurator = DRCConfigurator.from_file(default_request_config_file)
        configurator.validate()

        file_client: FileClientService = build_client(ServiceType.FILE)
        group_file_client: GroupProjectFileClientService = build_client(
            ServiceType.GROUP_FILE
        )

        file_size = os.stat(default_request_config_file.name).st_size
        if file_size > 10737418240:  # 10 GiB
            secho_error_and_exit(
                f"Size of default request config file({file_size}bytes) should be <= 10 GiB"
            )
        file_info = {
            "name": os.path.basename(default_request_config_file.name),
            "path": os.path.basename(default_request_config_file.name),
            "mtime": datetime.fromtimestamp(
                os.stat(default_request_config_file.name).st_mtime, tz=tzlocal()
            ).isoformat(),
            "size": file_size,
        }
        file_id = group_file_client.create_misc_file(file_info=file_info)["id"]

        upload_url = file_client.get_misc_file_upload_url(misc_file_id=file_id)
        with tqdm(
            total=file_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc="Uploading default request config",
        ) as t:
            upload_file(
                file_path=default_request_config_file.name,
                url=upload_url,
                ctx=t,
            )
        file_client.make_misc_file_uploaded(misc_file_id=file_id)
        config["orca_config"]["default_request_config_id"] = file_id

    config["scaler_config"] = {}
    config["scaler_config"]["min_replica_count"] = min_replicas
    config["scaler_config"]["max_replica_count"] = max_replicas

    request_data = {
        "project_id": str(project_id),
        "model_id": checkpoint_id,
        "deployment_type": deployment_type,
        "name": deployment_name,
        "vm": {"gpu_type": gpu_type},
        "cloud": cloud,
        "region": region,
        "total_gpus": total_gpus,
        "infrequest_perm_check": True
        if security_level == DeploymentSecurityLevel.PROTECTED
        else False,
        "infrequest_log": True if logging else False,
        **config,
    }
    if description:
        request_data["description"] = description
    client: DeploymentClientService = build_client(ServiceType.DEPLOYMENT)
    deployment = client.create_deployment(request_data)
    deployment_id = get_deployment_id_from_namespace(deployment["namespace"])

    typer.secho(
        f"Deployment ({deployment_id}) started successfully. Use 'pf deployment view {deployment_id}' to see the deployment details.\n"
        f"Send inference requests to '{deployment['endpoint']}'.",
        fg=typer.colors.GREEN,
    )


@app.command()
def update(
    deployment_id: str = typer.Argument(..., help="Deployment id to update."),
    min_replicas: int = typer.Option(
        ..., "--min-replicas", "-min", help="Set min_replicas of deployment."
    ),
    max_replicas: int = typer.Option(
        ..., "--max-replicas", "-max", help="Set max_replicas of deployment."
    ),
):
    """[Experimental] Update deployment."""
    if min_replicas > max_replicas:
        secho_error_and_exit(
            "Invalid #replicas: min_replicas should be less than max_replicas."
        )
    client: DeploymentClientService = build_client(ServiceType.DEPLOYMENT)
    client.update_deployment_scaler(
        deployment_id=deployment_id,
        min_replicas=min_replicas,
        max_replicas=max_replicas,
    )
    typer.secho(
        f"Scaler of deployment ({deployment_id}) is updated.\n"
        f"Set min_replicas to {min_replicas}, max_replicas to {max_replicas}",
        fg=typer.colors.GREEN,
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


@app.command()
def req_resp(
    deployment_id: str = typer.Argument(
        ..., help="Deployment ID to download request-response logs."
    ),
    since: str = typer.Option(
        ...,
        "--since",
        help="Start time of request-response logs. The format should be {YYYY}-{MM}-{DD}T{HH}. "
        "The UTC timezone will be used by default.",
    ),
    until: str = typer.Option(
        ...,
        "--until",
        help="End time of request-response logs. The format should be {YYYY}-{MM}-{DD}T{HH}. "
        "The UTC timezone will be used by default.",
    ),
    save_directory: str = typer.Option(
        None,
        "-s",
        "--save-dir",
        help="Directory path to save request-response logs",
    ),
):
    """Download request-response logs for a deployment."""
    if save_directory is not None and not os.path.isdir(save_directory):
        secho_error_and_exit(f"Directory({save_directory}) is not found.")
    save_directory = save_directory or os.getcwd()

    if not os.access(save_directory, os.W_OK):
        secho_error_and_exit(f"Cannot save logs to {save_directory} which is readonly.")

    try:
        start = datetime.strptime(since, "%Y-%m-%dT%H")
        end = datetime.strptime(until, "%Y-%m-%dT%H")
    except ValueError:
        secho_error_and_exit(
            "Invalid datetime format. The format should be {YYYY}-{MM}-{DD}T{HH} (e.g., 1999-01-01T01)."
        )
    if start > end:
        secho_error_and_exit(
            "Time value of `--since` option should be earlier than the value of `--until`."
        )

    client: DeploymentReqRespClientService = build_client(
        ServiceType.DEPLOYMENT_REQ_RESP, deployment_id=deployment_id
    )
    download_infos = client.get_download_urls(
        deployment_id=deployment_id, start=start, end=end
    )
    if len(download_infos) == 0:
        secho_error_and_exit("No logs are found.")

    for i, download_info in enumerate(download_infos):
        typer.echo(f"Downloading files {i + 1}/{len(download_infos)}...")
        full_storage_path = download_info["path"]
        deployment_id_part = extract_deployment_id_part(full_storage_path)
        timestamp_part = extract_datetime_part(full_storage_path)
        filename = f"{deployment_id_part}_{timestamp_part}.log"
        download_file(
            url=download_info["url"], out=os.path.join(save_directory, filename)
        )


@template_app.command("create")
def template_create(
    save_path: typer.FileTextWrite = typer.Option(
        ..., "--save-path", "-s", help="Path to save job YAML configruation file."
    )
):
    """Create a deployment engine configuration YAML file."""
    configurator = build_deployment_interactive_configurator(EngineType.ORCA)
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
