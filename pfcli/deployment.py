# Copyright (C) 2021 FriendliAI

"""CLI for Deployment"""

from dateutil.parser import parse
from typing import Optional

import ruamel.yaml
import typer
import yaml
from click import Choice

from pfcli.service import (
    ServiceType,
    GpuType,
    CloudType,
    EngineType,
)
from pfcli.service.client import (
    DeploymentClientService,
    build_client,
)
from pfcli.service.config import build_deployment_configurator
from pfcli.service.formatter import PanelFormatter, TableFormatter
from pfcli.utils.format import datetime_to_pretty_str, secho_error_and_exit
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
    name="Overview",
    fields=[
        "id",
        "config.name",
        "status",
        "vms",
        "config.gpu_type",
        "config.total_gpus",
        "start",
        "endpoint",
    ],
    headers=["ID", "Name", "Status", "VM", "Device", "Device Cnt", "Start", "Endpoint"],
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

deployment_table.add_substitution_rule("waiting", "[bold]waiting")
deployment_table.add_substitution_rule("enqueued", "[bold cyan]enqueued")
deployment_table.add_substitution_rule("running", "[bold blue]running")
deployment_table.add_substitution_rule("success", "[bold green]success")
deployment_table.add_substitution_rule("failed", "[bold red]failed")
deployment_table.add_substitution_rule("terminated", "[bold yellow]terminated")
deployment_table.add_substitution_rule("terminating", "[bold magenta]terminating")
deployment_table.add_substitution_rule("cancelling", "[bold magenta]cancelling")


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
    client: DeploymentClientService = build_client(ServiceType.DEPLOYMENT)
    deployments = client.list_deployments()["deployments"]

    for deployment in deployments:
        started_at = deployment.get("start")
        if started_at is not None:
            start = datetime_to_pretty_str(parse(started_at))
        else:
            start = None
        deployment["start"] = start
        deployment["vms"] = deployment["vms"][0]["name"]

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
    deployment["vms"] = deployment["vms"][0]["name"]
    deployment_panel.render([deployment])


@app.command()
def create(
    project_id: str = typer.Option(
        ..., "--project-id", "-pid", help="Project to deploy."
    ),
    checkpoint_id: str = typer.Option(
        ..., "--checkpoint-id", "-id", help="Checkpoint id to deploy."
    ),
    deployment_name: str = typer.Option(
        ..., "--name", "-n", help="The name of deployment. "
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
    if engine is not EngineType.ORCA:
        secho_error_and_exit("Only ORCA is supported!")

    try:
        config: dict = yaml.safe_load(config_file)
    except yaml.YAMLError as e:
        secho_error_and_exit(f"Error occurred while parsing engine config file... {e}")

    request_data = {
        "project_id": project_id,
        "model_id": checkpoint_id,
        "name": deployment_name,
        "gpu_type": gpu_type,
        "cloud": cloud,
        "region": region,
        **config,
    }
    typer.secho(f"request_data: {request_data}")
    client: DeploymentClientService = build_client(ServiceType.DEPLOYMENT)
    typer.secho(f"deployment url:{client.url_kwargs}")
    deployment = client.create_deployment(request_data)

    typer.secho(
        f"Deployment ({deployment['id']}) started successfully. Use 'pf deployment view <id>' to see the deployment details.\n"
        f"Run 'curl {deployment['endpoint']}' for inference request",
        fg=typer.colors.BLUE,
    )


@template_app.command("create")
def template_crate(
    save_path: typer.FileTextWrite = typer.Option(
        ..., "--save-path", "-s", help="Path to save job YAML configruation file."
    )
):
    """Create a deployment engine configuration YAML file."""
    engine_type = typer.prompt(
        "What kind of engine type do you want?\n",
        type=Choice([e.value for e in EngineType]),
        prompt_suffix="\n>> ",
    )
    configurator = build_deployment_configurator(engine_type)
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
