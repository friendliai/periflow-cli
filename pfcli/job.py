# Copyright (C) 2021 FriendliAI

"""PeriFlow Job"""

from __future__ import annotations

import asyncio
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple, Union
from uuid import UUID

import ruamel.yaml
import tabulate
import typer
from click import Choice
from dateutil import parser
from dateutil.parser import parse
from rich.columns import Columns
from rich.live import Live
from rich.spinner import Spinner
from rich.text import Text

from pfcli.configurator.job import (
    JobConfigurator,
    build_job_interactive_configurator,
    get_configurator,
)
from pfcli.service import (
    JobStatus,
    JobType,
    ServiceType,
    SimpleJobStatus,
    job_status_map,
    job_status_map_inv,
    storage_type_map_inv,
)
from pfcli.service.client import (
    JobWebSocketClientService,
    ProjectJobClientService,
    UserClientService,
    build_client,
)
from pfcli.service.client.job import (
    ProjectJobArtifactClientService,
    ProjectJobCheckpointClientService,
)
from pfcli.service.client.metrics import MetricsClientService
from pfcli.service.formatter import PanelFormatter, TableFormatter
from pfcli.utils.format import (
    datetime_to_pretty_str,
    datetime_to_simple_string,
    secho_error_and_exit,
    timedelta_to_pretty_str,
    utc_to_local,
)
from pfcli.utils.prompt import get_default_editor, open_editor
from pfcli.utils.validate import validate_datetime_format

tabulate.PRESERVE_WHITESPACE = True

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
metrics_app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False,
)

app.add_typer(template_app, name="template", help="Manage job templates.")
app.add_typer(metrics_app, name="metrics", help="Show job metrics.")

job_table = TableFormatter(
    name="Jobs",
    fields=[
        "number",
        "name",
        "status",
        "vm_config.vm_config_type.code",
        "vm_config.vm_config_type.device_type",
        "num_desired_devices",
        "data_name",
        "user.username",
        "started_at",
        "duration",
        "progress",
    ],
    headers=[
        "Number",
        "Name",
        "Status",
        "VM",
        "Device",
        "Device Cnt",
        "Data",
        "Created by",
        "Start",
        "Duration",
        "Progress",
    ],
)
job_table.apply_styling("ID", style="bold")
job_table.add_substitution_rule("waiting", "[bold]waiting")
job_table.add_substitution_rule("allocating", "[bold cyan]allocating")
job_table.add_substitution_rule("preparing", "[bold cyan]preparing")
job_table.add_substitution_rule("running", "[bold blue]running")
job_table.add_substitution_rule("success", "[bold green]success")
job_table.add_substitution_rule("failed", "[bold red]failed")
job_table.add_substitution_rule("stopping", "[bold magenta]stopping")
job_table.add_substitution_rule("stopped", "[bold yellow]stopped")
job_table.add_substitution_rule("None", "-")
job_panel = PanelFormatter(
    name="Overview",
    fields=[
        "id",
        "number",
        "name",
        "status",
        "vm_config.vm_config_type.code",
        "vm_config.vm_config_type.device_type",
        "num_desired_devices",
        "data_name",
        "started_at",
        "duration",
        "progress",
        "error_message",
    ],
    headers=[
        "ID",
        "Number",
        "Name",
        "Status",
        "VM",
        "Device",
        "Device Cnt",
        "Data",
        "Start",
        "Duration",
        "Progress",
        "Error",
    ],
)
job_panel.add_substitution_rule("waiting", "[bold]waiting")
job_panel.add_substitution_rule("enqueued", "[bold cyan]enqueued")
job_panel.add_substitution_rule("running", "[bold blue]running")
job_panel.add_substitution_rule("success", "[bold green]success")
job_panel.add_substitution_rule("failed", "[bold red]failed")
job_panel.add_substitution_rule("terminated", "[bold yellow]terminated")
job_panel.add_substitution_rule("terminating", "[bold magenta]terminating")
job_table.add_substitution_rule("cancelled", "[bold yellow]cancelled")
job_panel.add_substitution_rule("cancelling", "[bold magenta]cancelling")
job_panel.add_substitution_rule("None", "-")
ckpt_table = TableFormatter(
    name="Checkpoints",
    fields=["id", "vendor", "region", "iteration", "model_form_category", "created_at"],
    headers=["ID", "Cloud", "Region", "Iteration", "Format", "Created At"],
)
artifact_table = TableFormatter(
    name="Artifacts",
    fields=["id", "name", "path", "mtime", "mime_type"],
    headers=["ID", "Name", "Path", "Mtime", "Media Type"],
)
metrics_list_table = TableFormatter(
    name="Metrics List",
    fields=["name"],
    headers=["Name"],
)
metrics_table = TableFormatter(
    name="Metrics",
    fields=["name", "iteration", "created", "value"],
    headers=["Name", "Iteration", "Created", "Value"],
)

PROGRESS_STRING_PATTERNS = [
    r"VM allocation is in progress.*",
    r"Preparing disks for training datasets.*",
    r"Checking VM health.*",
    r"Pulling docker images.*",
]


def is_progress_string(s: str) -> bool:
    for pattern in PROGRESS_STRING_PATTERNS:
        if re.fullmatch(pattern, s):
            return True
    return False


@app.command()
def run(
    config_file: typer.FileText = typer.Option(
        ..., "--config-file", "-f", help="Path to configuration file"
    ),
    workspace_dir: Optional[Path] = typer.Option(
        None,
        "--workspace-dir",
        "-w",
        help="Path to workspace directory in your local file system",
    ),
    job_name: Optional[str] = typer.Option(
        None,
        "--name",
        "-n",
        help="The name of this job. "
        "If not provided, the value in the config file will be used.",
    ),
    vm_name: Optional[str] = typer.Option(
        None,
        "--vm",
        "-v",
        help="VM type. You can check the list of VMs with `pf vm list`. "
        "If not provided, the value in the config file will be used.",
    ),
    num_devices: Optional[int] = typer.Option(
        None, "--num-devices", "-d", help="The number of devices to use in the job"
    ),
):
    """Run a job."""
    configurator = get_configurator(f=config_file)
    configurator.update_config(
        vm=vm_name,
        num_devices=num_devices,
        name=job_name,
    )
    configurator.validate()
    config = configurator.get_job_request_body()

    if workspace_dir is not None:
        workspace_dir = workspace_dir.resolve()  # ensure absolute path
        if not workspace_dir.exists():
            secho_error_and_exit("Specified workspace does not exist...")
        if not workspace_dir.is_dir():
            secho_error_and_exit("Specified workspace is not directory...")

    client: ProjectJobClientService = build_client(ServiceType.PROJECT_JOB)
    job_data = client.run_job(config, workspace_dir)

    typer.secho(
        f"Job ({job_data['number']}) started successfully. Use 'pf job log {job_data['number']}' to see the job logs.",
        fg=typer.colors.BLUE,
    )


@app.command()
def list(
    limit: int = typer.Option(
        20,
        "--limit",
        "-l",
        help="The number of recent jobs to see.",
    ),
    show_all: bool = typer.Option(
        False,
        "--all",
        "-a",
        help="Show all jobs in my project including jobs launched by other users",
    ),
    since: str = typer.Option(
        None,
        "--since",
        help="Filter jobs by creation time. The format should be {YYYY}-{MM}-{DD}T{HH}:{MM}:{SS}. "
        "The local timezone will be used by default.",
        callback=validate_datetime_format,
    ),
    until: str = typer.Option(
        None,
        "--until",
        help="Filter jobs by creation time. The format should be {YYYY}-{MM}-{DD}T{HH}:{MM}:{SS}. "
        "The local timezone will be used by default.",
        callback=validate_datetime_format,
    ),
    job_name: str = typer.Option(
        None,
        "--job-name",
        help="Filter jobs by job name",
    ),
    vm: str = typer.Option(
        None,
        "--vm",
        help="Filter jobs by vm name",
    ),
    status: SimpleJobStatus = typer.Option(
        None,
        "--status",
        help="Filter jobs by job status",
    ),
):
    """List all jobs."""
    client: ProjectJobClientService = build_client(ServiceType.PROJECT_JOB)

    user_ids = None
    if not show_all:
        user_client: UserClientService = build_client(ServiceType.USER)
        user_ids = [user_client.get_current_user_id()]

    real_statuses = job_status_map_inv[status] if status is not None else None
    jobs = client.list_jobs(
        limit=limit,
        since=since,
        until=until,
        job_name=job_name,
        vm=vm,
        statuses=real_statuses,
        user_ids=user_ids,
    )

    for job in jobs:
        started_at = job.get("started_at")
        finished_at = job.get("finished_at")
        if started_at is not None:
            start = datetime_to_pretty_str(parse(job["started_at"]))
        else:
            start = None
        if started_at is not None and finished_at is not None:
            duration = timedelta_to_pretty_str(parse(finished_at) - parse(started_at))
        elif started_at is not None and job["status"] == JobStatus.RUNNING:
            start_time = parse(started_at)
            curr_time = datetime.now(start_time.tzinfo)
            duration = timedelta_to_pretty_str(curr_time - start_time)
        else:
            duration = None

        job["started_at"] = start
        job["duration"] = duration
        if job["data_store"] is not None:
            job["data_name"] = job["data_store"]["name"]
        elif job["public_data"] is not None:
            job["data_name"] = job["public_data"]
        else:
            job["data_name"] = None
        if job["progress"] is not None:
            job["progress"] = "{:.2f}%".format(job["progress"])
        job["status"] = job_status_map[job["status"]].value

    job_table.render(jobs)


@app.command()
def stop(job_number: int = typer.Argument(..., help="Job number to be stopped")):
    """Termiate/cancel a running/enqueued job."""
    client: ProjectJobClientService = build_client(ServiceType.PROJECT_JOB)
    job_status = client.get_job(job_number)["status"]

    if job_status == JobStatus.WAITING:
        client.cancel_job(job_number)
    elif job_status in (
        JobStatus.ENQUEUED,
        JobStatus.STARTED,
        JobStatus.ALLOCATING,
        JobStatus.PREPARING,
        JobStatus.RUNNING,
    ):
        client.terminate_job(job_number)
    else:
        secho_error_and_exit(f"No need to stop {job_status} job...")


@app.command()
def delete(
    job_number: int = typer.Argument(..., help="Job number to be deleted"),
):
    """Delete a job."""
    do_delete = typer.confirm("Are your sure to delete job?")
    if not do_delete:
        raise typer.Abort()

    client: ProjectJobClientService = build_client(ServiceType.PROJECT_JOB)
    client.delete_job(job_number)

    typer.secho(f"Job ({job_number}) deleted successfully!", fg=typer.colors.BLUE)


@app.command()
def view(
    job_number: int = typer.Argument(..., help="Job number to view detail"),
):
    """Show job detail."""
    job_client: ProjectJobClientService = build_client(ServiceType.PROJECT_JOB)
    job_checkpoint_client: ProjectJobCheckpointClientService = build_client(
        ServiceType.PROJECT_JOB_CHECKPOINT, job_number=job_number
    )
    job_artifact_client: ProjectJobArtifactClientService = build_client(
        ServiceType.PROJECT_JOB_ARTIFACT, job_number=job_number
    )

    job = job_client.get_job(job_number)
    job_checkpoints = job_checkpoint_client.list_checkpoints()
    job_artifacts = job_artifact_client.list_artifacts()

    started_at = job.get("started_at")
    finished_at = job.get("finished_at")
    if started_at is not None:
        start = datetime_to_pretty_str(parse(job["started_at"]))
    else:
        start = None
    if started_at is not None and finished_at is not None:
        duration = timedelta_to_pretty_str(parse(finished_at) - parse(started_at))
    elif started_at is not None and job["status"] == JobStatus.RUNNING:
        start_time = parse(started_at)
        curr_time = datetime.now(start_time.tzinfo)
        duration = timedelta_to_pretty_str(curr_time - start_time)
    else:
        duration = None

    job["started_at"] = start
    job["duration"] = duration
    if job["data_store"] is not None:
        job["data_name"] = job["data_store"]["name"]
    elif job["public_data"] is not None:
        job["data_name"] = job["public_data"]
    else:
        job["data_name"] = None
    if job["progress"] is not None:
        job["progress"] = "{:.2f}%".format(job["progress"])
    job["status"] = job_status_map[job["status"]].value

    checkpoint_list = []
    for checkpoint in reversed(job_checkpoints):
        checkpoint["created_at"] = datetime_to_pretty_str(
            parse(checkpoint["created_at"]), long_list=True
        )
        checkpoint["vendor"] = storage_type_map_inv[checkpoint["vendor"]].value
        checkpoint_list.append(checkpoint)

    job_panel.render([job], show_detail=True)
    ckpt_table.render(checkpoint_list)
    artifact_table.render(job_artifacts)


@template_app.command("create")
def template_create(
    save_path: typer.FileTextWrite = typer.Option(
        ..., "--save-path", "-s", help="Path to save job YAML configruation file."
    )
):
    """Create a job configuration YAML file"""
    job_type = typer.prompt(
        "What kind of job do you want?\n",
        type=Choice([e.value for e in JobType]),
        prompt_suffix="\n>> ",
    )
    configurator = build_job_interactive_configurator(JobType(job_type))
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


def _split_machine_ids(value: Optional[str]) -> Optional[List[int]]:
    if value is None:
        return value
    try:
        return [int(machine_id) for machine_id in value.split(",")]
    except ValueError:
        secho_error_and_exit("Machine index should be integer. (e.g., --machine 0,1,2)")


def _format_log_string(
    log_record: Dict[str, Any],
    show_time: bool,
    show_machine_id: bool,
    use_style: bool = True,
    show_loading: bool = True,
) -> Generator[Tuple[Union[str, Columns], bool], None, None]:
    timestamp_str = f"⏰ {datetime_to_simple_string(utc_to_local(parser.parse(log_record['timestamp'])))} "
    node_rank = log_record["node_rank"]
    node_rank_str = "📈 PF " if node_rank == -1 else f"💻 #{node_rank} "

    if use_style:
        timestamp_str = typer.style(timestamp_str, fg=typer.colors.BLUE)
        node_rank_str = typer.style(node_rank_str, fg=typer.colors.GREEN)

    lines = [x for x in re.split(r"(\n|\r)", log_record["content"]) if x]

    job_finished = False
    for line in lines:
        if line in ("\n", "\r"):
            yield line, job_finished
        else:
            use_spinner = show_loading and is_progress_string(line)
            job_finished = (
                line in ("Job completed successfully.", "Job failed.")
                and node_rank == -1
            )
            additional_info = ""
            if node_rank == -1:
                line = typer.style(line, fg=typer.colors.MAGENTA)
            if show_machine_id:
                additional_info += node_rank_str
            if show_time:
                additional_info += timestamp_str
            if use_spinner:
                line = Columns(
                    [
                        Text(additional_info),
                        Spinner(name="dots", text=line, style="green"),
                    ]
                )
            else:
                line = additional_info + line
            yield line, job_finished


async def monitor_logs(
    job_id: UUID,
    log_types: Optional[List[str]],
    machines: Optional[List[int]],
    show_time: bool,
    show_machine_id: bool,
):
    ws_client: JobWebSocketClientService = build_client(ServiceType.JOB_WS)

    job_finished = False
    async with ws_client.open_connection(job_id, log_types, machines):
        with Live(transient=True) as live:
            async for response in ws_client:
                if live.is_started:
                    live.stop()
                for line, job_finished in _format_log_string(
                    log_record=response,
                    show_time=show_time,
                    show_machine_id=show_machine_id,
                    show_loading=True,
                ):
                    if isinstance(line, str):
                        typer.echo(line, nl=False)
                    else:
                        sys.stdout.write("\033[F")  # Cursor up one line
                        live.update(line)
                        live.start()
                    if job_finished:
                        return


# TODO: Implement since/until if necessary
@app.command()
def log(
    job_number: int = typer.Argument(..., help="Job number to view log"),
    num_records: int = typer.Option(
        100, "--num-records", "-n", help="The number of recent records to view"
    ),
    content: Optional[str] = typer.Option(
        None, "--content", "-c", help="Filter logs by content"
    ),
    machines: str = typer.Option(
        None,
        "--machine",
        "-m",
        callback=_split_machine_ids,
        help="Filter logs by machine ID. Comma-separated indices of machine to print logs (e.g., 0,1,2,3). "
        "By default, it will print logs from all machines.",
    ),
    head: bool = typer.Option(False, "--head", help="View logs from the oldest one"),
    export_path: Optional[Path] = typer.Option(
        None, "--export-path", "-e", help="Path to export logs"
    ),
    follow: bool = typer.Option(False, "--follow", "-f", help="Follow logs"),
    show_time: bool = typer.Option(
        False, "--timestamp", "-t", help="Print logs with timestamp"
    ),
    show_machine_id: bool = typer.Option(
        False, "--show-machine-id", help="Print logs with machine index"
    ),
):
    """Show job logs."""
    if num_records <= 0 or num_records > 10000:
        secho_error_and_exit(
            "'num_records' should be a positive integer, equal or smaller than 10000"
        )

    if head and follow:
        secho_error_and_exit("'follow' cannot be set in 'head' mode")

    if export_path is not None and follow:
        secho_error_and_exit("'follow' cannot be set when 'export_path' is given")

    client: ProjectJobClientService = build_client(ServiceType.PROJECT_JOB)
    logs = client.get_text_logs(
        job_number=job_number,
        num_records=num_records,
        head=head,
        log_types=None,
        machines=machines,  # type: ignore
        content=content,
    )
    status = client.get_job(job_number=job_number)["status"]

    if export_path is not None:
        with export_path.open("w", encoding="utf-8") as export_file:
            for record in logs:
                for line, _ in _format_log_string(
                    record,
                    show_time,
                    show_machine_id,
                    use_style=False,
                    show_loading=False,
                ):
                    assert isinstance(line, str)
                    export_file.write(line)
    else:
        for record in logs:
            for line, _ in _format_log_string(
                record,
                show_time,
                show_machine_id,
                show_loading=False,
            ):
                typer.echo(line, nl=False)
    job_finished = status in (
        JobStatus.TERMINATED,
        JobStatus.FAILED,
        JobStatus.SUCCESS,
        JobStatus.CANCELLED,
    )

    if not job_finished and follow:
        job_client: ProjectJobClientService = build_client(ServiceType.PROJECT_JOB)
        job_id = UUID(job_client.get_job(job_number)["id"])
        try:
            # Subscribe job log
            asyncio.run(
                monitor_logs(
                    job_id=job_id,
                    log_types=None,
                    machines=machines,  # type: ignore
                    show_time=show_time,
                    show_machine_id=show_machine_id,
                )
            )
        except KeyboardInterrupt:
            secho_error_and_exit(f"Keyboard Interrupt...", color=typer.colors.MAGENTA)


@metrics_app.command("list")
def metrics_list(
    job_number: int = typer.Argument(..., help="Job number"),
):
    """Show available metrics"""
    job_client: ProjectJobClientService = build_client(ServiceType.PROJECT_JOB)
    job = job_client.get_job(job_number)
    job_id = job["id"]

    client: MetricsClientService = build_client(ServiceType.METRICS)
    results = client.list_metrics(job_id=job_id)
    metrics_list_table.render(results)


@metrics_app.command("show")
def show_metrics(
    job_number: int = typer.Argument(..., help="Job number"),
    name: List[str] = typer.Option(..., help="metrics name"),
    limit: int = typer.Option(10, help="Number of metrics to show"),
):
    """Show latest metrics values"""
    if limit <= 0 or limit > 1000:
        secho_error_and_exit(
            "'limit' should be a positive integer, equal or smaller than 1000"
        )
    job_client: ProjectJobClientService = build_client(ServiceType.PROJECT_JOB)
    job = job_client.get_job(job_number)
    job_id = job["id"]

    client: MetricsClientService = build_client(ServiceType.METRICS)
    metrics_set = []
    for metric_name in name:
        metrics = client.get_metrics_values(job_id, metric_name, limit)
        if not metrics:
            secho_error_and_exit(
                f"There is no available metrics with name '{metric_name}'."
            )
        metrics_set.append(metrics)

    for metrics in metrics_set:
        metrics_table.render(metrics)
