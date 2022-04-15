# Copyright (C) 2021 FriendliAI

"""PeriFlow Job"""

import asyncio
import textwrap
from pathlib import Path
from typing import Optional, List
from dateutil import parser
from dateutil.parser import parse
from datetime import datetime

import tabulate
import typer
import yaml
import ruamel.yaml
from click import Choice

from pfcli.service import JobType, LogType, ServiceType
from pfcli.service.client import (
    GroupDataClientService,
    GroupExperimentClientService,
    GroupJobClientService,
    GroupVMClientService,
    JobArtifactClientService,
    JobCheckpointClientService,
    JobClientService,
    JobTemplateClientService,
    JobWebSocketClientService,
    build_client,
)
from pfcli.service.config import build_job_configurator
from pfcli.service.formatter import TableFormatter
from pfcli.utils import (
    get_default_editor,
    get_remaining_terminal_columns,
    open_editor,
    secho_error_and_exit,
    datetime_to_pretty_str,
    timedelta_to_pretty_str,
    utc_to_local,
    datetime_to_simple_string,
)

app = typer.Typer()
template_app = typer.Typer()
log_app = typer.Typer()

app.add_typer(template_app, name="template", help="Manager job templates.")
app.add_typer(log_app, name="log", help="Manage job logs.")

job_formatter = TableFormatter(
    fields=[
        'id',
        'name',
        'status',
        'vm_config.vm_config_type.vm_instance_type.name',
        'vm_config.vm_config_type.vm_instance_type.device_type',
        'num_desired_devices',
        'data_name',
        'started_at',
        'duration',
    ],
    headers=['id', 'name', 'status', 'vm', 'device', '# devices', 'data', 'start', 'duration'],
    extra_fields=['error_message'],
    extra_headers=['error']
)
ckpt_formatter = TableFormatter(
    fields=['id', 'vendor', 'region', 'iteration', 'created_at'],
    headers=['id', 'cloud', 'region', 'iteration', 'created at']
)
artifact_formatter = TableFormatter(
    fields=['id', 'name', 'path', 'mtime', 'mime_type'],
    headers=['id', 'name', 'path', 'mtime', 'media type']
)


def refine_config(config: dict,
                  vm_name: Optional[str],
                  num_devices: Optional[int],
                  experiment_name: Optional[str],
                  job_name: Optional[str]) -> None:
    assert "job_setting" in config

    if num_devices is not None:
        config["num_devices"] = num_devices
    else:
        assert "num_devices" in config

    if job_name is not None:
        config["name"] = job_name

    if config["job_setting"]["type"] == "custom" and "workspace" not in config["job_setting"]:
        config["job_setting"]["workspace"] = {"mount_path": "/workspace"}

    experiment_client: GroupExperimentClientService = build_client(ServiceType.GROUP_EXPERIMENT)
    data_client: GroupDataClientService = build_client(ServiceType.GROUP_DATA)
    vm_client: GroupVMClientService = build_client(ServiceType.GROUP_VM)
    job_template_client: JobTemplateClientService = build_client(ServiceType.JOB_TEMPLATE)

    experiment_name = experiment_name or config["experiment"]
    experiment_id = experiment_client.get_id_by_name(experiment_name)
    if experiment_id is None:
        create_new = typer.confirm(
            f"Experiment with the name ({experiment_name}) is not found.\n"
            "Do you want to proceed with creating a new experiment? "
            f"If yes, a new experiment will be created with the name: {experiment_name}",
            prompt_suffix="\n>> "
        )
        if not create_new:
            typer.echo("Please run the job after creating an experiment first.")
            raise typer.Abort()
        experiment_id = experiment_client.create_experiment(experiment_name)["id"]
        typer.echo(f"A new experiment ({experiment_name}) is created.")

    del config["experiment"]
    config["experiment_id"] = experiment_id

    vm_name = vm_name or config["vm"]
    vm_config_id = vm_client.get_id_by_name(vm_name)
    if vm_config_id is None:
        secho_error_and_exit(f"VM ({vm_name}) is not found.")
    del config["vm"]
    config["vm_config_id"] = vm_config_id

    if "data" in config:
        data_name = config["data"]["name"]
        data_id = data_client.get_id_by_name(data_name)
        if data_id is None:
            secho_error_and_exit(f"Dataset ({data_name}) is not found.")
        del config["data"]["name"]
        config["data"]["id"] = data_id

    if config["job_setting"]["type"] == "custom":
        config["job_setting"]["launch_mode"] = "node"
    else:
        job_template_name = config["job_setting"]["template_name"]
        job_template_config = job_template_client.get_job_template_by_name(job_template_name)
        if job_template_config is None:
            secho_error_and_exit(f"Predefined job template ({job_template_name}) is not found.")
        del config["job_setting"]["template_name"]
        config["job_setting"]["engine_code"] = job_template_config["engine_code"]
        config["job_setting"]["model_code"] = job_template_config["model_code"]


@app.command("run", help="Run a new job.")
def run(
    config_file: typer.FileText = typer.Option(
        ...,
        "--config-file",
        "-f",
        help="Path to configuration file"
    ),
    workspace_dir: Optional[Path] = typer.Option(
        None,
        "--workspace-dir",
        "-d",
        help="Path to workspace directory in your local file system"
    ),
    vm_name: Optional[str] = typer.Option(
        None,
        "--vm",
        "-v",
        help="VM type. You can check the list of VMs with `pf vm list`. "
             "If not provided, the value in the config file will be used."
    ),
    num_devices: Optional[int] = typer.Option(
        None,
        "--num-devices",
        "-n",
        help="The number of devices to use in the job"
    ),
    experiment_name: Optional[str] = typer.Option(
        None,
        "--experiment",
        "-e",
        help="The name of experiment. "
             "If not provided, the value in the config file will be used."
    ),
    job_name: Optional[str] = typer.Option(
       None,
       "--name",
       "-n",
       help="The name of this job. " 
             "If not provided, the value in the config file will be used."
    )
):
    try:
        config: dict = yaml.safe_load(config_file)
    except yaml.YAMLError as e:
        secho_error_and_exit(f"Error occurred while parsing config file... {e}")

    refine_config(config, vm_name, num_devices, experiment_name, job_name)

    if workspace_dir is not None:
        if not workspace_dir.exists():
            secho_error_and_exit(f"Specified workspace does not exist...")
        if not workspace_dir.is_dir():
            secho_error_and_exit(f"Specified workspace is not directory...")

    client: JobClientService = build_client(ServiceType.JOB)
    job_data = client.run_job(config, workspace_dir)

    typer.secho(
        f"Job ({job_data['id']}) started successfully. Use 'pf job log view' to see the job logs.",
        fg=typer.colors.BLUE
    )


@app.command("list", help="List jobs.")
def list(
    tail: Optional[int] = typer.Option(
        None,
        "--tail",
        "-t",
        help="The number of job list to view at the tail"
    ),
    head: Optional[int] = typer.Option(
        None,
        "--head",
        "-h",
        help="The number of job list to view at the head"
    ),
    show_group_job: bool = typer.Option(
        False,
        "--show-group-job",
        "-g",
        help="Show all jobs in my group including jobs launched by other users"
    )
):
    if show_group_job:
        client: GroupJobClientService = build_client(ServiceType.GROUP_JOB)
    else:
        client: JobClientService = build_client(ServiceType.JOB)
    jobs = client.list_jobs()

    for job in jobs:
        started_at = job.get("started_at")
        finished_at = job.get("finished_at")
        if started_at is not None:
            start = datetime_to_pretty_str(parse(job["started_at"]))
        else:
            start = None
        if started_at is not None and finished_at is not None:
            duration = timedelta_to_pretty_str(parse(started_at), parse(finished_at))
        elif started_at is not None and job["status"] == "running":
            start_time = parse(started_at)
            curr_time = datetime.now(start_time.tzinfo)
            duration = timedelta_to_pretty_str(start_time, curr_time)
        else:
            duration = None
        job['started_at'] = start
        job['duration'] = duration
        job['data_name'] = job["data_store"]['name'] if job["data_store"] is not None else None

    if tail is not None or head is not None:
        target_job_list = []
        if tail:
            target_job_list.extend(jobs[:tail])
            target_job_list.append(["..."])
        if head:
            target_job_list.append(["..."])
            target_job_list.extend(jobs[-head:]) 
    else:
        target_job_list = jobs

    typer.echo(job_formatter.render(target_job_list))


@app.command("stop", help="Stop running job.")
def stop(
    job_id: int = typer.Option(
        ...,
        "--job-id",
        "-i",
        help="ID of job to stop"
    )
):
    client: JobClientService = build_client(ServiceType.JOB)
    job_status = client.get_job(job_id)["status"]

    if job_status == "waiting":
        client.cancel_job(job_id)
    elif job_status in ("running", "enqueued"):
        client.terminate_job(job_id)
    else:
        secho_error_and_exit(f"No need to stop {job_status} job...")


@app.command("view", help="See the job detail.")
def view(
    job_id: int = typer.Option(
        ...,
        "--job-id",
        "-i",
        help="ID of job to view log"
    ),
):
    job_client: JobClientService = build_client(ServiceType.JOB)
    job_checkpoint_client: JobCheckpointClientService = build_client(ServiceType.JOB_CHECKPOINT, job_id=job_id)
    job_artifact_client: JobArtifactClientService = build_client(ServiceType.JOB_ARTIFACT, job_id=job_id)

    job = job_client.get_job(job_id)
    job_checkpoints = job_checkpoint_client.list_checkpoints()
    job_artifacts = job_artifact_client.list_artifacts() 

    started_at = job.get("started_at")
    finished_at = job.get("finished_at")
    if started_at is not None:
        start = datetime_to_pretty_str(parse(job["started_at"]))
    else:
        start = None
    if started_at is not None and finished_at is not None:
        duration = timedelta_to_pretty_str(parse(started_at), parse(finished_at))
    elif started_at is not None and job["status"] == "running":
        start_time = parse(started_at)
        curr_time = datetime.now(start_time.tzinfo)
        duration = timedelta_to_pretty_str(start_time, curr_time)
    else:
        duration = None

    job['started_at'] = start
    job['duration'] = duration
    job['data_name'] = job["data_store"]['name'] if job["data_store"] is not None else None

    checkpoint_list = []
    for checkpoint in reversed(job_checkpoints):
        checkpoint_list.append(
            [
                checkpoint["id"],
                checkpoint["vendor"],
                checkpoint["region"],
                checkpoint["iteration"],
                datetime_to_pretty_str(parse(checkpoint["created_at"]), long_list=True),
            ]
        )

    artifact_list = []
    for artifact in reversed(job_artifacts):
        artifact_list.append(
            [
                artifact["id"],
                artifact["name"],
                artifact["path"],
                artifact["mtime"],
                artifact["mime_type"]
            ]
        )

    typer.echo(
        "OVERVIEW\n\n" + \
        job_formatter.render([job], show_detail=True, in_list=True) + \
        "\n\nCHECKPOINTS\n\n" + \
        ckpt_formatter.render(checkpoint_list) + \
        "\n\nARTIFACTS\n\n" + \
        artifact_formatter.render(artifact_list)
    )


@template_app.command("create")
def template_create(
    save_path: typer.FileTextWrite = typer.Option(
        ...,
        "--save-path",
        "-s",
        help="Path to save job YAML configruation file."
    )
):
    job_type = typer.prompt(
        "What kind job do you want?\n",
        type=Choice([ e.value for e in JobType ]),
        prompt_suffix="\n>> "
    )
    configurator = build_job_configurator(job_type)
    yaml_str = configurator.render()

    yaml = ruamel.yaml.YAML()
    code = yaml.load(yaml_str)
    yaml.dump(code, save_path)

    continue_edit = typer.confirm(
        f"Do you want to open editor to configure the job YAML file? (default editor: {get_default_editor()})",
        prompt_suffix="\n>> "
    )
    if continue_edit:
        open_editor(save_path.name)


def _split_log_types(value: Optional[str]) -> Optional[List[LogType]]:
    if value is None:
        return value
    return [ x.lower() for x in value.split(",") ]


def _split_machine_ids(value: Optional[str]) -> Optional[List[int]]:
    if value is None:
        return value
    try:
        return [ int(machine_id) for machine_id in value.split(",") ]
    except ValueError:
        secho_error_and_exit("Machine index should be integer. (e.g., --machine 0,1,2)")


async def monitor_logs(job_id: int,
                       log_types: Optional[List[str]],
                       machines: Optional[List[str]],
                       show_time: bool,
                       show_machine_id: bool):
    ws_client: JobWebSocketClientService = build_client(ServiceType.JOB_WS)

    async with ws_client.open_connection(job_id, log_types, machines):
        async for response in ws_client:
            log_list = []
            if show_time:
                log_list.append(f"⏰ {datetime_to_simple_string(utc_to_local(parser.parse(response['timestamp'])))}")
            if show_machine_id:
                node_rank = response['node_rank']
                if node_rank == -1:
                    node_rank = "sys"
                log_list.append(f"💻 #{node_rank}")
            log_list.append(
                "\n".join(
                    textwrap.wrap(
                        response['content'],
                        width=get_remaining_terminal_columns(
                            len(tabulate.tabulate([log_list], tablefmt='plain')) + 5
                        ),
                        break_long_words=False,
                        replace_whitespace=False
                    )
                )
            )
            typer.echo(
                tabulate.tabulate(
                    [log_list],
                    tablefmt='plain'
                )
            )


# TODO: Implement since/until if necessary
@log_app.command("view", help="Watch the job logs.")
def log_view(
    job_id: int = typer.Option(
        ...,
        "--job-id",
        "-i",
        help="ID of job to view log"
    ),
    num_records: int = typer.Option(
        100,
        "--num-records",
        "-n",
        help="The number of recent records to view"
    ),
    content: Optional[str] = typer.Option(
        None,
        "--content",
        "-c",
        help="Filter logs by content"
    ),
    log_types: LogType = typer.Option(
        None,
        "--log-type",
        "-l",
        callback=_split_log_types,
        help="Filter logs by type. Comma-separated string of 'stdout', 'stderr' and 'vmlog'. "
             "By default, it will print logs for all types"
    ),
    machines: str = typer.Option(
        None,
        "--machine",
        "-m",
        callback=_split_machine_ids,
        help="Filter logs by machine ID. Comma-separated indices of machine to print logs (e.g., 0,1,2,3). "
             "By default, it will print logs from all machines."
    ),
    head: bool = typer.Option(
        False,
        "--head",
        "-h",
        help="View logs from the oldest one"
    ),
    export_path: Optional[Path] = typer.Option(
        None,
        "--export-path",
        "-e",
        help="Path to export logs"
    ),
    follow: bool = typer.Option(
        False,
        "--follow",
        "-f",
        help="Follow logs"
    ),
    show_time: bool = typer.Option(
        False,
        "--show-time",
        help="Print logs with timestamp"
    ),
    show_machine_id: bool = typer.Option(
        False,
        "--show-machine-id",
        help="Print logs with machine index"
    )
):
    if num_records <= 0 or num_records > 10000:
        secho_error_and_exit("'num_records' should be a positive integer, equal or smaller than 10000")

    if head and follow:
        secho_error_and_exit("'follow' cannot be set in 'head' mode")

    if export_path is not None and follow:
        secho_error_and_exit("'follow' cannot be set when 'export_path' is given")

    client: JobClientService = build_client(ServiceType.JOB)
    logs = client.get_text_logs(job_id, num_records, head, log_types, machines, content)

    if export_path is not None:
        with export_path.open("w") as export_file:
            for record in logs:
                log_list = []
                if show_time:
                    log_list.append(
                        f"⏰ {datetime_to_simple_string(utc_to_local(parser.parse(record['timestamp'])))}"
                    )
                if show_machine_id:
                    node_rank = record['node_rank']
                    if node_rank == -1:
                        node_rank = "sys"
                    log_list.append(f"💻 #{node_rank}")
                log_list.append(record['content'])
                export_file.write(
                    tabulate.tabulate(
                        [ log_list ],
                        tablefmt='plain'
                    ) + "\n"
                )
    else:
        for record in logs:
            log_list = []
            if show_time:
                log_list.append(
                    f"⏰ {datetime_to_simple_string(utc_to_local(parser.parse(record['timestamp'])))}"
                )
            if show_machine_id:
                node_rank = record['node_rank']
                if node_rank == -1:
                    node_rank = "sys"
                log_list.append(f"💻 #{node_rank}")
            log_list.append(
                "\n".join(
                    textwrap.wrap(
                        record['content'],
                        width=get_remaining_terminal_columns(
                            len(tabulate.tabulate([log_list], tablefmt='plain')) + 5
                        ),
                        break_long_words=False,
                        replace_whitespace=False
                    )
                )
            )
            typer.echo(
                tabulate.tabulate([log_list], tablefmt='plain')
            )

    if follow:
        try:
            # Subscribe job log
            asyncio.run(
                monitor_logs(job_id, log_types, machines, show_time, show_machine_id)
            )
        except KeyboardInterrupt:
            secho_error_and_exit(f"Keyboard Interrupt...", color=typer.colors.MAGENTA)


if __name__ == '__main__':
    app()
