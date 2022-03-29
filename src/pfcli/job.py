"""PeriFlow Job
"""

import asyncio
import json
import textwrap
from enum import Enum
from pathlib import Path
from typing import Optional, List
from dateutil import parser
from dateutil.parser import parse
from datetime import datetime

import tabulate
import typer
import yaml
import websockets
import ruamel.yaml
from requests import HTTPError

from pfcli import autoauth
from pfcli.utils import (
    get_default_editor,
    get_remaining_terminal_columns,
    get_uri,
    get_wss_uri,
    open_editor,
    secho_error_and_exit,
    get_group_id,
    datetime_to_pretty_str,
    timedelta_to_pretty_str,
    utc_to_local,
    datetime_to_simple_string,
    zip_dir
)

app = typer.Typer()
template_app = typer.Typer()
log_app = typer.Typer()

app.add_typer(template_app, name="template", help="Manager job templates.")
app.add_typer(log_app, name="log", help="Manage job logs.")


def lint_config(config: dict) -> None:
    assert "vm" in config
    assert "num_devices" in config
    assert "experiment" in config


def refine_config(config: dict) -> None:
    experiment_name = config["experiment"]
    experiment_id = infer_experiment_id_from_name(experiment_name)
    del config["experiment"]
    config["experiment"] = {"id": experiment_id}

    vm_name = config["vm"]
    vm_config_id = infer_vm_config_id_from_name(vm_name)
    del config["vm"]
    config["vm_config_id"] = vm_config_id

    if "data" in config:
        data_name = config["data"]["name"]
        data_id = infer_data_id_from_name(data_name)
        del config["data"]["name"]
        config["data"]["id"] = data_id

    if config["job_setting"]["type"] == "custom":
        config["job_setting"]["launch_mode"] = "node"
    else:
        job_template_name = config["job_setting"]["template_name"]
        job_template_config = infer_job_template_config_from_name(job_template_name)
        del config["job_setting"]["template_name"]
        config["job_setting"]["engine_code"] = job_template_config["engine_code"]
        config["job_setting"]["model_code"] = job_template_config["model_code"]
        config["job_setting"]["model_config"] = {}


def infer_job_template_config_from_name(name: str) -> dict:
    r = autoauth.get(get_uri(f"job_template/"))
    try:
        r.raise_for_status()
    except HTTPError:
        secho_error_and_exit(f"Listing failed!")

    for template in r.json():
        if template["name"] == name:
            return template

    secho_error_and_exit(f"Predefined job template with name ({name}) is not found.")


def infer_vm_config_id_from_name(name: str) -> int:
    group_id = get_group_id()
    r = autoauth.get(get_uri(f"group/{group_id}/vm_config/"))
    try:
        r.raise_for_status()
    except HTTPError:
        secho_error_and_exit("Failed to get VM configs.")
    vm_configs = r.json()
    for vm_config in vm_configs:
        if name == vm_config["vm_config_type"]["vm_instance_type"]["code"]:
            return vm_config["id"]

    secho_error_and_exit(f"VM with name ({name}) is not supported.")


def infer_data_id_from_name(name: str) -> int:
    group_id = get_group_id()
    r = autoauth.get(get_uri(f"group/{group_id}/datastore/"))
    try:
        r.raise_for_status()
    except HTTPError:
        secho_error_and_exit("Failed to get Datastores.")
    datastores = r.json()
    for datastore in datastores:
        if name == datastore["name"]:
            return datastore["id"]

    secho_error_and_exit(f"Datastore with name ({name}) is not found.")


def infer_experiment_id_from_name(name: str) -> int:
    group_id = get_group_id()
    r = autoauth.get(get_uri(f"group/{group_id}/experiment/"))
    try:
        r.raise_for_status()
    except HTTPError:
        secho_error_and_exit("Failed to get experiments.")
    experiments = r.json()
    for experiment in experiments:
        if name == experiment["name"]:
            return experiment["id"]

    create_new = typer.confirm(
        f"Experiment with the name ({name}) is not found.\n"
        "Do you want to proceed with creating a new experiment? "
        f"If yes, a new experiment will be created with the name: {name}"
    )
    if not create_new:
        typer.echo("Please run the job after creating an experiment first.")
        raise typer.Abort()

    experiment_data = create_experiment(name)
    typer.echo(f"A new experiment ({name}) is created.")
    return experiment_data["id"]


def create_experiment(name: str) -> dict:
    group_id = get_group_id()
    r = autoauth.post(get_uri(f"group/{group_id}/experiment/"), data={"name": name})
    try:
        r.raise_for_status()
    except HTTPError:
        secho_error_and_exit("Failed to create experiment.")

    return r.json()


@app.command("run", help="Run a new job.")
def run(
    config_file: typer.FileText = typer.Option(
        ...,
        "--config-file",
        "-f",
        help="Path to configuration file"
    ),
    training_dir: Optional[Path] = typer.Option(
        None,
        "--training-dir",
        "-d",
        help="Path to training workspace directory"
    )
):
    try:
        config: dict = yaml.safe_load(config_file)
    except yaml.YAMLError as e:
        secho_error_and_exit(f"Error occurred while parsing config file... {e}")

    lint_config(config)
    refine_config(config)

    if training_dir is not None:
        if not training_dir.exists():
            secho_error_and_exit(f"Specified workspace does not exist...")
        if not training_dir.is_dir():
            secho_error_and_exit(f"Specified workspace is not directory...")
        workspace_zip = Path(training_dir.parent / (training_dir.name + ".zip"))
        with zip_dir(training_dir, workspace_zip) as zip_file:
            files = {'workspace_zip': ('workspace.zip', zip_file)}
            r = autoauth.post(get_uri("job/"), data={"data": json.dumps(config)}, files=files)
    else:
        r = autoauth.post(get_uri("job/"), json=config)
        typer.echo(r.request.body)
    try:
        r.raise_for_status()
        headers = ["id", "workspace_signature", "workspace_id"]
        typer.echo(
            tabulate.tabulate(
                [
                    [
                        r.json()["id"],
                        r.json()["workspace_signature"],
                        r.json()["workspace_id"]
                    ]
                ],
                headers=headers
            )
        )
    except HTTPError:
        secho_error_and_exit(f"Failed to run the specified job!")


@app.command("list", help="List jobs.")
def list(
    long_list: bool = typer.Option(
        False,
        "--long-list",
        "-l",
        help="View job list with detail"
    ),
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
    group_id = get_group_id()
    job_list = []
    request_data = {}
    request_data.update({"group_id": group_id})
    request_url = f"group/{group_id}/job" if show_group_job else "job/"
    r = autoauth.get(get_uri(request_url), params=request_data)
    try:
        r.raise_for_status()
    except HTTPError:
        secho_error_and_exit(f"Job listing failed!")
    for job in r.json()["results"]:
        started_at = job.get("started_at")
        finished_at = job.get("finished_at")
        if started_at is not None:
            start = datetime_to_pretty_str(parse(job["started_at"]), long_list=long_list)
        else:
            start = None
        if started_at is not None and finished_at is not None:
            duration = timedelta_to_pretty_str(parse(started_at), parse(finished_at), long_list=long_list)
        elif started_at is not None and job["status"] == "running":
            start_time = parse(started_at)
            curr_time = datetime.now(start_time.tzinfo)
            duration = timedelta_to_pretty_str(start_time, curr_time, long_list=long_list)
        else:
            duration = None
        job_list.append(
            [
                job["id"],
                job["status"],
                job["vm_config"]["vm_config_type"]["name"],
                job["vm_config"]["vm_config_type"]["vm_instance_type"]["device_type"],
                job["num_desired_devices"],
                job["data_store"]['storage_name'] if job["data_store"] is not None else None,
                start,
                duration
            ]
        )
    if tail is not None or head is not None:
        target_job_list = []
        if tail:
            target_job_list.extend(job_list[:tail])
            target_job_list.append(["..."])
        if head:
            target_job_list.append(["..."])
            target_job_list.extend(job_list[-head:]) 
    else:
        target_job_list = job_list
    typer.echo(
        tabulate.tabulate(
            target_job_list,
            headers=["id", "status", "vm_name", "device", "# devices", "data_name", "start", "duration"]
        )
    )


@app.command("stop", help="Stop running job.")
def stop(
    job_id: int = typer.Option(
        ...,
        "--job-id",
        "-i",
        help="ID of job to stop"
    )
):
    r = autoauth.get(get_uri(f"job/{job_id}/"))
    try:
        r.raise_for_status()
    except HTTPError:
        secho_error_and_exit(f"Cannot fetch job info...")
    job_status = r.json()["status"]
    if job_status == "waiting":
        r = autoauth.post(get_uri(f"job/{job_id}/cancel/"))
        try:
            r.raise_for_status()
            typer.echo("Job is now cancelling...")
        except HTTPError:
            # TODO: Handle synchronization error if necessary...
            secho_error_and_exit(f"Job stop failed!")
    elif job_status == "running" or job_status == "enqueued":
        r = autoauth.post(get_uri(f"job/{job_id}/terminate/"))
        try:
            r.raise_for_status()
            typer.echo("Job is now terminating...")
        except HTTPError:
            secho_error_and_exit(f"Job stop failed!")
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
    long_list: bool = typer.Option(
        False,
        "--long-list",
        "-l",
        help="View job info with detail"
    )
):
    job_r = autoauth.get(get_uri(f"job/{job_id}/"))
    job_checkpoint_r = autoauth.get(get_uri(f"job/{job_id}/checkpoint/"))
    job_artifact_r = autoauth.get(get_uri(f"job/{job_id}/artifact/"))
    try:
        job_r.raise_for_status()
        job_checkpoint_r.raise_for_status()
        job_artifact_r.raise_for_status()
        job = job_r.json()
        job_checkpoints = json.loads(job_checkpoint_r.content)
        job_artifacts = json.loads(job_artifact_r.content)

        started_at = job.get("started_at")
        finished_at = job.get("finished_at")
        if started_at is not None:
            start = datetime_to_pretty_str(parse(job["started_at"]), long_list=long_list)
        else:
            start = None
        if started_at is not None and finished_at is not None:
            duration = timedelta_to_pretty_str(parse(started_at), parse(finished_at), long_list=long_list)
        elif started_at is not None and job["status"] == "running":
            start_time = parse(started_at)
            curr_time = datetime.now(start_time.tzinfo)
            duration = timedelta_to_pretty_str(start_time, curr_time, long_list=long_list)
        else:
            duration = None

        job_list = [
            [
                job["id"],
                job["status"],
                job["vm_config"]["vm_config_type"]["name"],
                job["vm_config"]["vm_config_type"]["vm_instance_type"]["device_type"],
                job["num_desired_devices"],
                job["data_store"]['storage_name'] if job["data_store"] is not None else None,
                start,
                duration,
                job["error_message"]
            ]
        ]

        checkpoint_list = []
        for checkpoint in reversed(job_checkpoints):
            checkpoint_list.append(
                [
                    checkpoint["id"],
                    checkpoint["vendor"],
                    checkpoint["region"],
                    checkpoint["iteration"],
                    datetime_to_pretty_str(parse(checkpoint["created_at"]), long_list=long_list),
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
            tabulate.tabulate(
                job_list,
                headers=["id", "status", "vm_name", "device", "# devices", "datastore", "start", "duration", "err_msg"]
            ) + \
            "\n\nCHECKPOINTS\n\n" + \
            tabulate.tabulate(
                checkpoint_list,
                headers=["id", "vendor", "region", "iteration", "created_at"]
            ) + \
            "\n\nARTIFACTS\n\n" + \
            tabulate.tabulate(
                artifact_list,
                headers=["id", "name", "path", "mtime", "type"]
            )
        )
    except HTTPError:
        secho_error_and_exit(f"View failed!")


DEFAULT_TEMPLATE_CONFIG = """\
# The name of experiment
experiment:

# The name of job
name:

# The name of vm type
vm:
"""

DATA_CONFIG_WO_MOUNT_PATH= """
# Configure dataset
data:
  # The name of dataset
  name:
"""

DATA_CONFIG_W_MOUNT_PATH = DATA_CONFIG_WO_MOUNT_PATH + """
  # Path to mount your dataset volume
  mount_path:
"""


@template_app.command("create")
def template_create(
    save_path: typer.FileTextWrite = typer.Option(
        ...,
        "--save-path",
        "-s",
        help="Path to save job YAML configruation file."
    )
):
    yaml_str = DEFAULT_TEMPLATE_CONFIG

    job_type = typer.prompt(
        "What kind job do you want?\n",
        "Options: 'predefined', 'custom'"
    )
    if job_type == "custom":
        use_private_img: typer.confirm(
            "Will you use your private docker image? (You should provide credential)."
        )
        use_dist: typer.confirm(
            "Will you run distributed training job?"
        )
    elif job_type == "predefined":
        job_template_name = typer.prompt(
            "Which job do you want to run? Choose one in the following catalog:\n",
            f"Options: {', '.join(list_template_names())}"
        )
    else:
        secho_error_and_exit("Invalid job type...!")
    use_data = typer.confirm(
        "Will you use dataset for the job?"
    )
    if use_data:
        if job_type == "custom":
            yaml_str += DATA_CONFIG_W_MOUNT_PATH
        else:
            yaml_str += DATA_CONFIG_WO_MOUNT_PATH

    use_input_checkpoint = typer.confirm(
        "Will you use input checkpoint for the job?"
    )
    use_output_checkpoint = typer.confirm(
        "Does your job generate model checkpoint file?"
    )
    use_wandb_credential = typer.confirm(
        "Will you use W&B monitoring for the job?"
    )
    use_slack_credential = typer.confirm(
        "Do you want to get slack notifaction for the job?"
    )
    # TODO: Support workspace, artifact

    yaml = ruamel.yaml.YAML()
    code = yaml.load(yaml_str)
    yaml.dump(code, save_path)

    continue_edit = typer.confirm(
        f"Do you want to open editor to configure the job YAML file? (default editor: {get_default_editor()})"
    )
    if continue_edit:
        open_editor(save_path.name)


def list_template_names() -> List[str]:
    r = autoauth.get(get_uri(f"job_template/"))
    try:
        r.raise_for_status()
    except HTTPError:
        secho_error_and_exit(f"Failed to get job templates.")

    return [ template["name"] for template in r.json() ]


@template_app.command("list")
def template_list():
    r = autoauth.get(get_uri(f"job_template/"))
    try:
        r.raise_for_status()
    except HTTPError:
        secho_error_and_exit(f"Listing failed!")

    template_list = []
    for template in r.json():
        result = {
            "job_setting": {
                "type": "predefined",
                "model_code": template["model_code"],
                "engine_code": template["engine_code"],
                "model_config": template["data_example"]
            }
        }
        template_list.append((template["name"], yaml.dump(result, sort_keys=False, indent=2)))
    typer.echo(
        tabulate.tabulate(
            template_list,
            headers=["name", "model_config"],
            tablefmt="grid"
        )
    )


@template_app.command("get")
def template_get(
    template_name: str = typer.Option(
        ...,
        "--template-name",
        "-n",
        help="Name of job template"
    ),
    download_file: Optional[typer.FileTextWrite] = typer.Option(
        None,
        "--download-file",
        "-f",
        help="Path to save job template YAML configuration file"
    )
):
    r = autoauth.get(get_uri("job_template/"))
    try:
        r.raise_for_status()
    except HTTPError:
        secho_error_and_exit(f"Get failed!")

    try:
        chosen = next(template for template in r.json() if template['name'] == template_name)
    except:
        typer.echo("\nNo matching template found! :(\n")
        return
    result = {
        "job_setting": {
            "type": "predefined",
            "model_code": chosen["model_code"],
            "engine_code": chosen["engine_code"],
            "model_config": chosen["data_example"]
        }
    }
    result_yaml = yaml.dump(result, sort_keys=False, indent=4)
    if download_file is not None:
        download_file.write(result_yaml)
        typer.echo("\nTemplate File Download Success!\n")
    else:
        typer.echo(result_yaml)


class LogType(str, Enum):
    STDOUT = "stdout"
    STDERR = "stderr"
    VMLOG = "vmlog"


async def _subscribe(websocket: websockets.WebSocketClientProtocol,
                     sources: List[str],
                     node_ranks: List[int]):
    subscribe_json = {
        "type": "subscribe",
        "sources": sources,
        "node_ranks": node_ranks
    }
    await websocket.send(json.dumps(subscribe_json))
    r = await websocket.recv()
    decoded_r = json.loads(r)
    try:
        assert decoded_r.get("response_type") == "subscribe" and \
            set(sources) == set(decoded_r["sources"])
    except json.JSONDecodeError:
        secho_error_and_exit("Error occurred while decoding websocket response...")
    except AssertionError:
        secho_error_and_exit(f"Invalid websocket response... {r}")


async def _consume_and_print_logs(websocket: websockets.WebSocketClientProtocol,
                                  show_time: bool = False,
                                  show_machine_id: bool = False):
    try:
        while True:
            r = await websocket.recv()
            decoded_response = json.loads(r)
            assert "content" in decoded_response
            log_list = []
            if show_time:
                log_list.append(f"⏰ {datetime_to_simple_string(utc_to_local(parser.parse(decoded_response['timestamp'])))}")
            if show_machine_id:
                node_rank = decoded_response['node_rank']
                if node_rank == -1:
                    node_rank = "vm"
                log_list.append(f"💻 #{node_rank}")
            log_list.append(
                "\n".join(
                    textwrap.wrap(
                        decoded_response['content'],
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
    except json.JSONDecodeError:
        secho_error_and_exit(f"Error occurred while decoding websocket response...")
    except AssertionError:
        secho_error_and_exit(f"Response format error...")


async def _monitor_job_logs_via_ws(uri: str,
                                   log_types: Optional[str],
                                   machines: Optional[str],
                                   show_time: bool = False,
                                   show_machine_id: bool = False):
    if log_types is None:
        sources = [f"process.{x.value}" for x in LogType]
    else:
        sources = [f"process.{log_type}" for log_type in log_types]

    if machines is None:
        node_ranks = []
    else:
        node_ranks = machines

    async with autoauth.connect_with_auth(uri) as conn:
        await _subscribe(conn, sources, node_ranks)
        await _consume_and_print_logs(conn, show_time, show_machine_id)


def validate_log_types(value: Optional[str]) -> Optional[List[LogType]]:
    if value is None:
        return value
    log_types = [ x.lower() for x in value.split(",") ]
    if not all(x in set(LogType) for x in log_types):
        secho_error_and_exit("Log type should be one of 'stdout', 'stderr' and 'vmlog'.")
    return log_types


def validate_machine_ids(value: Optional[str]) -> Optional[List[int]]:
    if value is None:
        return value
    try:
        return [ int(machine_id) for machine_id in value.split(",") ]
    except ValueError as exc:
        secho_error_and_exit("Machine index should be integer. (e.g., --machine 0,1,2)")


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
    log_types: str = typer.Option(
        None,
        "--log-type",
        "-l",
        callback=validate_log_types,
        help="Filter logs by type. Comma-separated string of 'stdout', 'stderr' and 'vmlog'. "
             "By default, it will print logs for all types"
    ),
    machines: str = typer.Option(
        None,
        "--machine",
        "-m",
        callback=validate_machine_ids,
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

    request_data = dict()
    if head:
        request_data['ascending'] = 'true'
    else:
        request_data['ascending'] = 'false'
    request_data['limit'] = num_records
    if content is not None:
        request_data['content'] = content
    if log_types is not None:
        request_data['log_types'] = ",".join(log_types)
    if machines is not None:
        request_data['node_ranks'] = ",".join([str(machine) for machine in machines])
    init_r = autoauth.get(get_uri(f"job/{job_id}/text_log/"), params=request_data)
    fetched_lines = 0

    try:
        init_r.raise_for_status()
        logs = init_r.json()['results']
        fetched_lines += len(logs)
        if not head:
            logs.reverse()
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
                            node_rank = "vm"
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
                    log_list.append(f"⏰ {datetime_to_simple_string(utc_to_local(parser.parse(record['timestamp'])))}")
                if show_machine_id:
                    node_rank = record['node_rank']
                    if node_rank == -1:
                        node_rank = "vm"
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
                    tabulate.tabulate(
                        [log_list],
                        tablefmt='plain'
                    )
                )

        if follow:
            try:
                uri = f"job/{job_id}/"
                # Subscribe job log
                asyncio.run(
                    _monitor_job_logs_via_ws(get_wss_uri(uri), log_types, machines, show_time, show_machine_id)
                )
            except KeyboardInterrupt:
                secho_error_and_exit(f"Keyboard Interrupt...", color=typer.colors.MAGENTA)
    except HTTPError:
        secho_error_and_exit(f"Log fetching failed!")


if __name__ == '__main__':
    app()
