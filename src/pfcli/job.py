"""PeriFlow Job
"""

import os
import asyncio
import json
import zipfile
import textwrap
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Optional, List
from dateutil import parser

import tabulate
import typer
import yaml
import websockets
from dateutil.parser import parse
from requests import HTTPError

from pfcli import autoauth
from pfcli.errors import InvalidParamError
from pfcli.utils import (
    get_remaining_terminal_columns,
    get_uri,
    get_wss_uri,
    secho_error_and_exit,
    get_group_id,
    datetime_to_pretty_str,
    timedelta_to_pretty_str
)

app = typer.Typer()
template_app = typer.Typer()
log_app = typer.Typer()

app.add_typer(template_app, name="template")
app.add_typer(log_app, name="log")


@contextmanager
def _zip_dir(dir_path: Path, zip_path: Path):
    typer.secho("Compressing training directory...", fg=typer.colors.MAGENTA)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as zip_file:
        for e in dir_path.rglob("*"):
            zip_file.write(e, e.relative_to(dir_path.parent))
    typer.secho("Compressing finished... Now uploading...", fg=typer.colors.MAGENTA)
    yield zip_path.open("rb")
    zip_path.unlink()


@app.command()
def run(
    vm_config_id: int = typer.Option(
        ...,
        "--vm-config-id",
        "-v",
        help="ID of VM Config"
    ),
    config_file: typer.FileText = typer.Option(
        ...,
        "--config-file",
        "-f",
        help="Path to configuration file"
    ),
    num_devices: int = typer.Option(
        1,
        "--num-devices",
        "-n",
        help="The number of devices to use"
    ),
    training_dir: Optional[Path] = typer.Option(
        None,
        "--training-dir",
        "-d",
        help="Path to training workspace directory"
    )
):
    # TODO: Support template.
    # TODO: Support datastore.
    request_data = {
        "vm_config_id": vm_config_id,
        "num_devices": num_devices
    }

    try:
        config: dict = yaml.safe_load(config_file)
    except yaml.YAMLError as e:
        secho_error_and_exit(f"Error occurred while parsing config file... {e}")

    for k, v in config.items():
        request_data.update({k: v})
    if training_dir is not None:
        if not training_dir.exists():
            secho_error_and_exit(f"Specified workspace does not exist...")
        if not training_dir.is_dir():
            secho_error_and_exit(f"Specified workspace is not directory...")
        workspace_zip = Path(training_dir.parent / (training_dir.name + ".zip"))
        with _zip_dir(training_dir, workspace_zip) as zip_file:
            files = {'workspace_zip': ('workspace.zip', zip_file)}
            r = autoauth.post(get_uri("job/"), data={"data": json.dumps(request_data)}, files=files)
    else:
        r = autoauth.post(get_uri("job/"), json=request_data)
        typer.echo(r.request.body)
    try:
        r.raise_for_status()
        headers = ["id", "workspace_signature", "workspace_id"]
        typer.echo(tabulate.tabulate([[r.json()["id"],
                                       r.json()["workspace_signature"],
                                       r.json()["workspace_id"]]], headers=headers))
    except HTTPError:
        secho_error_and_exit(f"Failed to run the specified job!")


@app.command()
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
        if job.get("started_at") is not None:
            start = datetime_to_pretty_str(parse(job["started_at"]), long_list=long_list)
        else:
            start = None
        if job.get("started_at") is not None and job.get("finished_at") is not None:
            duration = timedelta_to_pretty_str(parse(job["started_at"]), parse(job["finished_at"]), long_list=long_list)
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


@app.command()
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


@app.command()
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
    r = autoauth.get(get_uri(f"job/{job_id}/"))
    try:
        r.raise_for_status()
        job = r.json()
        if job.get("started_at") is not None:
            start = datetime_to_pretty_str(parse(job["started_at"]), long_list=long_list)
        else:
            start = None
        if job.get("started_at") is not None and job.get("finished_at") is not None:
            duration = timedelta_to_pretty_str(parse(job["started_at"]), parse(job["finished_at"]), long_list=long_list)
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
        typer.echo(
            tabulate.tabulate(
                job_list,
                headers=[
                    "id", "status", "vm_name", "device", "# devices", "datastore", "start", "duration", "err_msg"
                ]
            )
        )
    except HTTPError:
        secho_error_and_exit(f"View failed!")


@template_app.command("list")
def template_list():
    r = autoauth.get(get_uri(f"job_template/"))
    try:
        r.raise_for_status()
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
                headers=["id", "status", "vm_name", "device", "# devices", "data_name", "start", "duration"],
                tablefmt="grid"
            )
        )
    except HTTPError:
        secho_error_and_exit(f"Listing failed!")


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
    except HTTPError:
        secho_error_and_exit(f"Get failed!")


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
                log_list.append(f"⏰ {parser.parse(decoded_response['timestamp'])}")
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
                            len(tabulate.tabulate([log_list], tablefmt='plain')) + 3
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
    log_types = value.split(",")
    for log_type in log_types:
        assert log_type in [ e for e in LogType ]
    return log_types


def validate_machine_ids(value: Optional[str]) -> Optional[List[int]]:
    if value is None:
        return value
    machine_ids = value.split(",")
    try:
        return [int(machine_id) for machine_id in machine_ids]
    except ValueError as exc:
        raise InvalidParamError("Machine index should be integer. (e.g., --machine 0,1,2)") from exc


# TODO: Implement since/until if necessary
@log_app.command("view")
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
                        log_list.append(f"⏰ {parser.parse(record['timestamp'])}")
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
                    log_list.append(f"⏰ {parser.parse(record['timestamp'])}")
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
                                len(tabulate.tabulate([log_list], tablefmt='plain')) + 3
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
