from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Optional, List
import asyncio
import json
import zipfile

from requests import HTTPError
import tabulate
import typer
import yaml
from dateutil.parser import parse
import websockets

from pfcli import autoauth
from pfcli.utils import (get_uri,
                         get_wss_uri,
                         secho_error_and_exit,
                         get_group_id,
                         datetime_to_pretty_str,
                         timedelta_to_pretty_str)

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
def run(vm_config_id: int = typer.Option(...),
        config_file: typer.FileText = typer.Option(...),
        num_devices: int = typer.Option(1),
        training_dir: Optional[Path] = typer.Option(None)):
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
        secho_error_and_exit(f"Failed to run the specified job! Error Code = {r.status_code} Detail = {r.text}")


@app.command()
def list(long_list: bool = typer.Option(False, "--long-list", "-l")):
    group_id = get_group_id()
    # TODO: Solve discrepancy in "limit"
    job_list = []
    request_data = {}
    request_data.update({"group_id": group_id})
    r = autoauth.get(get_uri("job/"), params=request_data)
    try:
        r.raise_for_status()
    except HTTPError:
        secho_error_and_exit(f"List failed! Error Code = {r.status_code}, Detail = {r.text}")
    for job in r.json()["results"]:
        if job.get("started_at") is not None:
            start = datetime_to_pretty_str(parse(job["started_at"]), long_list=long_list)
        else:
            start = None
        if job.get("started_at") is not None and job.get("finished_at") is not None:
            duration = timedelta_to_pretty_str(parse(job["started_at"]), parse(job["finished_at"]), long_list=long_list)
        else:
            duration = None
        job_list.append([job["id"],
                         job["status"],
                         job["vm_config"]["vm_config_type"]["name"],
                         job["vm_config"]["vm_config_type"]["vm_instance_type"]["device_type"],
                         job["num_desired_devices"],
                         job["data_store"]['storage_name'] if job["data_store"] is not None else None,
                         start,
                         duration])
    typer.echo(tabulate.tabulate(
        job_list,
        headers=["id", "status", "vm_name", "device", "# devices", "data_name", "start", "duration"]))


@app.command()
def stop(job_id: int = typer.Option(...)):
    r = autoauth.get(get_uri(f"job/{job_id}/"))
    try:
        r.raise_for_status()
    except HTTPError:
        secho_error_and_exit(f"Cannot fetch job info... Error Code = {r.status_code}, Detail = {r.text}")
    job_status = r.json()["status"]
    if job_status == "waiting":
        r = autoauth.post(get_uri(f"job/{job_id}/cancel/"))
        try:
            r.raise_for_status()
            typer.echo("Job is now cancelling...")
        except HTTPError:
            # TODO: Handle synchronization error if necessary...
            secho_error_and_exit(f"Job stop failed! Error Code = {r.status_code}, Detail = {r.text}")
    elif job_status == "running" or job_status == "enqueued":
        r = autoauth.post(get_uri(f"job/{job_id}/terminate/"))
        try:
            r.raise_for_status()
            typer.echo("Job is now terminating...")
        except HTTPError:
            secho_error_and_exit(f"Job stop failed! Error Code = {r.status_code}, Detail = {r.text}")
    else:
        secho_error_and_exit(f"No need to stop {job_status} job...")


@app.command()
def view(job_id: int = typer.Option(...),
         long_list: bool = typer.Option(False, "--long-list", "-l")):
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
        job_list = [[job["id"],
                     job["status"],
                     job["vm_config"]["vm_config_type"]["name"],
                     job["vm_config"]["vm_config_type"]["vm_instance_type"]["device_type"],
                     job["num_desired_devices"],
                     job["data_store"]['storage_name'] if job["data_store"] is not None else None,
                     start,
                     duration,
                     job["error_message"]]]
        typer.echo(
            tabulate.tabulate(
                job_list,
                headers=["id", "status", "vm_name", "device", "# devices", "datastore",
                         "start", "duration", "err_msg"]))
    except HTTPError:
        secho_error_and_exit(f"View failed! Error Code = {r.status_code}, Detail = {r.text}")


@template_app.command("list")
def template_list():
    r = autoauth.get(get_uri(f"job_template/"))
    try:
        r.raise_for_status()
        # TODO: Elaborate
        typer.echo(json.dumps(r.json(), sort_keys=True, indent=2))
    except HTTPError:
        secho_error_and_exit(f"Listing failed! Error Code = {r.status_code}, Detail = {r.text}")


@template_app.command("view")
def template_view(template_id: int = typer.Option(...)):
    r = autoauth.get(get_uri(f"job_template/{template_id}/"))
    try:
        r.raise_for_status()
        # TODO: Elaborate
        typer.echo(r.text)
    except HTTPError:
        secho_error_and_exit(f"View failed! Error Code = {r.status_code}, Detail = {r.text}")


class LogType(Enum):
    STDOUT = "stdout"
    STDERR = "stderr"


async def _subscribe(websocket: websockets.WebSocketClientProtocol, sources: List[str]):
    subscribe_json = {
        "type": "subscribe",
        "sources": sources
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


async def _consume_and_print_logs(websocket: websockets.WebSocketClientProtocol):
    try:
        while True:
            r = await websocket.recv()
            decoded_response = json.loads(r)
            assert "content" in decoded_response
            typer.echo(f"[{decoded_response['timestamp']}] {decoded_response['content']}")
    except json.JSONDecodeError:
        secho_error_and_exit(f"Error occurred while decoding websocket response...")
    except AssertionError:
        secho_error_and_exit(f"Response format error...")


async def _monitor_job_logs_via_ws(uri: str, log_type: Optional[LogType]):
    if log_type is None:
        sources = [f"process.{x.value}" for x in LogType]
    else:
        sources = [f"process.{log_type.value}"]

    async with autoauth.connect_with_auth(uri) as conn:
        await _subscribe(conn, sources)
        await _consume_and_print_logs(conn)


# TODO: Implement since/until if necessary
@log_app.command("view")
def log_view(job_id: int = typer.Option(...),
             num_records: int = typer.Option(100),
             content: Optional[str] = typer.Option(None),
             log_type: Optional[LogType] = typer.Option(None),
             head: bool = typer.Option(False),
             export_path: Optional[Path] = typer.Option(None),
             follow: bool = typer.Option(False)
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
    if log_type is not None:
        request_data['log_type'] = log_type
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
                    export_file.write(f"[{record['timestamp']}] {record['content']}\n")
        else:
            for record in logs:
                typer.echo(f"[{record['timestamp']}] {record['content']}")
        # TODO: [PFT-162] Use WebSockets when following logs.
        if follow:
            try:
                uri = f"job/{job_id}/"
                # Subscribe job log
                asyncio.run(_monitor_job_logs_via_ws(get_wss_uri(uri), log_type))
            except KeyboardInterrupt:
                secho_error_and_exit(f"Keyboard Interrupt...", color=typer.colors.MAGENTA)
    except HTTPError:
        secho_error_and_exit(f"Log fetching failed! Error Code = {init_r.status_code}, Detail = {init_r.text}")


if __name__ == '__main__':
    app()
