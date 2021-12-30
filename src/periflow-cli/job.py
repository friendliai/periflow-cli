from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import json
import time
import zipfile

from requests import HTTPError
import tabulate
import typer
import yaml

import autoauth
from utils import get_uri, secho_error_and_exit, get_group_id

app = typer.Typer()
template_app = typer.Typer()
log_app = typer.Typer()

app.add_typer(template_app, name="template")
app.add_typer(log_app, name="log")


@contextmanager
def _zip_dir(dir_path: Path, zip_path: Path):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_LZMA) as zip_file:
        for e in dir_path.rglob("*"):
            zip_file.write(e, e.relative_to(dir_path.parent))
    yield zip_path.open("rb")
    zip_path.unlink()


@app.command()
def run(vm_config_id: int = typer.Option(...),
        config_file: typer.FileText = typer.Option(...),
        num_desired_devices: int = typer.Option(1),
        workspace_dir: Optional[Path] = typer.Option(None),
        data_store_name: Optional[str] = typer.Option(None)):
    # TODO: Support template.
    # TODO: Support datastore.
    request_data = {
        "vm_config_id": vm_config_id
    }

    try:
        config: dict = yaml.safe_load(config_file)
    except yaml.YAMLError as e:
        secho_error_and_exit(f"Error occurred while parsing config file... {e}")

    for k, v in config.items():
        request_data.update({k: v})
    if workspace_dir is not None:
        if not workspace_dir.exists():
            secho_error_and_exit(f"Specified workspace does not exist...")
        if not workspace_dir.is_dir():
            secho_error_and_exit(f"Specified workspace is not directory...")
        workspace_zip = Path(workspace_dir.parent / (workspace_dir.name + ".zip"))
        with _zip_dir(workspace_dir, workspace_zip) as zip_file:
            files = {'workspace_zip': ('workspace.zip', zip_file)}
            r = autoauth.post(get_uri("job/"), data={"data": json.dumps(request_data)}, files=files)
    else:
        r = autoauth.post(get_uri("job/"), data={"data": json.dumps(request_data)})
    try:
        r.raise_for_status()
        headers = ["id", "workspace_signature", "workspace_id"]
        typer.echo(tabulate.tabulate([[r.json()["id"],
                                       r.json()["workspace_signature"],
                                       r.json()["workspace_id"]]], headers=headers))
    except HTTPError:
        secho_error_and_exit(f"Failed to run the specified job! Error Code = {r.status_code} Detail = {r.text}")


@app.command()
def list():
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
        job_list.append([job["id"],
                         job["status"],
                         job["vm_config"]["vm_config_type"]["name"],
                         job["vm_config"]["vm_config_type"]["vm_instance_type"]["device_type"],
                         job["num_desired_devices"],
                         job["data_store"],
                         job["started_at"],
                         job["finished_at"]])
    typer.echo(tabulate.tabulate(
        job_list,
        headers=["id", "status", "vm_name", "device", "# devices", "datastore", "start", "end"]))


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
def view(job_id: int = typer.Option(...)):
    r = autoauth.get(get_uri(f"job/{job_id}/"))
    try:
        r.raise_for_status()
        job = r.json()
        job_list = [[job["id"],
                     job["status"],
                     job["vm_config"]["vm_config_type"]["name"],
                     job["vm_config"]["vm_config_type"]["vm_instance_type"]["device_type"],
                     job["num_desired_devices"],
                     job["data_store"],
                     job["started_at"],
                     job["finished_at"]]]
        typer.echo(
            tabulate.tabulate(
                job_list,
                headers=["id", "status", "vm_name", "device", "# devices", "datastore", "start", "end"]))
    except HTTPError:
        secho_error_and_exit(f"View failed! Error Code = {r.status_code}, Detail = {r.text}")


@template_app.command("list")
def template_list():
    r = autoauth.get(get_uri(f"job_template/"))
    try:
        r.raise_for_status()
        # TODO: Elaborate
        typer.echo(r.text)
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


# TODO: Implement since/until if necessary
@log_app.command("view")
def log_view(job_id: int = typer.Option(...),
             num_records: int = typer.Option(100),
             pattern: Optional[str] = typer.Option(None),
             log_type: Optional[str] = typer.Option(None),
             head: bool = typer.Option(False),
             export_path: Optional[Path] = typer.Option(None),
             follow: bool = typer.Option(False),
             interval: Optional[int] = typer.Option(1)
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
    if pattern is not None:
        request_data['content'] = pattern
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
                if len(logs) > 0:
                    last_timestamp_str = logs[-1]['timestamp']
                else:
                    last_timestamp_str = None
                while True:
                    time.sleep(interval)
                    request_data = {}
                    datetime_format = '%Y-%m-%dT%H:%M:%S.%fZ'
                    if last_timestamp_str is not None:
                        last_datetime = datetime.strptime(last_timestamp_str, datetime_format)
                        new_datetime = last_datetime + timedelta(milliseconds=1)
                        request_data['since'] = new_datetime.strftime(datetime_format)
                    request_data['ascending'] = 'true'
                    follow_r = autoauth.get(get_uri(f"job/{job_id}/text_log/"), params=request_data)
                    try:
                        follow_r.raise_for_status()
                        follow_logs = follow_r.json()["results"]
                        for record in follow_logs:
                            typer.echo(f"[{record['timestamp']}] {record['content']}")
                        if len(follow_logs) > 0:
                            last_timestamp_str = follow_logs[-1]['timestamp']
                    except HTTPError:
                        secho_error_and_exit(f"Log fetching failed! Error Code = {init_r.status_code}, "
                                             f"Detail = {init_r.text}")
            except KeyboardInterrupt:
                secho_error_and_exit(f"Keyboard Interrupt...", color=typer.colors.MAGENTA)
    except HTTPError:
        secho_error_and_exit(f"Log fetching failed! Error Code = {init_r.status_code}, Detail = {init_r.text}")


if __name__ == '__main__':
    app()
