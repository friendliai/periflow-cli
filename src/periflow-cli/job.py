from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import json
import time
import zipfile

import tabulate
import typer
import yaml

import autoauth
from utils import get_uri

app = typer.Typer()
template_app = typer.Typer()
log_app = typer.Typer()

app.add_typer(template_app, name="template")
app.add_typer(log_app, name="log")


def _zip_dir(dir_path: Path, zip_path: Path):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_LZMA) as zip_file:
        for e in dir_path.rglob("*"):
            zip_file.write(e, e.relative_to(dir_path.parent))


@app.command()
def run(vm_config_id: int = typer.Option(...),
        config_file: str = typer.Option(...),
        num_desired_devices: int = typer.Option(1),
        workspace_dir: Optional[str] = typer.Option(None),
        data_store_name: Optional[str] = typer.Option(None)):
    # TODO: Support template.
    request_data = {
        "vm_config_id": vm_config_id
    }

    config: dict = yaml.safe_load(Path(config_file).open("r"))
    for k, v in config.items():
        request_data.update({k: v})
    files = None
    if workspace_dir is not None:
        if workspace_dir.endswith("/"):
            workspace_dir = workspace_dir[:-1]
        assert not workspace_dir.endswith("/")
        workspace = Path(workspace_dir)
        if not workspace.is_dir():
            typer.secho(f"Specified workspace is not directory...",
                        err=True,
                        fg=typer.colors.RED)
            typer.Exit(1)
        workspace_zip = Path(workspace_dir + ".zip")
        _zip_dir(workspace, workspace_zip)
        files = {'workspace_zip': ('workspace.zip', workspace_zip.open('rb'))}
    r = autoauth.post(get_uri("job/"), data={"data": json.dumps(request_data)}, files=files)
    if r.status_code == 201:
        headers = ["id", "workspace_signature", "workspace_id"]
        typer.echo(tabulate.tabulate([[r.json()["id"],
                                       r.json()["workspace_signature"],
                                       r.json()["workspace_id"]]], headers=headers))
    else:
        typer.secho(f"Failed to run the specified job! Error Code = {r.status_code} Detail = {r.text}",
                    err=True,
                    fg=typer.colors.RED)


@app.command()
def list():
    r = autoauth.get(get_uri("user/group/"))
    if r.status_code != 200:
        typer.echo(r.text)
        typer.Exit(1)
    groups = r.json()["results"]
    request_data = {}
    # TODO: Solve discrepancy in "limit"
    job_list = []
    for group in groups:
        request_data.update({"group_id": group["id"]})
        r = autoauth.get(get_uri("job/"), json=request_data)
        if r.status_code != 200:
            typer.echo(r.text)
            typer.Exit(1)
        for job in r.json()["results"]:
            job_list.append([job["id"],
                             job["status"],
                             job["vm_config"]["vm_config_type"]["name"],
                             job["vm_config"]["vm_config_type"]["vm_instance_type"]["device_type"],
                             job["num_desired_devices"],
                             job["data_store"],
                             job["started_at"],
                             job["finished_at"]])
    if r.status_code == 200:
        typer.echo(tabulate.tabulate(
            job_list,
            headers=["id", "status", "vm_name", "device", "# devices", "datastore", "start", "end"]))
    else:
        typer.secho(f"List failed! Error Code = {r.status_code}, Detail = {r.text}",
                    err=True,
                    fg=typer.colors.RED)


@app.command()
def stop(job_id: int = typer.Option(...)):
    r = autoauth.get(get_uri(f"job/{job_id}/"))
    if r.status_code != 200:
        typer.echo(r.text)
        typer.Exit(1)
    job_status = r.json()["status"]
    if job_status == "waiting":
        r = autoauth.post(get_uri(f"job/{job_id}/cancel/"))
        if r.status_code == 204:
            typer.echo("Job is now cancelling...")
        else:
            # TODO: Handle synchronization error
            typer.secho(f"Job stop failed! Error Code = {r.status_code}, Detail = {r.text}",
                        err=True,
                        fg=typer.colors.RED)
    elif job_status == "running" or job_status == "enqueued":
        r = autoauth.post(get_uri(f"job/{job_id}/terminate/"))
        if r.status_code == 204:
            typer.echo("Job is now terminating...")
        else:
            typer.secho(f"Job stop failed! Error Code = {r.status_code}, Detail = {r.text}",
                        err=True,
                        fg=typer.colors.RED)
    else:
        typer.secho(f"No need to stop existing job... Current job status = {job_status}",
                    err=True,
                    fg=typer.colors.RED)


@app.command()
def view(job_id: int = typer.Option(...)):
    r = autoauth.get(get_uri(f"job/{job_id}/"))
    if r.status_code == 200:
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
    else:
        typer.secho(f"View failed! Error Code = {r.status_code}, Detail = {r.text}",
                    err=True,
                    fg=typer.colors.RED)


@template_app.command("list")
def template_list():
    r = autoauth.get(get_uri(f"job_template/"))
    # TODO: Elaborate
    typer.echo(r.text)


@template_app.command("view")
def template_view(template_id: int = typer.Option(...)):
    r = autoauth.get(get_uri(f"job_template/{template_id}/"))
    # TODO: Elaborate
    typer.echo(r.text)


# TODO: Implement since/until if necessary
@log_app.command("view")
def log_view(job_id: int = typer.Option(...),
             num_lines: int = typer.Option(100),
             pattern: Optional[str] = typer.Option(None),
             log_type: Optional[str] = typer.Option(None),
             head: bool = typer.Option(False),
             export_path: Optional[str] = typer.Option(None),
             follow: bool = typer.Option(False)
             ):
    if num_lines <= 0 or num_lines > 10000:
        typer.secho("'num_lines' should be a positive integer, equal or smaller than 10000",
                    err=True,
                    fg=typer.colors.RED)
        typer.Exit(1)

    if head and follow:
        typer.secho("'follow' cannot be set in 'head' mode",
                    err=True,
                    fg=typer.colors.RED)
        typer.Exit(1)

    if export_path is not None and follow:
        typer.secho("'follow' cannot be set when 'export_path' is given",
                    err=True,
                    fg=typer.colors.RED)
        typer.Exit(1)

    request_data = dict()
    if head:
        request_data['ascending'] = 'true'
    else:
        request_data['ascending'] = 'false'
    request_data['limit'] = num_lines
    if pattern is not None:
        request_data['content'] = pattern
    if log_type is not None:
        request_data['log_type'] = log_type
    init_r = autoauth.get(get_uri(f"job/{job_id}/text_log/"), params=request_data)
    fetched_lines = 0

    if init_r.status_code == 200:
        logs = init_r.json()['results']
        fetched_lines += len(logs)
        if not head:
            logs.reverse()
        if export_path is not None:
            with Path(export_path).open("w") as export_file:
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
                    time.sleep(1)
                    request_data = {}
                    datetime_format = '%Y-%m-%dT%H:%M:%S.%fZ'
                    if last_timestamp_str is not None:
                        last_datetime = datetime.strptime(last_timestamp_str, datetime_format)
                        new_datetime = last_datetime + timedelta(seconds=1)
                        request_data['since'] = new_datetime.strftime(datetime_format)
                    request_data['ascending'] = 'true'
                    follow_r = autoauth.get(get_uri(f"job/{job_id}/text_log/"), params=request_data)
                    if follow_r.status_code == 200:
                        follow_logs = follow_r.json()["results"]
                        for record in follow_logs:
                            typer.echo(f"[{record['timestamp']}] {record['content']}")
                        if len(follow_logs) > 0:
                            last_timestamp_str = follow_logs[-1]['timestamp']
                    else:
                        typer.secho(f"Log fetching failed! Error Code = {init_r.status_code}, Detail = {init_r.text}",
                                    err=True,
                                    fg=typer.colors.RED)
                        typer.Exit(1)
            except KeyboardInterrupt:
                typer.secho(f"Keyboard Interrupt...",
                            fg=typer.colors.MAGENTA)
                pass
    else:
        typer.secho(f"Log fetching failed! Error Code = {init_r.status_code}, Detail = {init_r.text}",
                    err=True,
                    fg=typer.colors.RED)
        typer.Exit(1)


if __name__ == '__main__':
    app()
