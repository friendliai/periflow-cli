import tabulate
import typer
import requests
import yaml
import zipfile
from pathlib import Path
from typing import Optional

import autoauth
from utils import get_uri, auto_token_refresh, get_auth_header

app = typer.Typer()
template_app = typer.Typer()
log_app = typer.Typer()

app.add_typer(template_app, name="template")
app.add_typer(log_app, name="log")


def zip_dir(dir_path: Path, zip_path: Path):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for e in dir_path.rglob("*"):
            zip_file.write(e, e.relative_to(dir_path))


@app.command()
def run(vm_config_id: int = typer.Option(...),
        config_path: str = typer.Option(...),
        num_desired_devices: int = typer.Option(1),
        workspace_path: Optional[str] = typer.Option(None),
        data_store_name: Optional[str] = typer.Option(None)):
    request_data = {
        "vm_config_id": vm_config_id
    }
    config: dict = yaml.safe_load(Path(config_path).open("r"))
    for k, v in config.items():
        request_data.update({k: v})
    r = autoauth.post(get_uri("job/"), json=request_data)
    typer.echo(r.text)


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


@log_app.command("view")
def log_view(job_id: int = typer.Option(...),
             ascending: bool = typer.Option(False),
             limit: Optional[int] = typer.Option(None),
             log_type: Optional[str] = typer.Option(None),
             ):
    request_data = {
        "ascending": ascending
    }
    if limit is not None:
        request_data["limit"] = limit
    if log_type is not None:
        request_data["log_type"] = log_type
    r = autoauth.get(get_uri(f"job/{job_id}/text_log/"), json=request_data)
    # TODO: Elaborate
    typer.echo(r.text)


if __name__ == '__main__':
    app()
