# Copyright (C) 2021 FriendliAI

"""PeriFlow Experiment CLI"""

from typing import Optional
from datetime import datetime
from dateutil.parser import parse

import typer

from pfcli.service import ServiceType
from pfcli.service.client import ExperimentClientService, GroupExperimentClientService, build_client
from pfcli.service.formatter import TableFormatter
from pfcli.job import job_formatter
from pfcli.utils import datetime_to_pretty_str, timedelta_to_pretty_str

app = typer.Typer()
formatter = TableFormatter(fields=['id', 'name'], headers=['id', 'name'])


@app.command()
def create(
    name: str = typer.Option(
        ...,
        '--name',
        '-n',
        help="The name of experiment to create."
    )
):
    client: GroupExperimentClientService = build_client(ServiceType.GROUP_EXPERIMENT)
    experiment = client.create_experiment(name)
    typer.echo(formatter.render([experiment]))


@app.command()
def list():
    client: GroupExperimentClientService = build_client(ServiceType.GROUP_EXPERIMENT)
    experiments = client.list_experiments()
    typer.echo(formatter.render(experiments))


@app.command()
def view(
    name: str = typer.Option(
        ...,
        '--name',
        '-n',
        help="The name of experiment to view details."
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
):
    client: ExperimentClientService = build_client(ServiceType.EXPERIMENT)
    group_client: GroupExperimentClientService = build_client(ServiceType.GROUP_EXPERIMENT)
    experiment_id = group_client.get_id_by_name(name)
    jobs = client.get_jobs_in_experiment(experiment_id)

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


@app.command()
def update(
    name: str = typer.Option(
        ...,
        '--name',
        '-n',
        help="The name of experiment to update."
    ),
    new_name: str = typer.Option(
        ...,
        '--new-name',
        '-nn',
        help="The new new of experiment"
    )
):
    client: ExperimentClientService = build_client(ServiceType.EXPERIMENT)
    group_client: GroupExperimentClientService = build_client(ServiceType.GROUP_EXPERIMENT)

    experiment_id = group_client.get_id_by_name(name)
    experiment = client.update_experiment_name(experiment_id, new_name)
    typer.echo(formatter.render([experiment]))


@app.command()
def delete(
    name: str = typer.Option(
        ...,
        '-n',
        help="The name of experiment to delete."
    ),
    force: bool = typer.Option(
        False,
        '--force',
        '-f',
        help="Forcefully delete experiments and all jobs inside the experiment without confirmation prompt."
    )
):
    if not force:
        do_delete = typer.confirm(
            "!!! This action is VERY DESTRUCTIVE !!!\n"
            "Are your sure to delete experiment and ALL THE JOBS inside the experiment?"
        )
        if not do_delete:
            raise typer.Abort()
        really_do_delete = typer.confirm("STOP!!! Are you really sure to delete? This is the last confirmation.")
        if not really_do_delete:
            raise typer.Abort()

    client: ExperimentClientService = build_client(ServiceType.EXPERIMENT)
    group_client: GroupExperimentClientService = build_client(ServiceType.GROUP_EXPERIMENT)

    experiment_id = group_client.get_id_by_name(name)
    client.delete_experiment(experiment_id)

    typer.secho(f"Experiment ({name}) is deleted successfully!", fg=typer.colors.BLUE)
