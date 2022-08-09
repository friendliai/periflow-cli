# Copyright (C) 2021 FriendliAI

"""PeriFlow Experiment CLI"""

from typing import Optional
from datetime import datetime
from dateutil.parser import parse

import typer

from pfcli.service import ServiceType
from pfcli.service.client import ExperimentClientService, ProjectExperimentClientService, build_client
from pfcli.service.formatter import PanelFormatter, TableFormatter
from pfcli.job import job_table
from pfcli.utils import datetime_to_pretty_str, timedelta_to_pretty_str

app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False
)
table_formatter = TableFormatter(name="Experiments", fields=['name'], headers=['Name'])
panel_formatter = PanelFormatter(name="Overview", fields=['name'], headers=['Name'])


@app.command()
def create(
    name: str = typer.Option(
        ...,
        '--name',
        '-n',
        help="The name of experiment to create."
    )
):
    """Create a new experiment.
    """
    client: ProjectExperimentClientService = build_client(ServiceType.PROJECT_EXPERIMENT)
    experiment = client.create_experiment(name)
    panel_formatter.render([experiment])


@app.command()
def list():
    """List all experiments.
    """
    client: ProjectExperimentClientService = build_client(ServiceType.PROJECT_EXPERIMENT)
    experiments = client.list_experiments()
    table_formatter.render(experiments)


@app.command()
def view(
    name: str = typer.Argument(
        ...,
        help="The name of experiment to view details."
    ),
    tail: Optional[int] = typer.Option(
        None,
        "--tail",
        help="The number of job list to view at the tail"
    ),
    head: Optional[int] = typer.Option(
        None,
        "--head",
        help="The number of job list to view at the head"
    ),
):
    """Show a list of jobs in the experiment.
    """
    client: ExperimentClientService = build_client(ServiceType.EXPERIMENT)
    project_client: ProjectExperimentClientService = build_client(ServiceType.PROJECT_EXPERIMENT)
    experiment_id = project_client.get_id_by_name(name)
    jobs = client.list_jobs_in_experiment(experiment_id)

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
        if head:
            target_job_list.extend(jobs[-head:]) 
    else:
        target_job_list = jobs

    job_table.render(target_job_list)


@app.command()
def edit(
    name: str = typer.Argument(
        ...,
        help="The name of experiment to update."
    ),
    new_name: str = typer.Option(
        ...,
        '--name',
        '-n',
        help="The new new of experiment"
    )
):
    """Edit experiment info.
    """
    client: ExperimentClientService = build_client(ServiceType.EXPERIMENT)
    project_client: ProjectExperimentClientService = build_client(ServiceType.PROJECT_EXPERIMENT)

    experiment_id = project_client.get_id_by_name(name)
    experiment = client.update_experiment_name(experiment_id, new_name)
    panel_formatter.render([experiment])


@app.command()
def delete(
    name: str = typer.Argument(
        ...,
        help="The name of experiment to delete."
    ),
    force: bool = typer.Option(
        False,
        '--force',
        '-f',
        help="Forcefully delete experiments and all jobs inside the experiment without confirmation prompt."
    )
):
    """Delete a experiment.
    """
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
    project_client: ProjectExperimentClientService = build_client(ServiceType.PROJECT_EXPERIMENT)

    experiment_id = project_client.get_id_by_name(name)
    client.delete_experiment(experiment_id)

    typer.secho(f"Experiment ({name}) is deleted successfully!", fg=typer.colors.BLUE)
