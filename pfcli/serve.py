# Copyright (C) 2021 FriendliAI

"""CLI for Serve"""

from dateutil.parser import parse
from typing import Optional

import typer

from pfcli.service import (
    ServiceType,
    GpuType,
)
from pfcli.service.client import (
    ServeClientService,
    build_client,
)
from pfcli.service.formatter import PanelFormatter, TableFormatter
from pfcli.utils import datetime_to_pretty_str


app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False
)

serve_panel = PanelFormatter(
    name="Overview",
    fields=['id', 'name', 'status', 'vm', 'gpu_type', 'num_gpus', 'start', 'endpoint'],
    headers=['ID', 'Name', 'Status', 'VM', 'Device', 'Device Cnt', 'Start', 'Endpoint'],
    extra_fields=['error'],
    extra_headers=['error']
)

serve_table = TableFormatter(
    name="Serves",
    fields=[
        'id',
        'name',
        'status',
        'vm',
        'gpu_type',
        'num_gpus',
        'start',
    ],
    headers=['ID', 'Name', 'Status', 'VM', 'Device', 'Device Cnt', 'Start'],
    extra_fields=['error'],
    extra_headers=['error']
)

serve_panel.add_substitution_rule("waiting", "[bold]waiting")
serve_panel.add_substitution_rule("enqueued", "[bold cyan]enqueued")
serve_panel.add_substitution_rule("running", "[bold blue]running")
serve_panel.add_substitution_rule("success", "[bold green]success")
serve_panel.add_substitution_rule("failed", "[bold red]failed")
serve_panel.add_substitution_rule("terminated", "[bold yellow]terminated")
serve_panel.add_substitution_rule("terminating", "[bold magenta]terminating")
serve_panel.add_substitution_rule("cancelling", "[bold magenta]cancelling")

serve_table.add_substitution_rule("waiting", "[bold]waiting")
serve_table.add_substitution_rule("enqueued", "[bold cyan]enqueued")
serve_table.add_substitution_rule("running", "[bold blue]running")
serve_table.add_substitution_rule("success", "[bold green]success")
serve_table.add_substitution_rule("failed", "[bold red]failed")
serve_table.add_substitution_rule("terminated", "[bold yellow]terminated")
serve_table.add_substitution_rule("terminating", "[bold magenta]terminating")
serve_table.add_substitution_rule("cancelling", "[bold magenta]cancelling")


@app.command()
def list(
    tail: Optional[int] = typer.Option(
        None,
        "--tail",
        help="The number of serve list to view at the tail"
    ),
    head: Optional[int] = typer.Option(
        None,
        "--head",
        help="The number of serve list to view at the head"
    )
):
    """List all serves,
    """
    client: ServeClientService = build_client(ServiceType.SERVE)
    serves = client.list_serves()

    for serve in serves:
        started_at = serve.get("start")
        if started_at is not None:
            start = datetime_to_pretty_str(parse(started_at))
        else:
            start = None    
        serve["start"] = start
    
    if tail is not None or head is not None:
        target_serve_list = []
        if tail:
            target_serve_list.extend(serves[:tail])
        if head:
            target_serve_list.extend(serves[-head:]) 
    else:
        target_serve_list = serves
    
    serve_table.render(target_serve_list)


@app.command()
def stop(
    serve_id: str = typer.Argument(
        ...,
        help="ID of serve to stop"
    )
):
    """Delete serve.
    """
    client: ServeClientService = build_client(ServiceType.SERVE)
    client.delete_serve(serve_id)


@app.command()
def view(
    serve_id: str = typer.Argument(
        ...,
        help="serve id to inspect detail."
    )
):
    """Show details of a serve.
    """
    client: ServeClientService = build_client(ServiceType.SERVE)
    serve = client.get_serve(serve_id)

    started_at = serve.get("start")
    if started_at is not None:
        start = datetime_to_pretty_str(parse(started_at))
    else:
        start = None    
    serve["start"] = start

    serve_panel.render([serve])


@app.command()
def create(
    checkpoint_id: str = typer.Option(
        ...,
        "--checkpoint-id",
        "-c",
        help="Checkpoint id to deploy."
    ),
    name: str = typer.Option(
        ...,
        "--name",
        "-n",
        help="The name of serve deployment."
    ),
    gpu_type: GpuType = typer.Option(
        ...,
        "--gpu-type",
        "-g",
        help="The GPU type where the serve is deployed."
    ),
    num_sessions: int = typer.Option(
        ...,
        "--num-sessions",
        "-s",
        help="The number of sessions of serve deployment."
    ),
):
    """Create a serve object by using model checkpoint.
    """
    request_data = {
        "name": name,
        "model_id": checkpoint_id,
        "gpu_type": gpu_type,
        "num_sessions": num_sessions
    }
    client: ServeClientService = build_client(ServiceType.SERVE)
    serve = client.create_serve(request_data)

    typer.secho(
        f"Serve ({serve['id']}) started successfully. Use 'pf serivce view <id>' to see the serve details.\n" \
        f"Run 'curl {serve['endpoint']}' for inference request",
        fg=typer.colors.BLUE
    )
