# Copyright (C) 2021 FriendliAI

"""CLI for Serve"""

from dateutil.parser import parse

import typer

from pfcli.service import (
    ServiceType,
    GpuType,
)
from pfcli.service.client import (
    ServeClientService,
    build_client,
)
from pfcli.service.formatter import PanelFormatter
from pfcli.utils import datetime_to_pretty_str


app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False
)

# table_formatter = TableFormatter(
#     name="Checkpoints",
#     fields=['id', 'category', 'vendor', 'storage_name', 'iteration', 'created_at'],
#     headers=['ID', 'Category', 'Cloud', 'Storage Name', 'Iteration', 'Created At']
# )

serve_panel = PanelFormatter(
    name="Overview",
    fields=['deployment_id', 'name', 'status', 'vm', 'gpu_type', 'num_gpu', 'start', 'endpoint'],
    headers=['ID', 'Name', 'Status', 'VM', 'Device', 'Device Cnt', 'Start', 'Endpoint'],
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
    num_gpu: int = typer.Option(
        ...,
        "--num-gpu",
        "-ng",
        help="The number of gpu of serve deployment."
    ),
):
    """Create a serve object by using model checkpoint.
    """
    request_data = {
        "name": name,
        "model_id": checkpoint_id,
        "system_config": {
            "gpu_type": gpu_type,
            "num_devices": num_gpu,
            "num_workers": 1,
            "max_token_count": 8912,
            "max_batch_size": 1024,
            "kv_cache_size": 16384
        }
    }
    client: ServeClientService = build_client(ServiceType.SERVE)
    serve = client.create_serve(request_data)

    typer.secho(
        f"Serve ({serve['deployment_id']}) started successfully. Use 'pf serivce view <id>' to see the serve details.\n" \
        f"Run 'curl {serve['endpoint']}' for inference request",
        fg=typer.colors.BLUE
    )
