# Copyright (C) 2021 FriendliAI

"""CLI for Serve"""

from dateutil.parser import parse
from typing import List, Optional

import typer
import requests

from pfcli.service import (
    ServiceType,
    GpuType,
    CloudType,
)
from pfcli.service.client import (
    ServeClientService,
    build_client,
)
from pfcli.service.formatter import PanelFormatter, TableFormatter
from pfcli.utils import datetime_to_pretty_str, get_pfs_inf_uri

EXIT_TEXTS = ["exit()", "quit()"]
DEFAULT_INF_NAME = "pfs-inference-server-deployment"

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
    serves: List[dict] = client.list_serves()['deployments']

    for serve in serves:
        started_at = serve.get("start")
        if started_at is not None:
            start = datetime_to_pretty_str(parse(started_at))
        else:
            start = None
        serve["start"] = start
        serve["gpu_type"] = serve.get("config")["gpu_type"]
        serve["name"] = serve.get("config")["name"]

        # Hard coding
        serve["vm"] = "p3.8xlarge"
        serve["num_gpus"] = serve.get("config")["total_gpus"]
        serve["status"] = "enqueued" if serve["status"] == "False" else "running"
        serve["id"] = serve.get("id").split("-")[-1]
    
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
    client.delete_serve(f"{DEFAULT_INF_NAME}-{serve_id}")

    typer.secho(f"Stop serve {serve_id}")


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
    serve = client.get_serve(f"{DEFAULT_INF_NAME}-{serve_id}")

    started_at = serve.get("start")
    if started_at is not None:
        start = datetime_to_pretty_str(parse(started_at))
    else:
        start = None    
    serve["start"] = start
    serve["name"] = serve.get("config")["name"]
    serve["gpu_type"] = serve.get("config")["gpu_type"]

    # Hard coding..
    serve["vm"] = "p3.8xlarge"
    serve["num_gpus"] = serve.get("config")["total_gpus"]
    serve["status"] = "enqueued" if serve["status"] == "False" else "running"
    serve["id"] = serve.get("id").split("-")[-1]

    serve["endpoint"] = serve.get("endpoint")
    serve_panel.render([serve])


@app.command()
def create(
    model_id: str = typer.Option(
        ...,
        "--model-id",
        "-m",
        help="Model id to deploy."
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
    cloud: CloudType = typer.Option(
        CloudType.AWS,
        "--cloud-type",
        "-c",
        help="The cloud type where the serve is deployed."
    ),
    region: str = typer.Option(
        "us-east-1",
        "--region",
        "-r",
        help="The region of cloud where the serve is deployed."
    ),
):
    """Create a serve object by using model checkpoint.
    """
    request_data = {
        "name": name,
        "model_id": model_id,
        "gpu_type": gpu_type,
        "inference_server_type": {
            "name":"orca", "repo": "friendliai/orca", "tag": "8fa2bdb-product-cuda11.4"
        },
        "cloud": cloud,
        "region": region,
        "project_id": "project-a" # Hard coding.
    }

    if model_id in ["13b", "13B"] or name in ["13b", "13B"]:
        request_data["orca_config"] = {
            "num_workers": 1,
            "num_devices": 4,
            "max_token_count": 10000,
            "max_batch_size": 256,
            "kv_cache_size": 40000,
        }
        request_data["total_gpus"] = 4
        # "orca_config": {
        #     "max_batch_size": 1,
        #     "num_sessions": 1,
        #     "num_workers": 1,
        #     "num_devices": 1,
        #     "max_token_count": 0,
        #     "kv_cache_size": 0
        # },

    client: ServeClientService = build_client(ServiceType.SERVE)
    serve = client.create_serve(request_data)

    typer.secho(
        f"Serve ({serve['id']}) started successfully. Use 'pf serve view <id>' to see the serve details.\n" \
        f"Run 'curl {serve['endpoint']}' for inference request",
        fg=typer.colors.BLUE
    )

@app.command()
def request(
    serve_id: str = typer.Argument(
        ...,
        help="Serve id to chat."
    ),
    max_tokens: int = typer.Option(
        20,
        "--max_tokens",
        "-t",
        help="Maximum sentence length to generate."
    ),
    top_k: int = typer.Option(
        5,
        "--top_k",
        "-k",
        help="Value of top k."
    ),
    no_repeat_ngram: int = typer.Option(
        4,
        "--no-repeat-ngram",
        "-g",
        help="N-gram size not to be repeated."
    )
):
    """Request to the serve id engine.
    """
    # client: ServeClientService = build_client(ServiceType.SERVE)
    # serve = client.get_serve(serve_id)
    # endpoint = serve.get("endpoint")
    engine_path = f"{DEFAULT_INF_NAME}-{serve_id}/v1/completions"
    endpoint = f"{get_pfs_inf_uri(engine_path)}"

    typer.secho(
        f"Chat with ({serve_id}) inference engine.\n" \
        f"For exit chat, enter empty line.",
        fg=typer.colors.BLUE
    )

    while True:
        prompt: str = input("> ")
        if not prompt:
            break
        request_body = {
            "prompt": prompt,
            "max_tokens": max_tokens,
            "no_repeat_ngram": no_repeat_ngram,
            "top_k": top_k,
            "n": 1
        }
        headers = {"Authorization": "Bearer test", "accept": "application/json"}

        response = requests.post(url=endpoint, json=request_body, headers=headers)
        output = response.json()["choices"][0]["text"]
        typer.secho(f"! {output}", fg=typer.colors.MAGENTA)

    typer.secho("Bye~!", fg=typer.colors.BLUE)