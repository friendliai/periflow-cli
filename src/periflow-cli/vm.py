"""CLI for VM
"""
import tabulate
import typer
from requests.exceptions import HTTPError

import autoauth
from utils import get_group_id, get_uri, secho_error_and_exit

app = typer.Typer()
quota_app = typer.Typer()
config_app = typer.Typer()

app.add_typer(quota_app, name="quota")
app.add_typer(config_app, name="config")


@quota_app.command("list")
def quota_list():
    """List all VM quota information.
    """
    group_id = get_group_id()

    response = autoauth.get(get_uri(f"group/{group_id}/vm_quota/"))
    try:
        response.raise_for_status()
    except HTTPError:
        secho_error_and_exit(
            f"Failed to get VM quota. Error code = {response.status_code} "
            f"detail = {response.text}.")

    quotas = response.json()["results"]

    headers = ["vm_instance_type", "initial_quota", "quota"]

    results = []
    for quota in quotas:
        results.append([quota[header] for header in headers])

    typer.echo(tabulate.tabulate(results, headers=["Instance Type", "Initial Quota", "Quota"]))
