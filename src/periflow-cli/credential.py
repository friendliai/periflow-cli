from pathlib import Path
from typing import Optional, List, Dict

import tabulate
import typer
import yaml

import autoauth
from utils import get_uri


app = typer.Typer()


def _print_cred_list(cred_list: List[Dict]):
    headers = ["id", "name", "type", "type_version", "created_at"]
    results = []
    for cred in cred_list:
        results.append([cred["id"], cred["name"], cred["type"], cred["type_version"], cred["created_at"]])
    typer.echo(tabulate.tabulate(results, headers=headers))


@app.command()
def create(cred_type: str = typer.Option(...),
           name: str = typer.Option(...),
           yaml_path: str = typer.Option(...),
           type_version: int = typer.Option(1)):
    request_data = {
        "type": cred_type,
        "name": name,
        "type_version": type_version
    }
    yaml_path = Path(yaml_path)
    value = yaml.safe_load(yaml_path.open(mode="r"))
    request_data.update({"value": value})

    r = autoauth.post(get_uri("credential/"),
                      json=request_data)
    if r.status_code == 201:
        typer.echo(f"Credential registered... ID = {r.json()['id']}")
    else:
        typer.secho(f"Credential register failed... Code = {r.status_code}, Msg = {r.text}",
                    err=True,
                    color=typer.colors.RED)


@app.command()
def list(cred_type: str = typer.Option(...)):
    request_data = {"type": cred_type}
    r = autoauth.get(get_uri("credential/"),
                     json=request_data)
    if r.status_code == 200:
        _print_cred_list(r.json())
    else:
        typer.secho(f"Credential listing failed... Error Code = {r.status_code}, Detail = {r.text}",
                    err=True,
                    color=typer.colors.RED)


@app.command()
def update(cred_id: str = typer.Option(...),
           cred_type: Optional[str] = typer.Option(None),
           name: Optional[str] = typer.Option(None),
           type_version: int = typer.Option(None),
           yaml_path: Optional[str] = typer.Option(None)):
    request_data = {}
    if cred_type is not None:
        request_data["cred_type"] = cred_type
    if name is not None:
        request_data["name"] = name
    if type_version is not None:
        request_data["type_version"] = type_version
    if yaml_path is not None:
        yaml_path = Path(yaml_path)
        value = yaml.safe_load(yaml_path.open(mode="r"))
        request_data["value"] = value
    if not request_data:
        typer.secho("No properties to be updated...",
                    err=True,
                    fg=typer.colors.RED)
        typer.Exit(1)
    r = autoauth.patch(get_uri(f"credential/{cred_id}/"), json=request_data)
    cred = r.json()
    if r.status_code == 200:
        typer.echo(tabulate.tabulate(
            [[cred["id"], cred["name"], cred["type"], cred["type_version"], cred["created_at"]]],
            headers=["id", "name", "type", "type_version", "created_at"]))
    else:
        typer.secho(f"Update Failed... Error Code = {r.status_code}, Detail = {r.text}",
                    err=True,
                    fg=typer.colors.RED)


@app.command()
def delete(cred_id: str = typer.Option(...)):
    r = autoauth.delete(get_uri(f"credential/{cred_id}/"))
    if r.status_code == 204:
        typer.echo(f"Successfully deleted credential ID = {cred_id}")
    else:
        typer.secho(f"Delete failed... Error Code = {r.status_code}, Detail = {r.text}",
                    err=True,
                    fg=typer.colors.RED)


if __name__ == '__main__':
    app()
