from pathlib import Path
from typing import Optional, List, Dict

import tabulate
import typer
import yaml
from requests import HTTPError

from pfcli import autoauth
from pfcli.utils import get_uri, secho_error_and_exit, get_group_id


app = typer.Typer()

def cred_id_by_name(cred_name: str, cred_type: str):
    request_data = {"type": cred_type}
    r = autoauth.get(get_uri("credential/"), params=request_data)
    try:
        r.raise_for_status()
    except HTTPError:
        secho_error_and_exit(f"Credential listing failed... Error Code = {r.status_code}, Detail = {r.text}")
    creds = r.json()
    try:
        cred_id = next(cred["id"] for cred in creds if cred["name"] == cred_name)
        return cred_id
    except:
        secho_error_and_exit(f"Cannot find a credential with such name")

    
def _print_cred_list(cred_list: List[Dict]):
    headers = ["name", "type", "type_version", "created_at"]
    results = []
    for cred in cred_list:
        results.append([cred["name"], cred["type"], cred["type_version"], cred["created_at"]])
    typer.echo(tabulate.tabulate(results, headers=headers))


@app.command()
def create(cred_type: str = typer.Option(...),
           name: str = typer.Option(...),
           config_file: typer.FileText = typer.Option(...),
           type_version: int = typer.Option(1)):
    request_data = {
        "type": cred_type,
        "name": name,
        "type_version": type_version
    }
    try:
        value = yaml.safe_load(config_file)
    except yaml.YAMLError as e:
        secho_error_and_exit(f"Error occurred while parsing config file... {e}")
    group_id = get_group_id()
    request_data.update({"value": value})

    r = autoauth.post(get_uri(f"group/{group_id}/credential/"),
                      json=request_data)
    try:
        r.raise_for_status()
        typer.echo(f"Credential registered... Name = {r.json()['name']}")
    except HTTPError:
        secho_error_and_exit(f"Credential register failed... Code = {r.status_code}, Msg = {r.text}")


@app.command()
def list(cred_type: str = typer.Option(...)):
    request_data = {"type": cred_type}
    r = autoauth.get(get_uri("credential/"),
                     params=request_data)
    try:
        r.raise_for_status()
        _print_cred_list(r.json())
    except HTTPError:
        secho_error_and_exit(f"Credential listing failed... Error Code = {r.status_code}, Detail = {r.text}")


@app.command()
def update(name: str = typer.Option(...),
           cred_type: str = typer.Option(...),
           type_version: int = typer.Option(None),
           config_file: Optional[typer.FileText] = typer.Option(None)):
    request_data = {}
    request_data["cred_type"] = cred_type
    request_data["name"] = name
    if type_version is not None:
        request_data["type_version"] = type_version
    if config_file is not None:
        try:
            value = yaml.safe_load(config_file)
        except yaml.YAMLError as e:
            secho_error_and_exit(f"Error occurred while parsing config file... {e}")
        request_data["value"] = value
    if type_version == None and config_file == None:
        secho_error_and_exit("No properties to be updated...")

    cred_id = cred_id_by_name(name, cred_type)
    r = autoauth.patch(get_uri(f"credential/{cred_id}/"), json=request_data)
    cred = r.json()
    try:
        r.raise_for_status()
        typer.echo(tabulate.tabulate(
            [cred["name"], cred["type"], cred["type_version"], cred["created_at"]]],
            headers=["name", "type", "type_version", "created_at"]))
    except HTTPError:
        secho_error_and_exit(f"Update Failed... Error Code = {r.status_code}, Detail = {r.text}")


@app.command()
def delete(name: str = typer.Option(...), cred_type: str = typer.Option(...)):
    cred_id = cred_id_by_name(name, cred_type)
    r = autoauth.delete(get_uri(f"credential/{cred_id}/"))
    try:
        r.raise_for_status()
        typer.echo(f"Successfully deleted credential Name = {name}")
    except HTTPError:
        secho_error_and_exit(f"Delete failed... Error Code = {r.status_code}, Detail = {r.text}")


if __name__ == '__main__':
    app()
