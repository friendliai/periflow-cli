"""CLI for VM
"""
from typing import Optional

import json
import tabulate
import typer
import yaml
from requests.exceptions import HTTPError

from pfcli import autoauth
from pfcli.utils import get_group_id, get_uri, secho_error_and_exit

app = typer.Typer()
quota_app = typer.Typer()
config_app = typer.Typer()

app.add_typer(quota_app, name="quota")
app.add_typer(config_app, name="config")


def config_id_by_type_code(vm_config_code: str):
    group_id = get_group_id()

    response = autoauth.get(get_uri(f"group/{group_id}/vm_config/"))
    try:
        response.raise_for_status()
    except HTTPError:
        secho_error_and_exit(
            f"Failed to get VM configs. Error code = {response.status_code} "
            f"detail = {response.text}.")
    vm_configs = response.json()
    try:
        config_id = next(vm_config['id'] for vm_config in vm_configs if vm_config['vm_config_type']['code'] == vm_config_code)
        return config_id
    except:
        secho_error_and_exit(f"Cannot find a vm config with such code")


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

    quotas = response.json()

    headers = ["vm_instance_type", "initial_quota", "quota"]

    results = []
    for quota in quotas:
        sub_result = []
        for header in headers:
            if header == "vm_instance_type":
                sub_result.append(json.dumps(quota[header], indent=2))
            else:
                sub_result.append(quota[header])
        results.append(sub_result)

    typer.echo(tabulate.tabulate(results, headers=[x.replace("_", " ") for x in headers]))


@config_app.command("type")
def config_type_list():
    response = autoauth.get(get_uri("vm_config_type/"))
    try:
        response.raise_for_status()
    except HTTPError:
        secho_error_and_exit(
            f"Failed to get VM config. Error code = {response.status_code} "
            f"detail = {response.text}.")

    vm_config_types = response.json()

    headers = ["name", "code", "data_schema", "vm_instance_type", "num_devices_per_vm"]
    results = []
    for vm_config_type in vm_config_types:
        sub_result = []
        for header in headers:
            if header == "data_schema":
                sub_result.append(yaml.dump(vm_config_type[header], indent=2))
            elif header == "vm_instance_type":
                type_detail = {
                    "code": vm_config_type[header]["code"],
                    "name": vm_config_type[header]["name"], 
                    "device type": vm_config_type[header]["device_type"],
                    "region": vm_config_type[header]["region"],
                    "vendor": vm_config_type[header]["vendor"]
                }
                sub_result.append(yaml.dump(type_detail, indent=2))
            else:
                sub_result.append(vm_config_type[header])
        results.append(sub_result)

    typer.echo(tabulate.tabulate(results, headers=[x.replace("_", " ") for x in headers], tablefmt="fancy_grid"))


@config_app.command("list")
def config_list():
    group_id = get_group_id()

    response = autoauth.get(get_uri(f"group/{group_id}/vm_config/"))
    try:
        response.raise_for_status()
    except HTTPError:
        secho_error_and_exit(
            f"Failed to get VM configs. Error code = {response.status_code} "
            f"detail = {response.text}.")

    vm_configs = response.json()

    headers = ["name", "code"]
    results = []
    for vm_config in vm_configs:
        sub_result = []
        for header in headers:
            if header == "name":
                sub_result.append(vm_config['vm_config_type'][header])
            else:
                sub_result.append(vm_config['vm_config_type'][header])
        results.append(sub_result)

    typer.echo("\nvm config types")
    typer.echo(tabulate.tabulate(results, headers))


@config_app.command("view")
def config_detail(vm_config_id: int = typer.Option(...)):
    response = autoauth.get(get_uri(f"vm_config/{vm_config_id}/"))
    try:
        response.raise_for_status()
    except HTTPError:
        secho_error_and_exit(
            f"Failed to get VM config. Error code = {response.status_code} "
            f"detail = {response.text}.")

    result = response.json()
    typer.echo(f"id: {result['id']}")
    typer.echo(f"group id: {result['group_id']}")
    typer.echo("config type:")
    typer.echo(f"    id: {result['vm_config_type']['id']}")
    typer.echo(f"    name: {result['vm_config_type']['name']}")
    typer.echo("template data:")
    typer.echo(json.dumps(result["template_data"], indent=4))


@config_app.command("create")
def config_create(vm_config_type_code: str = typer.Option(...),
                  template_data_file: typer.FileText = typer.Option(...)):
    group_id = get_group_id()

    try:
        template_data = yaml.safe_load(template_data_file)
    except yaml.YAMLError as exc:
        secho_error_and_exit(f"Error occurred while parsing template file... {exc}")

    request_data = {}
    request_data["vm_config_type_code"] = vm_config_type_code
    request_data["template_data"] = template_data

    response = autoauth.post(get_uri(f"group/{group_id}/vm_config/"), json=request_data)

    try:
        response.raise_for_status()
    except HTTPError:
        secho_error_and_exit(
            f"Failed to create VM config. Error code = {response.status_code} "
            f"detail = {response.text}.")

    result = {
        "config type": {
            "name": response.json()['vm_config_type']['name'],
            "code": response.json()['vm_config_type']['code']
        },
        "template data": response.json()['template_data']
    }
    typer.echo(yaml.dump(result, indent=4))


@config_app.command("update")
def config_update(vm_config_type_code: str = typer.Option(...),
                  template_data_file: typer.FileText = typer.Option(...)):
    try:
        template_data = yaml.safe_load(template_data_file)
    except yaml.YAMLError as exc:
        secho_error_and_exit(f"Error occurred while parsing template file... {exc}")

    vm_config_id = config_id_by_type_code(vm_config_type_code)
    request_data = {}
    request_data["template_data"] = template_data
    response = autoauth.patch(get_uri(f"vm_config/{vm_config_id}/"), json=request_data)
    try:
        response.raise_for_status()
    except HTTPError:
        secho_error_and_exit(
            f"Failed to update VM config. Error code = {response.status_code} "
            f"detail = {response.text}.")
    result = {
        "config type": {
            "name": response.json()['vm_config_type']['name'],
            "code": response.json()['vm_config_type']['code']
        },
        "template data": response.json()['template_data']
    }
    typer.echo(yaml.dump(result, indent=4))


@config_app.command("delete")
def config_delete(vm_config_type_code: str = typer.Option(...)):
    vm_config_id = config_id_by_type_code(vm_config_type_code)
    response = autoauth.delete(get_uri(f"vm_config/{vm_config_id}/"))
    try:
        response.raise_for_status()
    except HTTPError:
        secho_error_and_exit(
            f"Failed to delete VM config. Error code = {response.status_code} "
            f"detail = {response.text}.")
    typer.echo(f"Successfully deleted vm config (vm config type code = {vm_config_type_code})")
