# Copyright (C) 2021 FriendliAI

"""PeriFlow Credential CLI"""

from typing import List, Dict

import tabulate
import typer

from pfcli.service import ServiceType
from pfcli.service import CredType
from pfcli.service.client import CredentialClientService, GroupCredentialClientService, build_client
from pfcli.service.config import CredentialConfigService


app = typer.Typer()


def _print_cred_list(cred_list: List[Dict]):
    headers = ["id", "name", "type", "type_version", "created_at", "owner type"]
    results = []
    for cred in cred_list:
        results.append([cred["id"], cred["name"], cred["type"], cred["type_version"], cred["created_at"], cred["owner_type"]])
    typer.echo(tabulate.tabulate(results, headers=headers))


@app.command()
def create():
    configurator = CredentialConfigService()

    configurator.start_interaction()
    name, cred_type, value = configurator.render()

    is_group_shared = typer.confirm("Do you want to shared the credential with your group members?", default=True)
    if is_group_shared:
        client: GroupCredentialClientService = build_client(ServiceType.GROUP_CREDENTIAL)
    else:
        client: CredentialClientService = build_client(ServiceType.CREDENTIAL)

    info = client.create_credential(cred_type, name, 1, value)

    typer.secho("Credential created successfully!", fg=typer.colors.BLUE)
    _print_cred_list([info])


@app.command()
def list(
    cred_type: CredType = typer.Option(
        ...,
        '--cred-type',
        '-t',
        help="Type of credentials to list."
    ),
    group: bool = typer.Option(
        False,
        '--group',
        '-g',
        help="List group-shared credentials"
    )
):
    if group:
        client: GroupCredentialClientService = build_client(ServiceType.GROUP_CREDENTIAL)
    else:
        client: CredentialClientService = build_client(ServiceType.CREDENTIAL)
    creds = client.list_credentials(cred_type)

    _print_cred_list(creds)


@app.command()
def update(
    cred_id: str = typer.Option(
        ...,
        '--cred-id',
        '-i',
        help='UUID of credential to update.'
    )
):
    configurator = CredentialConfigService()

    configurator.start_interaction_for_update(cred_id)
    name, cred_type, value = configurator.render()

    client: CredentialClientService = build_client(ServiceType.CREDENTIAL)

    info = client.update_credential(cred_id, cred_type=cred_type, name=name, type_version=1, value=value)

    typer.secho("Credential updated successfully!", fg=typer.colors.BLUE)
    _print_cred_list([info])


@app.command()
def delete(
    cred_id: str = typer.Option(
        ...,
        '--cred-id',
        '-i',
        help='UUID of credential to delete.'
    )
):
    client: CredentialClientService = build_client(ServiceType.CREDENTIAL)
    client.delete_credential(cred_id)

    typer.secho("Credential deleted successfully!", fg=typer.colors.BLUE)


if __name__ == '__main__':
    app()
