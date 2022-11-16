# Copyright (C) 2021 FriendliAI

"""PeriFlow Credential CLI"""

import json
from uuid import UUID

import typer

from pfcli.service import ServiceType, CredType, cred_type_map_inv
from pfcli.service.client import (
    CredentialClientService,
    ProjectCredentialClientService,
    build_client,
)
from pfcli.service.config import CredentialConfigService
from pfcli.service.formatter import PanelFormatter, TableFormatter
from pfcli.utils.format import secho_error_and_exit


app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False,
)
create_app = typer.Typer(
    help="Create credential",
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False,
)

app.add_typer(create_app, name="create")

table_formatter = TableFormatter(
    name="Credentials",
    fields=["id", "name", "type", "created_at", "owner_type"],
    headers=["ID", "Name", "Type", "Created At", "Scope"],
)

panel_formatter = PanelFormatter(
    name="Overview",
    fields=["id", "name", "type", "created_at", "owner_type"],
    headers=["ID", "Name", "Type", "Created At", "Scope"],
)


S3_DOC_LINK = (
    "https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html"
)
AZURE_BLOB_DOC_LINK = "https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal"  # pylint: disable=line-too-long
GCP_DOC_LINK = (
    "https://cloud.google.com/iam/docs/creating-managing-service-account-keys"
)
SLACK_DOC_LINK = (
    "https://slack.com/help/articles/215770388-Create-and-regenerate-API-tokens"
)
WANDB_API_KEY_LINK = "https://wandb.ai/authorize"


@create_app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    interactive: bool = typer.Option(
        False, "--interactive", help="Use interactive mode."
    ),
):
    if not interactive:
        if ctx.invoked_subcommand is None:
            secho_error_and_exit(
                f"You should provide one of the credential types {[ e.value for e in CredType ]} as an argument, "
                "or use '--interactive' option. Run 'pf credential --help' for more details."
            )
        return

    configurator = CredentialConfigService()

    configurator.start_interaction()
    name, cred_type, value = configurator.render()

    client: ProjectCredentialClientService = build_client(
        ServiceType.PROJECT_CREDENTIAL
    )

    cred = client.create_credential(cred_type, name, 1, value)
    cred["type"] = cred_type_map_inv[cred["type"]].value

    typer.secho("Credential created successfully!", fg=typer.colors.BLUE)
    panel_formatter.render([cred])
    exit(0)


@create_app.command()
def docker(
    name: str = typer.Option(..., "--name", "-n", help="Name of the new credential."),
    username: str = typer.Option(..., help="Docker username."),
    password: str = typer.Option(..., help="Docker password."),
):
    """Create a credential for Docker."""
    client: ProjectCredentialClientService = build_client(
        ServiceType.PROJECT_CREDENTIAL
    )
    value = {"username": username, "password": password}
    cred = client.create_credential(CredType.DOCKER, name, 1, value)
    cred["type"] = cred_type_map_inv[cred["type"]].value
    panel_formatter.render([cred])


@create_app.command()
def s3(
    name: str = typer.Option(..., "--name", "-n", help="Name of the new credential."),
    aws_access_key_id: str = typer.Option(
        ..., help=f"[AWS] AWS Acess Key ID. Please see {S3_DOC_LINK}."
    ),
    aws_secret_access_key: str = typer.Option(
        ..., help=f"[AWS] AWS Secret Access Key. Please see {S3_DOC_LINK}."
    ),
    aws_default_region: str = typer.Option(
        ..., help=f"[AWS] Default region name. Please see {S3_DOC_LINK}."
    ),
):
    """Create a credential for AWS S3."""
    client: ProjectCredentialClientService = build_client(
        ServiceType.PROJECT_CREDENTIAL
    )
    value = {
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
        "aws_default_region": aws_default_region,
    }
    cred = client.create_credential(CredType.S3, name, 1, value)
    cred["type"] = cred_type_map_inv[cred["type"]].value
    panel_formatter.render([cred])


@create_app.command()
def azure_blob(
    name: str = typer.Option(..., "--name", "-n", help="Name of the new credential."),
    storage_account_name: str = typer.Option(
        ..., help=f"[Azure] Azure Blob storage account name."
    ),
    storage_account_key: str = typer.Option(
        ...,
        help=f"[Azure] Azure Blob storage account access key. Please see {AZURE_BLOB_DOC_LINK}.",
    ),
):
    """Create a credential for Azure Blob storage."""
    client: ProjectCredentialClientService = build_client(
        ServiceType.PROJECT_CREDENTIAL
    )
    value = {
        "storage_account_name": storage_account_name,
        "storage_account_key": storage_account_key,
    }
    cred = client.create_credential(CredType.BLOB, name, 1, value)
    cred["type"] = cred_type_map_inv[cred["type"]].value
    panel_formatter.render([cred])


@create_app.command()
def gcs(
    name: str = typer.Option(..., "--name", "-n", help="Name of the new credential."),
    service_account_key_file: typer.FileText = typer.Option(
        ...,
        help=f"[GCP] Path to GCP Service Account Key JSON file. Please see {GCP_DOC_LINK}.",
    ),
):
    """Create a credential for Google Cloud Storage."""
    client: ProjectCredentialClientService = build_client(
        ServiceType.PROJECT_CREDENTIAL
    )
    try:
        value = json.load(service_account_key_file)
    except json.JSONDecodeError as exc:
        secho_error_and_exit(f"Error occurred while parsing JSON file... {exc}")
    del value["type"]
    cred = client.create_credential(CredType.GCS, name, 1, value)
    cred["type"] = cred_type_map_inv[cred["type"]].value
    panel_formatter.render([cred])


@create_app.command()
def slack(
    name: str = typer.Option(..., "--name", "-n", help="Name of the new credential."),
    token: str = typer.Option(
        ..., help=f"Slack API Token. Please see {SLACK_DOC_LINK}."
    ),
):
    """Create a credential for Slack."""
    client: ProjectCredentialClientService = build_client(
        ServiceType.PROJECT_CREDENTIAL
    )
    value = {"token": token}
    cred = client.create_credential(CredType.SLACK, name, 1, value)
    cred["type"] = cred_type_map_inv[cred["type"]].value
    panel_formatter.render([cred])


@create_app.command()
def wandb(
    name: str = typer.Option(..., "--name", "-n", help="Name of the new credential."),
    api_key: str = typer.Option(
        ...,
        help=f"Weights & Biases API Key. You can get the key from {WANDB_API_KEY_LINK}.",
    ),
):
    """Create a credential for Weights & Biases."""
    client: ProjectCredentialClientService = build_client(
        ServiceType.PROJECT_CREDENTIAL
    )

    value = {"token": api_key}
    cred = client.create_credential(CredType.WANDB, name, 1, value)
    cred["type"] = cred_type_map_inv[cred["type"]].value
    panel_formatter.render([cred])


@app.command()
def list(
    cred_type: CredType = typer.Option(
        ..., "--cred-type", "-t", help="Type of credentials to list."
    ),
):
    """List credentials."""
    client: ProjectCredentialClientService = build_client(
        ServiceType.PROJECT_CREDENTIAL
    )
    creds = client.list_credentials(cred_type)
    for cred in creds:
        cred["type"] = cred_type_map_inv[cred["type"]].value

    table_formatter.render(creds)


@app.command()
def update(
    credential_id: UUID = typer.Argument(..., help="UUID of credential to update.")
):
    """Update a credential data."""
    configurator = CredentialConfigService()

    configurator.start_interaction_for_update(credential_id)
    name, _, value = configurator.render()

    client: CredentialClientService = build_client(ServiceType.CREDENTIAL)

    cred = client.update_credential(
        credential_id, name=name, type_version="1", value=value
    )
    cred["type"] = cred_type_map_inv[cred["type"]].value

    typer.secho("Credential updated successfully!", fg=typer.colors.BLUE)
    panel_formatter.render([cred])


@app.command()
def delete(
    credential_id: UUID = typer.Argument(..., help="UUID of credential to delete."),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Forcefully delete credential without confirmation prompt.",
    ),
):
    """Delete a credential."""
    if not force:
        do_delete = typer.confirm("Are you sure to delete credential?")
        if not do_delete:
            raise typer.Abort()

    client: CredentialClientService = build_client(ServiceType.CREDENTIAL)
    client.delete_credential(credential_id)

    typer.secho(
        f"Credential ({credential_id}) is deleted successfully!", fg=typer.colors.BLUE
    )
