"""CLI for Checkpoint
"""
from pathlib import Path
from typing import Dict, List, Optional

import boto3
import botocore
import tabulate
import typer
from azure.storage.blob import BlobServiceClient
from requests import HTTPError

from pfcli import autoauth
from pfcli.utils import get_group_id, get_uri, secho_error_and_exit

app = typer.Typer()


def _echo_checkpoint_detail(checkpoint_json: dict):
    typer.echo(f"id: {checkpoint_json['id']}")
    typer.echo(f"category: {checkpoint_json['category']}")
    typer.echo(f"vendor: {checkpoint_json['vendor']}")
    typer.echo(f"iteration: {checkpoint_json['iteration']}")
    typer.echo(f"created_at: {checkpoint_json['created_at']}")
    typer.echo("files:")
    headers = ["name", "path", "mtime", "size"]
    results = []
    for file in checkpoint_json['files']:
        results.append([file[header] for header in headers])
    headers[2] = "modified time"
    typer.echo(tabulate.tabulate(results, headers=headers))


@app.command("list")
def checkpoint_list(category: Optional[str] = typer.Option(None),
                    cursor: Optional[str] = typer.Option(None),
                    limit: Optional[int] = typer.Option(None)):
    """List all checkpoints that belong to the user's group
    """
    group_id = get_group_id()
    request_data = {}

    if category is not None:
        request_data.update({"category": category})
    if cursor is not None:
        request_data.update({"cursor": cursor})
    if limit is not None:
        request_data.update({"limit": limit})

    response = autoauth.get(get_uri(f"group/{group_id}/checkpoint/"), json=request_data)
    try:
        response.raise_for_status()
    except HTTPError:
        secho_error_and_exit(
            f"Cannot retrieve checkpoints. Error code = {response.status_code} detail = {response.text}")
    checkpoints = response.json()["results"]
    next_cursor = response.json()["next_cursor"]

    headers = ["id", "category", "vendor", "storage_name", "iteration", "created_at"]
    results = []
    for checkpoint in checkpoints:
        results.append([checkpoint[header] for header in headers])

    typer.echo(f"Cursor: {next_cursor}")
    typer.echo(tabulate.tabulate(results, headers=headers))


@app.command("view")
def checkpoint_detail(checkpoint_id: str = typer.Option(...)):
    """Show details of the given checkpoint_id
    """
    group_id = get_group_id()
    response = autoauth.get(get_uri(f"group/{group_id}/checkpoint/{checkpoint_id}/"))
    try:
        response.raise_for_status()
    except HTTPError:
        secho_error_and_exit(
            f"Cannot retrieve checkpoint. Error code = {response.status_code} detail = {response.text}")

    checkpoint_json = response.json()
    _echo_checkpoint_detail(checkpoint_json)


class CloudStorageHelper:
    def __init__(self,
                 file_or_dir: Path,
                 credential_json: Dict[str, str],
                 vendor: str,
                 storage_name: str):
        self.file_or_dir = file_or_dir
        self.credential_json = credential_json
        self.storage_name = storage_name
        self.vendor = vendor

    def _get_checkpoint_file_list_gcp(self) -> List[dict]:
        raise NotImplementedError

    def _get_checkpoint_file_list_azure(self) -> List[dict]:
        url = f"https://{self.credential_json['storage_account_name']}.blob.core.windows.net/"
        blob_service_client = BlobServiceClient(
            account_url=url,
            credential=self.credential_json['storage_account_key'])

        container_client = blob_service_client.get_container_client(self.storage_name)
        if not container_client.exists():
            secho_error_and_exit(f"Container {self.storage_name} does not exist")

        file_list = []

        object_contents = container_client.list_blobs(name_starts_with=str(self.file_or_dir))
        for object_content in object_contents:
            object_name = object_content['name']
            file_list.append({
                'name': object_name.split('/')[-1],
                'path': object_name,
                'mtime': object_content['last_modified'].isoformat(),
                'size': object_content['size'],
            })

        if not file_list:
            secho_error_and_exit(f"No file exists in Bucket {self.storage_name}")

        return file_list

    def _check_aws_bucket_exists(self, client) -> bool:
        try:
            client.head_bucket(Bucket=self.storage_name)
            return True
        except botocore.exceptions.ClientError:
            # include both Forbidden access, Not Exists
            return False

    def _get_checkpoint_file_list_aws(self) -> List[dict]:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=self.credential_json['aws_access_key_id'],
            aws_secret_access_key=self.credential_json['aws_secret_access_key'],
            region_name=self.credential_json.get('aws_default_region', None))

        if not self._check_aws_bucket_exists(s3_client):
            secho_error_and_exit(f"Bucket {self.storage_name} does not exist")

        file_list = []

        object_contents = s3_client.list_objects(
            Bucket=self.storage_name, Prefix=str(self.file_or_dir))['Contents']
        for object_content in object_contents:
            object_key = object_content['Key']
            file_list.append({
                'name': object_key.split('/')[-1],
                'path': object_key,
                'mtime': object_content['LastModified'].isoformat(),
                'size': object_content['Size']
            })

        if not file_list:
            secho_error_and_exit(f"No file exists in Bucket {self.storage_name}")

        return file_list

    def get_checkpoint_file_list(self) -> List[dict]:
        if self.vendor == "aws":
            return self._get_checkpoint_file_list_aws()
        if self.vendor == "azure":
            return self._get_checkpoint_file_list_azure()
        return self._get_checkpoint_file_list_gcp()


@app.command("create")
def checkpoint_create(file_or_dir: Path = typer.Option(...),
                      iteration: int = typer.Option(...),
                      vendor: str = typer.Option(...),
                      storage_name: str = typer.Option(...),
                      credential_id: str = typer.Option(...),
                      # advanced arguments
                      pp_degree: int = typer.Option(1),
                      dp_degree: int = typer.Option(1),
                      mp_degree: int = typer.Option(1),
                      dp_mode: str = typer.Option('allreduce'),
                      parallelism_order: str = typer.Option('pp,dp,mp')):
    """Create the checkpoint.
    """
    group_id = get_group_id()

    parallelism_order = parallelism_order.split(",")
    if {"pp", "dp", "mp"} != set(parallelism_order):
        secho_error_and_exit("Invalid Argument: parallelism_order should contain 'pp', 'dp', 'mp'")

    # dist_json
    dist_json = {
        "pp_degree": pp_degree,
        "dp_degree": dp_degree,
        "mp_degree": mp_degree,
        "dp_mode": dp_mode,
        "parallelism_order": parallelism_order
    }

    # TODO (TB): get job_setting_json from CLI

    if credential_id is None:
        secho_error_and_exit("Invalid Argument: credential_id should be provided")

    if vendor not in ("aws", "azure", "gcp"):
        secho_error_and_exit("Invalid Argument: vendor should be one of `azure`, `aws`, `gcp`.")

    request_data = {
        "category": "user_provided",
        "vendor": vendor,
        "iteration": iteration,
        "storage_name": storage_name,
        "dist_json": dist_json,
        "credential_id": credential_id,
        "job_setting_json": None,
    }

    response = autoauth.get(get_uri(f"credential/{credential_id}/"))
    try:
        response.raise_for_status()
    except HTTPError:
        secho_error_and_exit(
            "Cannot retrieve credential. "
            f"Error code = {response.status_code} detail = {response.text}")

    credential_json = response.json()
    if credential_json["type"] != vendor:
        secho_error_and_exit(
            f"Credential type and vendor mismatch: {credential_json['type']} and {vendor}")

    storage_helper = CloudStorageHelper(
        file_or_dir, credential_json["value"], vendor, storage_name)
    request_data["files"] = storage_helper.get_checkpoint_file_list()

    response = autoauth.post(get_uri(f"group/{group_id}/checkpoint/"), json=request_data)
    try:
        response.raise_for_status()
        _echo_checkpoint_detail(response.json())
    except HTTPError:
        secho_error_and_exit(
            "Failed to create checkpoint. "
            f"Error code = {response.status_code} detail = {response.text}")


@app.command("update")
def checkpoint_update(checkpoint_id: str = typer.Option(...),
                      file_or_dir: Path = typer.Option(...),
                      iteration: Optional[int] = typer.Option(None),
                      credential_id: Optional[str] = typer.Option(None),
                      vendor: Optional[str] = typer.Option(None),
                      storage_name: Optional[str] = typer.Option(None)):
    """Update the existing checkpoint.
    """
    # TODO (TB): Currently, cannot modify dist_json and job_setting_json
    group_id = get_group_id()

    response = autoauth.get(get_uri(f"group/{group_id}/checkpoint/{checkpoint_id}/"))
    try:
        response.raise_for_status()
    except HTTPError:
        secho_error_and_exit(
            "Cannot retrieve checkpoint. "
            f"Error code = {response.status_code} detail = {response.text}")
    checkpoint_json = response.json()

    request_data = {}
    if iteration is not None:
        request_data["iteration"] = iteration
    if vendor is not None:
        request_data["vendor"] = vendor
    else:
        vendor = checkpoint_json["vendor"]
    if credential_id is not None:
        request_data["credential_id"] = credential_id
    else:
        credential_id = checkpoint_json["credential_id"]
    if storage_name is not None:
        request_data["storage_name"] = storage_name
    else:
        storage_name = checkpoint_json["storage_name"]

    response = autoauth.get(get_uri(f"credential/{credential_id}/"))
    try:
        response.raise_for_status()
    except HTTPError:
        secho_error_and_exit(
            "Cannot retrieve credential. "
            f"Error code = {response.status_code} detail = {response.text}")

    credential_json = response.json()
    if credential_json["type"] != vendor:
        secho_error_and_exit(
            f"Credential type and vendor mismatch: {credential_json['type']} and {vendor}")

    storage_helper = CloudStorageHelper(
        file_or_dir, credential_json["value"], vendor, storage_name)
    request_data["files"] = storage_helper.get_checkpoint_file_list()

    response = autoauth.patch(get_uri(
        f"group/{group_id}/checkpoint/{checkpoint_id}/"), json=request_data)
    try:
        response.raise_for_status()
        _echo_checkpoint_detail(response.json())
    except HTTPError:
        secho_error_and_exit(
            "Failed to update checkpoint. "
            f"Error code = {response.status_code} detail = {response.text}")


@app.command("delete")
def checkpoint_delete(checkpoint_id: str = typer.Option(...)):
    """Delete the existing checkpoint.
    """
    group_id = get_group_id()

    response = autoauth.delete(get_uri(f"group/{group_id}/checkpoint/{checkpoint_id}/"))
    try:
        response.raise_for_status()
        typer.echo(f"Successfully deleted checkpoint (ID = {checkpoint_id})")
    except HTTPError:
        secho_error_and_exit(
            f"Delete failed. Error code = {response.status_code}, Detail = {response.text}")


if __name__ == '__main__':
    app()
