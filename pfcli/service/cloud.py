# Copyright (C) 2021 FriendliAI

"""PeriFlow Cloud Service"""

from typing import (
    Callable,
    Dict,
    List,
    TypeVar,
    Type,
    Optional,
    Union,
    Tuple,
)
from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError
from botocore.client import BaseClient
from azure.storage.blob import BlobServiceClient

from pfcli.service import StorageType
from pfcli.utils.format import secho_error_and_exit


C = TypeVar("C", bound="CloudStorageHelper")
T = TypeVar("T", bound=Union[BaseClient, BlobServiceClient])


@dataclass
class CloudStorageHelper:
    client: T

    def list_storage_files(
        self, storage_name: str, path_prefix: Optional[str] = None
    ) -> List[dict]:
        raise NotImplementedError  # pragma: no cover


@dataclass
class AWSCloudStorageHelper(CloudStorageHelper):
    def _check_aws_bucket_exists(self, storage_name: str) -> bool:
        try:
            self.client.head_bucket(Bucket=storage_name)
            return True
        except ClientError:
            # include both Forbidden access, Not Exists
            return False

    def list_storage_files(
        self, storage_name: str, path_prefix: Optional[str] = None
    ) -> List[dict]:
        if not self._check_aws_bucket_exists(storage_name):
            secho_error_and_exit(f"Bucket {storage_name} does not exist")

        file_list = []
        prefix_option = {"Prefix": path_prefix} if path_prefix is not None else {}
        object_contents = self.client.list_objects(
            Bucket=storage_name, **prefix_option
        )["Contents"]
        for object_content in object_contents:
            object_key = object_content["Key"]
            name = object_key.split("/")[-1]
            if not name:
                continue  # skip directory
            file_list.append(
                {
                    "name": name,
                    "path": object_key,
                    "mtime": object_content["LastModified"].isoformat(),
                    "size": object_content["Size"],
                }
            )

        if not file_list:
            secho_error_and_exit(f"No file exists in Bucket {storage_name}")

        return file_list


@dataclass
class AzureCloudStorageHelper(CloudStorageHelper):
    def list_storage_files(self, storage_name: str, path_prefix: Optional[str] = None):
        container_client = self.client.get_container_client(storage_name)
        if not container_client.exists():
            secho_error_and_exit(f"Container {storage_name} does not exist")

        file_list = []
        prefix_option = (
            {"name_starts_with": path_prefix} if path_prefix is not None else {}
        )
        object_contents = container_client.list_blobs(**prefix_option)
        for object_content in object_contents:
            object_name = object_content["name"]
            name = object_name.split("/")[-1]
            if not name:
                continue  # skip directory
            file_list.append(
                {
                    "name": name,
                    "path": object_name,
                    "mtime": object_content["last_modified"].isoformat(),
                    "size": object_content["size"],
                }
            )

        if not file_list:
            secho_error_and_exit(f"No file exists in Bucket {storage_name}")

        return file_list


def build_s3_client(credential_json: Dict[str, str]) -> BaseClient:
    return boto3.client(
        "s3",
        aws_access_key_id=credential_json["aws_access_key_id"],
        aws_secret_access_key=credential_json["aws_secret_access_key"],
        region_name=credential_json.get("aws_default_region", None),
    )


def build_blob_client(credential_json: Dict[str, str]) -> BlobServiceClient:
    url = f"https://{credential_json['storage_account_name']}.blob.core.windows.net/"
    return BlobServiceClient(
        account_url=url, credential=credential_json["storage_account_key"]
    )


# TODO: Add GCP support
vendor_helper_map: Dict[StorageType, Tuple[Type[C], Callable[[Dict[str, str]], T]]] = {
    StorageType.S3: (AWSCloudStorageHelper, build_s3_client),
    StorageType.BLOB: (AzureCloudStorageHelper, build_blob_client),
}


def build_storage_helper(vendor: StorageType, credential_json: dict) -> C:
    cls, client_build_fn = vendor_helper_map[vendor]
    client = client_build_fn(credential_json)
    return cls(client)
