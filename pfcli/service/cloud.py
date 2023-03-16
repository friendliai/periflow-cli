# Copyright (C) 2021 FriendliAI

"""PeriFlow Cloud Service"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import boto3
from azure.storage.blob import BlobServiceClient
from botocore.exceptions import ClientError
from mypy_boto3_s3.client import S3Client
from typing_extensions import TypeAlias

from pfcli.service import StorageType
from pfcli.utils.format import secho_error_and_exit

_CloudClient: TypeAlias = Union[S3Client, BlobServiceClient]
T = TypeVar("T", bound=_CloudClient)


@dataclass
class CloudStorageHelper(ABC, Generic[T]):
    client: T

    @abstractmethod
    def list_storage_files(
        self, storage_name: str, path_prefix: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List all file objects in the storage.

        Args:
            storage_name (str): Storage name
            path_prefix (Optional[str], optional): Direcotry path under the storage. Defaults to None.

        Returns:
            List[Dict[str, Any]]: A list of object info.

        """


@dataclass
class AWSCloudStorageHelper(CloudStorageHelper[S3Client]):
    def _check_aws_bucket_exists(self, storage_name: str) -> bool:
        try:
            self.client.head_bucket(Bucket=storage_name)
            return True
        except ClientError:
            # include both Forbidden access, Not Exists
            return False

    def list_storage_files(
        self, storage_name: str, path_prefix: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        if not self._check_aws_bucket_exists(storage_name):
            secho_error_and_exit(f"Bucket {storage_name} does not exist")

        file_list = []
        prefix_option = {"Prefix": path_prefix} if path_prefix is not None else {}
        resp = self.client.list_objects(Bucket=storage_name, **prefix_option)
        if "Contents" not in resp:
            secho_error_and_exit(
                f"No file exists at {path_prefix} in the bucket({storage_name})"
            )
        object_contents = resp["Contents"]
        for object_content in object_contents:
            try:
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
            except KeyError:
                secho_error_and_exit("Unexpected S3 error")

        if not file_list:
            secho_error_and_exit(f"No file exists in Bucket {storage_name}")

        return file_list


@dataclass
class AzureCloudStorageHelper(CloudStorageHelper[BlobServiceClient]):
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


def build_s3_client(credential_json: Dict[str, str]) -> S3Client:
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
vendor_helper_map: Dict[
    StorageType,
    Tuple[Type[CloudStorageHelper], Callable[[Dict[str, str]], _CloudClient]],
] = {
    StorageType.S3: (AWSCloudStorageHelper, build_s3_client),
    StorageType.BLOB: (AzureCloudStorageHelper, build_blob_client),
}


def build_storage_helper(
    vendor: StorageType, credential_json: Dict[str, Any]
) -> CloudStorageHelper:
    cls, client_build_fn = vendor_helper_map[vendor]
    client = client_build_fn(credential_json)
    return cls(client)
