# Copyright (C) 2021 FriendliAI

"""PeriFlow Cloud Service"""

from typing import Dict, List, TypeVar, Type, Optional
from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError
from azure.storage.blob import BlobServiceClient

from pfcli.service import StorageType
from pfcli.utils import secho_error_and_exit


C = TypeVar('C', bound='CloudStorageHelper')


@dataclass
class CloudStorageHelper:
    credential_json: Dict[str, str]

    def list_storage_files(self, storage_name: str, path_prefix: Optional[str] = None):
        raise NotImplementedError   # pragma: no cover


@dataclass
class AWSCloudStorageHelper(CloudStorageHelper):
    def __post_init__(self):
        self._s3_client = boto3.client(
            's3',
            aws_access_key_id=self.credential_json['aws_access_key_id'],
            aws_secret_access_key=self.credential_json['aws_secret_access_key'],
            region_name=self.credential_json.get('aws_default_region', None)
        )

    def _check_aws_bucket_exists(self, storage_name: str) -> bool:
        try:
            self._s3_client.head_bucket(Bucket=storage_name)
            return True
        except ClientError:
            # include both Forbidden access, Not Exists
            return False

    def list_storage_files(self, storage_name: str, path_prefix: Optional[str] = None) -> List[dict]:
        if not self._check_aws_bucket_exists(storage_name):
            secho_error_and_exit(f"Bucket {storage_name} does not exist")

        file_list = []
        prefix_option = {'Prefix': path_prefix} if path_prefix is not None else {}
        object_contents = self._s3_client.list_objects(Bucket=storage_name, **prefix_option)['Contents']
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


@dataclass
class AzureCloudStorageHelper(CloudStorageHelper):
    def __post_init__(self):
        url = f"https://{self.credential_json['storage_account_name']}.blob.core.windows.net/"
        self._blob_service_client = BlobServiceClient(
            account_url=url,
            credential=self.credential_json['storage_account_key']
        )

    def list_storage_files(self, storage_name: str, path_prefix: Optional[str] = None):
        container_client = self._blob_service_client.get_container_client(storage_name)
        if not container_client.exists():
            secho_error_and_exit(f"Container {storage_name} does not exist")

        file_list = []
        prefix_option = {'name_starts_with': path_prefix} if path_prefix is not None else {}
        object_contents = container_client.list_blobs(**prefix_option)
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


# TODO: Add GCP support
vendor_helper_map: Dict[StorageType, Type[C]] = {
    StorageType.S3: AWSCloudStorageHelper,
    StorageType.BLOB: AzureCloudStorageHelper,
}


def build_storage_helper(vendor: StorageType, credential_json: dict) -> C:
    cls = vendor_helper_map[vendor]
    return cls(credential_json=credential_json)
