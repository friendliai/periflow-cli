# Copyright (C) 2021 FriendliAI

"""PeriFlow Cloud Service"""

from pathlib import Path
from typing import Dict, List

import boto3
import botocore
from azure.storage.blob import BlobServiceClient

from pfcli.service import CloudType
from pfcli.utils import secho_error_and_exit


class CloudStorageHelper:
    def __init__(self,
                 file_or_dir: Path,
                 credential_json: Dict[str, str],
                 vendor: CloudType,
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
        if self.vendor == CloudType.S3:
            return self._get_checkpoint_file_list_aws()
        if self.vendor == CloudType.BLOB:
            return self._get_checkpoint_file_list_azure()
        return self._get_checkpoint_file_list_gcp()
