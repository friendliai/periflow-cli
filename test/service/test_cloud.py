# Copyright (C) 2021 FriendliAI

"""Test Client Service"""

from datetime import datetime
from unittest.mock import Mock

import pytest
import typer
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from azure.storage.blob import BlobServiceClient, ContainerClient

from pfcli.service import StorageType
from pfcli.service.cloud import AWSCloudStorageHelper, AzureCloudStorageHelper, build_storage_helper


@pytest.fixture
def s3_credential_json() -> dict:
    return {
        'aws_access_key_id': 'fake_aws_access_key_id',
        'aws_secret_access_key': 'fake_aws_secret_access_key',
        'aws_default_region': 'us-east-1'
    }


@pytest.fixture
def blob_credential_json() -> dict:
    return {
        'storage_account_name': 'fakestorageaccountname',
        'storage_account_key': 'fake_storage_account_key'
    }


@pytest.fixture
def blob_client_mock():
    return Mock(BlobServiceClient)()


@pytest.fixture
def container_client():
    return Mock(ContainerClient)()


@pytest.fixture
def s3_client_mock():
    return Mock(BaseClient)()


@pytest.fixture
def aws_storage_helper(s3_client_mock) -> AWSCloudStorageHelper:
    return AWSCloudStorageHelper(s3_client_mock)


@pytest.fixture
def azure_storage_helper(blob_client_mock) -> AzureCloudStorageHelper:
    return AzureCloudStorageHelper(blob_client_mock)


def test_build_storage_helper(s3_credential_json: dict, blob_credential_json: dict):
    aws_storage_helper = build_storage_helper(StorageType.S3, s3_credential_json)
    assert isinstance(aws_storage_helper, AWSCloudStorageHelper)
    assert isinstance(aws_storage_helper.client, BaseClient)

    azure_storage_helper = build_storage_helper(StorageType.BLOB, blob_credential_json)
    assert isinstance(azure_storage_helper, AzureCloudStorageHelper)
    assert isinstance(azure_storage_helper.client, BlobServiceClient)


def test_aws_list_storage_files(aws_storage_helper: AWSCloudStorageHelper, s3_client_mock):
    # Success
    file_data = [
        {
            'Key': 'path/to/file_1.txt',
            'LastModified': datetime.utcnow(),
            'ETAG': 'e32a59b7-3cc2-4666-bf99-27e238b7cf9c',
            'Size': 2048,
            'StorageClass': 'STANDARD',
            'Owner': {
                'ID': 'e32a59b7-3cc2-4666-bf99-27e238b7cf9c'
            }
        },
        {
            'Key': 'file_2.txt',
            'LastModified': datetime.utcnow(),
            'ETAG': 'e32a59b7-3cc2-4666-bf99-27e238b7cf9c',
            'Size': 2048,
            'StorageClass': 'STANDARD',
            'Owner': {
                'ID': 'e32a59b7-3cc2-4666-bf99-27e238b7cf9c'
            }
        }
    ]
    s3_client_mock.list_objects.return_value = {'Contents': file_data}

    aws_storage_helper.list_storage_files('my-bucket', 'dir') == [
        {
            'name': d['Key'].split('/')[-1],
            'path': d['Key'],
            'mtime': d['LastModified'].isoformat(),
            'size': d['Size']
        } for d in file_data
    ]
    s3_client_mock.head_bucket.assert_called_once_with(Bucket='my-bucket')
    s3_client_mock.list_objects.assert_called_once_with(Bucket='my-bucket', Prefix='dir')


def test_aws_list_storage_files_bucket_not_exist(aws_storage_helper: AWSCloudStorageHelper, s3_client_mock):
    s3_client_mock.head_bucket.side_effect = ClientError(
        {
            'Error': {
                'Code': 'fake err',
                'Message': 'fake err'
            }
        },
        'head_bucket'
    )

    with pytest.raises(typer.Exit):
        aws_storage_helper.list_storage_files('my-bucket')
    s3_client_mock.head_bucket.assert_called_once_with(Bucket='my-bucket')


def test_aws_list_storage_files_bucket_contains_no_file(aws_storage_helper: AWSCloudStorageHelper, s3_client_mock):
    s3_client_mock.list_objects.return_value = {'Contents': []}
    with pytest.raises(typer.Exit):
        aws_storage_helper.list_storage_files('my-bucket')
    s3_client_mock.head_bucket.assert_called_once_with(Bucket='my-bucket')
    s3_client_mock.list_objects.assert_called_once_with(Bucket='my-bucket')


def test_azure_list_storage_files(azure_storage_helper: AzureCloudStorageHelper, blob_client_mock, container_client):
    file_data = [
        {
            'name': 'path/to/file_1.txt',
            'container': 'my-container',
            'last_modified': datetime.utcnow(),
            'size': 2048
        },
        {
            'name': 'file_2.txt',
            'container': 'my-container',
            'last_modified': datetime.utcnow(),
            'size': 2048
        }
    ]

    blob_client_mock.get_container_client.return_value = container_client
    container_client.exists.return_value = True
    container_client.list_blobs.return_value = file_data

    assert azure_storage_helper.list_storage_files('my-container', 'dir') == [
        {
            'name': d['name'].split('/')[-1],
            'path': d['name'],
            'mtime': d['last_modified'].isoformat(),
            'size': d['size']
        } for d in file_data
    ]
    blob_client_mock.get_container_client.assert_called_once_with('my-container')
    container_client.exists.assert_called_once()
    container_client.list_blobs.assert_called_once_with(name_starts_with='dir')


def test_azure_list_storage_files_container_not_exists(azure_storage_helper: AzureCloudStorageHelper,
                                                       blob_client_mock,
                                                       container_client):
    blob_client_mock.get_container_client.return_value = container_client
    container_client.exists.return_value = False

    with pytest.raises(typer.Exit):
        azure_storage_helper.list_storage_files('my-container')
    blob_client_mock.get_container_client.assert_called_once_with('my-container')
    container_client.exists.assert_called_once()



def test_azure_list_storage_files_container_no_file(azure_storage_helper: AzureCloudStorageHelper,
                                                       blob_client_mock,
                                                       container_client):
    blob_client_mock.get_container_client.return_value = container_client
    container_client.exists.return_value = True
    container_client.list_blobs.return_value = []

    with pytest.raises(typer.Exit):
        azure_storage_helper.list_storage_files('my-container')
    blob_client_mock.get_container_client.assert_called_once_with('my-container')
    container_client.exists.assert_called_once()
    container_client.list_blobs.assert_called_once()