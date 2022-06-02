# Copyright (C) 2022 FriendliAI

"""Test DataClient Service"""

from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
import requests_mock
import typer

from pfcli.service import ServiceType, StorageType
from pfcli.service.client import build_client
from pfcli.service.client.data import DataClientService


@pytest.fixture
def data_client() -> DataClientService:
    return build_client(ServiceType.DATA)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_data_client_get_datastore(requests_mock: requests_mock.Mocker, data_client: DataClientService):
    assert isinstance(data_client, DataClientService)

    # Success
    requests_mock.get(data_client.url_template.render(0), json={'id': 0, 'name': 'cifar100'})
    assert data_client.get_datastore(0) == {'id': 0, 'name': 'cifar100'}

    # Failed due to HTTP error
    requests_mock.get(data_client.url_template.render(0), status_code=404)
    with pytest.raises(typer.Exit):
        data_client.get_datastore(0)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_data_client_update_datastore(requests_mock: requests_mock.Mocker, data_client: DataClientService):
    assert isinstance(data_client, DataClientService)

    # Success
    requests_mock.get(
        data_client.url_template.render(0),
        json={
            'id': 0,
            'name': 'cifar10',
            'vendor': 'aws',
            'region': 'us-west-2'
        }
    )
    requests_mock.patch(data_client.url_template.render(0), json={'id': 0, 'name': 'cifar100'})
    assert data_client.update_datastore(
        0,
        name='cifar100',
        vendor=StorageType.S3,
        region='us-east-1',
        storage_name='my-bucket',
        credential_id='f5609b48-5e7e-4431-81d3-23b141847211',
        metadata={'k': 'v'},
        files=[
            {'name': 'cifar100', 'path': '/path/to/cifar100'}
        ],
        active=True
    ) == {'id': 0, 'name': 'cifar100'}

    # Failed at region validation
    requests_mock.get(
        data_client.url_template.render(0),
        json={
            'id': 0,
            'name': 'cifar10',
            'vendor': 'aws',
            'region': 'us-west-2'
        }
    )
    with pytest.raises(typer.Exit):
        data_client.update_datastore(
            0,
            name='cifar100',
            vendor=StorageType.S3,
            region='busan',     # region not available in AWS S3
            storage_name='my-bucket',
            credential_id='f5609b48-5e7e-4431-81d3-23b141847211',
            metadata={'k': 'v'},
            files=[
                {'name': 'cifar100', 'path': '/path/to/cifar100'}
            ],
            active=True
        )

    # Failed due to HTTP error
    requests_mock.patch(data_client.url_template.render(0), status_code=400)
    with pytest.raises(typer.Exit):
        data_client.update_datastore(0)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_data_client_delete_datastore(requests_mock: requests_mock.Mocker, data_client: DataClientService):
    assert isinstance(data_client, DataClientService)

    # Success
    requests_mock.delete(data_client.url_template.render(0), status_code=204)
    try:
        data_client.delete_datastore(0)
    except typer.Exit:
        raise pytest.fail("Data client test failed.")

    # Failed due to HTTP error
    requests_mock.delete(data_client.url_template.render(0), status_code=404)
    with pytest.raises(typer.Exit):
        data_client.delete_datastore(0)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_data_client_get_upload_urls(requests_mock: requests_mock.Mocker, data_client: DataClientService):
    assert isinstance(data_client, DataClientService)

    url_template = deepcopy(data_client.url_template)
    url_template.attach_pattern('$datastore_id/upload/')

    # Success
    requests_mock.post(
        url_template.render(datastore_id=0),
        json=[
            {'path': '/path/to/local/file', 'upload_url': 'https://s3.bucket.com'}
        ]
    )
    with TemporaryDirectory() as dir:
        (Path(dir) / 'file').touch()
        assert data_client.get_upload_urls(0, Path(dir), True) == [
            {'path': '/path/to/local/file', 'upload_url': 'https://s3.bucket.com'}
        ]

        # Handle a single file
        assert data_client.get_upload_urls(0, Path(dir) / 'file', True) == [
            {'path': '/path/to/local/file', 'upload_url': 'https://s3.bucket.com'}
        ]

    # Failed when uploading empty directory.
    with TemporaryDirectory() as dir:
        with pytest.raises(typer.Exit):
            data_client.get_upload_urls(0, Path(dir), True)

    # Failed due to HTTP error
    requests_mock.post(url_template.render(datastore_id=0), status_code=500)
    with TemporaryDirectory() as dir:
        (Path(dir) / 'file').touch()
        with pytest.raises(typer.Exit):
            data_client.get_upload_urls(0, Path(dir), True)
