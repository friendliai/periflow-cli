# Copyright (C) 2022 FriendliAI

"""Test DataClient Service"""

import os
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
import requests_mock
import typer

from pfcli.service import ServiceType, StorageType
from pfcli.service.client import build_client
from pfcli.service.client.data import (
    S3_MPU_PART_MAX_SIZE,
    S3_UPLOAD_SIZE_LIMIT,
    DataClientService,
)


def write_file(path: str, size: int) -> None:
    with open(path, "wb") as f:
        f.seek(size - 1)
        f.write(b"\0")


@pytest.fixture
def data_client() -> DataClientService:
    return build_client(ServiceType.DATA)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_data_client_get_dataset(
    requests_mock: requests_mock.Mocker, data_client: DataClientService
):
    assert isinstance(data_client, DataClientService)

    # Success
    requests_mock.get(
        data_client.url_template.render(0), json={"id": 0, "name": "cifar100"}
    )
    assert data_client.get_dataset(0) == {"id": 0, "name": "cifar100"}

    # Failed due to HTTP error
    requests_mock.get(data_client.url_template.render(0), status_code=404)
    with pytest.raises(typer.Exit):
        data_client.get_dataset(0)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_data_client_update_dataset(
    requests_mock: requests_mock.Mocker, data_client: DataClientService
):
    assert isinstance(data_client, DataClientService)

    # Success
    requests_mock.get(
        data_client.url_template.render(0),
        json={"id": 0, "name": "cifar10", "vendor": "aws", "region": "us-west-2"},
    )
    requests_mock.patch(
        data_client.url_template.render(0), json={"id": 0, "name": "cifar100"}
    )
    assert data_client.update_dataset(
        0,
        name="cifar100",
        vendor=StorageType.S3,
        region="us-east-1",
        storage_name="my-bucket",
        credential_id="f5609b48-5e7e-4431-81d3-23b141847211",
        metadata={"k": "v"},
        files=[{"name": "cifar100", "path": "/path/to/cifar100"}],
        active=True,
    ) == {"id": 0, "name": "cifar100"}

    # Failed at region validation
    requests_mock.get(
        data_client.url_template.render(0),
        json={"id": 0, "name": "cifar10", "vendor": "aws", "region": "us-west-2"},
    )
    with pytest.raises(typer.Exit):
        data_client.update_dataset(
            0,
            name="cifar100",
            vendor=StorageType.S3,
            region="busan",  # region not available in AWS S3
            storage_name="my-bucket",
            credential_id="f5609b48-5e7e-4431-81d3-23b141847211",
            metadata={"k": "v"},
            files=[{"name": "cifar100", "path": "/path/to/cifar100"}],
            active=True,
        )

    # Failed due to HTTP error
    requests_mock.patch(data_client.url_template.render(0), status_code=400)
    with pytest.raises(typer.Exit):
        data_client.update_dataset(0)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_data_client_delete_dataset(
    requests_mock: requests_mock.Mocker, data_client: DataClientService
):
    assert isinstance(data_client, DataClientService)

    # Success
    requests_mock.delete(data_client.url_template.render(0), status_code=204)
    try:
        data_client.delete_dataset(0)
    except typer.Exit:
        raise pytest.fail("Data client test failed.")

    # Failed due to HTTP error
    requests_mock.delete(data_client.url_template.render(0), status_code=404)
    with pytest.raises(typer.Exit):
        data_client.delete_dataset(0)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_data_client_get_spu_urls(
    requests_mock: requests_mock.Mocker, data_client: DataClientService
):
    assert isinstance(data_client, DataClientService)

    url_template = deepcopy(data_client.url_template)
    url_template.attach_pattern("$dataset_id/upload/")

    # Success
    requests_mock.post(
        url_template.render(dataset_id=0),
        json=[{"path": "/path/to/local/file", "upload_url": "https://s3.bucket.com"}],
    )
    assert data_client.get_spu_urls(
        obj_id=0, storage_paths=["/path/to/local/file"]
    ) == [{"path": "/path/to/local/file", "upload_url": "https://s3.bucket.com"}]

    # Failed due to HTTP error
    requests_mock.post(url_template.render(dataset_id=0), status_code=500)
    with pytest.raises(typer.Exit):
        data_client.get_spu_urls(obj_id=0, storage_paths=["/path/to/local/file"])


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_data_client_get_mpu_urls(
    requests_mock: requests_mock.Mocker, data_client: DataClientService
):
    assert isinstance(data_client, DataClientService)

    url_template = deepcopy(data_client.url_template)
    url_template.attach_pattern("$dataset_id/start_mpu/")

    with TemporaryDirectory() as temp_dir:
        target_file_path = os.path.join(temp_dir, "large_file")
        resp_mock = {
            "path": target_file_path,
            "upload_id": "865b8d498d1fb82e92a7e808e82c4111",
            "upload_urls": [
                {
                    "upload_url": "https://mydata.s3.amazonaws.com/path/to/file/part1",
                    "part_number": 1,
                },
                {
                    "upload_url": "https://mydata.s3.amazonaws.com/path/to/file/part2",
                    "part_number": 2,
                },
            ],
        }
        write_file(target_file_path, S3_UPLOAD_SIZE_LIMIT * 2)

        requests_mock.post(url_template.render(dataset_id=0), json=resp_mock)
        assert data_client.get_mpu_urls(
            obj_id=0,
            local_paths=[target_file_path],
            storage_paths=["large_file"],
        ) == [resp_mock]

        requests_mock.post(url_template.render(dataset_id=0), status_code=500)
        with pytest.raises(typer.Exit):
            data_client.get_mpu_urls(
                obj_id=0,
                local_paths=[target_file_path],
                storage_paths=["large_file"],
            )


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_data_client_complete_mpu(
    requests_mock: requests_mock.Mocker, data_client: DataClientService
):
    assert isinstance(data_client, DataClientService)

    url_template = deepcopy(data_client.url_template)
    url_template.attach_pattern("$dataset_id/complete_mpu/")

    requests_mock.post(url_template.render(dataset_id=0))
    data_client.complete_mpu(
        0,
        "/path/to/file",
        "fakeuploadid",
        [
            {"etag": "fakeetag", "part_number": 1},
            {"etag": "fakeetag", "part_number": 2},
        ],
    )

    requests_mock.post(url_template.render(dataset_id=0), status_code=500)
    with pytest.raises(typer.Exit):
        data_client.complete_mpu(
            0,
            "/path/to/file",
            "fakeuploadid",
            [
                {"etag": "fakeetag", "part_number": 1},
                {"etag": "fakeetag", "part_number": 2},
            ],
        )


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_data_client_abort_mpu(
    requests_mock: requests_mock.Mocker, data_client: DataClientService
):
    assert isinstance(data_client, DataClientService)

    url_template = deepcopy(data_client.url_template)
    url_template.attach_pattern("$dataset_id/abort_mpu/")

    requests_mock.post(url_template.render(dataset_id=0))
    data_client.abort_mpu(
        0,
        "/path/to/file",
        "fakeuploadid",
    )

    requests_mock.post(url_template.render(dataset_id=0), status_code=500)
    with pytest.raises(typer.Exit):
        data_client.abort_mpu(
            0,
            "/path/to/file",
            "fakeuploadid",
        )


def test_upload_small_files(
    requests_mock: requests_mock.Mocker, data_client: DataClientService
):
    assert isinstance(data_client, DataClientService)
    fake_upload_url = "https://mybucket.s3.amazon.com"

    with TemporaryDirectory() as tmp_dir:
        path = os.path.join(tmp_dir, "small_file")
        write_file(path, 1024)

        # Success
        requests_mock.put(fake_upload_url)
        data_client.upload_files(
            obj_id=0,
            spu_url_dicts=[
                {
                    "path": "small_file",
                    "upload_url": fake_upload_url,
                }
            ],
            mpu_url_dicts=[],
            source_path=Path(tmp_dir),
        )

        # Upload failed
        requests_mock.put(fake_upload_url, status_code=400)
        with pytest.raises(typer.Exit):
            data_client.upload_files(
                obj_id=0,
                spu_url_dicts=[
                    {
                        "path": "small_file",
                        "upload_url": fake_upload_url,
                    }
                ],
                mpu_url_dicts=[],
                source_path=Path(tmp_dir),
            )


def test_upload_large_files(
    requests_mock: requests_mock.Mocker, data_client: DataClientService
):
    assert isinstance(data_client, DataClientService)
    fake_upload_url = "https://mybucket.s3.amazon.com"

    url_template = deepcopy(data_client.url_template)
    url_template.attach_pattern("$dataset_id/abort_mpu/")
    requests_mock.post(url_template.render(dataset_id=0))

    url_template = deepcopy(data_client.url_template)
    url_template.attach_pattern("$dataset_id/complete_mpu/")
    requests_mock.post(url_template.render(dataset_id=0))

    with TemporaryDirectory() as tmp_dir:
        path = os.path.join(tmp_dir, "large_file")
        write_file(path, S3_MPU_PART_MAX_SIZE)  # This is hack to pass assertion

        # Success
        requests_mock.put(fake_upload_url, headers={"ETag": "fakeetag"})
        data_client.upload_files(
            obj_id=0,
            spu_url_dicts=[],
            mpu_url_dicts=[
                {
                    "path": "large_file",
                    "upload_id": "uploadid1",
                    "upload_urls": [
                        {
                            "upload_url": fake_upload_url,
                            "part_number": 1,
                        },
                        {
                            "upload_url": fake_upload_url,
                            "part_number": 2,
                        },
                    ],
                },
            ],
            source_path=Path(tmp_dir),
        )

        # Upload failed
        requests_mock.put(fake_upload_url, status_code=400)
        with pytest.raises(typer.Exit):
            data_client.upload_files(
                obj_id=0,
                spu_url_dicts=[],
                mpu_url_dicts=[
                    {
                        "path": "large_file",
                        "upload_id": "uploadid1",
                        "upload_urls": [
                            {
                                "upload_url": fake_upload_url,
                                "part_number": 1,
                            },
                            {
                                "upload_url": fake_upload_url,
                                "part_number": 2,
                            },
                        ],
                    },
                ],
                source_path=Path(tmp_dir),
            )
