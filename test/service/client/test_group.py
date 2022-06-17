# Copyright (C) 2022 FriendliAI

"""Test GroupClient Service"""

import pytest
import requests_mock
import typer

from pfcli.service import CheckpointCategory, ModelFormCategory, ServiceType, StorageType
from pfcli.service.client import build_client
from pfcli.service.client.group import GroupProjectCheckpointClientService, GroupVMConfigClientService


@pytest.fixture
def group_vm_config_client(user_project_group_context) -> GroupVMConfigClientService:
    return build_client(ServiceType.GROUP_VM_CONFIG)

@pytest.fixture
def group_project_checkpoint_client(user_project_group_context) -> GroupProjectCheckpointClientService:
    return build_client(ServiceType.GROUP_PROJECT_CHECKPOINT)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_group_vm_config_client_get_id_by_name(requests_mock: requests_mock.Mocker,
                                               group_vm_config_client: GroupVMConfigClientService):
    example_data = [
        {
            "id": 0,
            "vm_config_type": {
                "id": 0,
                "name": "azure-v100",
                "code": "azure-v100",
                "vm_instance_type": {
                    "id": 0,
                    "name": "azure-v100",
                    "code": "azure-v100",
                    "vendor": "azure",
                    "region": "eastus",
                    "device_type": "V100"
                }
            }
        },
        {
            "id": 1,
            "vm_config_type": {
                "id": 1,
                "name": "aws-a100",
                "code": "aws-a100",
                "vm_instance_type": {
                    "id": 1,
                    "name": "aws-a100",
                    "code": "aws-a100",
                    "vendor": "aws",
                    "region": "us-east-1",
                    "device_type": "A100"
                }
            }
        },
    ]

    # Success
    requests_mock.get(group_vm_config_client.url_template.render(**group_vm_config_client.url_kwargs), json=example_data)
    assert group_vm_config_client.get_id_by_name('azure-v100') == 0
    assert group_vm_config_client.get_id_by_name('aws-a100') == 1
    assert group_vm_config_client.get_id_by_name('gcp-k80') is None

    # Failed due to HTTP error
    requests_mock.get(group_vm_config_client.url_template.render(**group_vm_config_client.url_kwargs), status_code=404)
    with pytest.raises(typer.Exit):
        group_vm_config_client.get_id_by_name('azure-a100')


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_group_vm_config_client_get_vm_config_id_map(requests_mock: requests_mock.Mocker,
                                                     group_vm_config_client: GroupVMConfigClientService):
    assert isinstance(group_vm_config_client, GroupVMConfigClientService)

    # Success
    requests_mock.get(
        group_vm_config_client.url_template.render(**group_vm_config_client.url_kwargs),
        json=[
            {'id': 0, 'vm_config_type': {'vm_instance_type': {'code': 'azure-v100'}}},
            {'id': 1, 'vm_config_type': {'vm_instance_type': {'code': 'aws-v100'}}}
        ]
    )
    assert group_vm_config_client.get_vm_config_id_map() == {
        'azure-v100': 0,
        'aws-v100' : 1
    }

    # Failed due to HTTP error
    requests_mock.get(group_vm_config_client.url_template.render(**group_vm_config_client.url_kwargs), status_code=404)
    with pytest.raises(typer.Exit):
        group_vm_config_client.get_vm_config_id_map()


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_group_checkpoint_list_checkpoints(requests_mock: requests_mock.Mocker,
                                           group_project_checkpoint_client: GroupProjectCheckpointClientService):
    def build_response_item(category: str, vendor: str, region: str) -> dict:
        return {
            "id": "22222222-2222-2222-2222-222222222222",
            "user_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
            "ownerships": [
                {
                "organization_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                "project_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6"
                }
            ],
            "model_category": category,
            "job_id": 2147483647,
            "name": "string",
            "attributes": {
                "job_setting_json": {},
                "data_json": {},
            },
            "forms": [
                {
                    "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                    "form_category": "MEGATRON",
                    "credential_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                    "vendor": vendor,
                    "region": region,
                    "storage_name": "STORAGE_NAME",
                    "dist_json": {
                    },
                    "files": [
                        {
                            "name": "NAME",
                            "path": "PATH",
                            "mtime": "2022-04-19T09:03:47.352Z",
                            "size": 9
                        }
                    ]
                }
            ],
            "iteration": 922,
            "created_at": "2022-04-19T09:03:47.352Z",
        }

    job_data = {
        "results": [
            build_response_item("JOB", "aws", "us-east-2"),
        ],
        "next_cursor": None,
    }

    user_data = {
        "results": [
            build_response_item("USER", "aws", "us-east-1"),
        ],
        "next_cursor": None,
    }

    # Success
    url = group_project_checkpoint_client.url_template.render(**group_project_checkpoint_client.url_kwargs)
    requests_mock.get(url, json=user_data)
    assert group_project_checkpoint_client.list_checkpoints(CheckpointCategory.USER_PROVIDED) == user_data['results']
    requests_mock.get(url, json=job_data)
    assert group_project_checkpoint_client.list_checkpoints(CheckpointCategory.JOB_GENERATED) == job_data['results']

    # Failed due to HTTP error
    requests_mock.get(url, status_code=400)
    with pytest.raises(typer.Exit):
        group_project_checkpoint_client.list_checkpoints(CheckpointCategory.USER_PROVIDED)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_group_checkpoint_create_checkpoints(requests_mock: requests_mock.Mocker,
                                             group_project_checkpoint_client: GroupProjectCheckpointClientService):
    data = {
        "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "category": "user_provided",
        "vendor": "aws",
        "region": "us-east-1",
        "credential_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "storage_name": "my-ckpt",
        "iteration": 1000,
        "files": [
            {
                "name": "new_ckpt_1000.pth",
                "path": "ckpt/new_ckpt_1000.pth",
                "mtime": "2022-04-20T06:27:37.907Z",
                "size": 2048,
            }
        ]
    }

    # Success
    # TODO: change after PFA integration
    url = group_project_checkpoint_client.url_template.render(
        **group_project_checkpoint_client.url_kwargs
    )
    requests_mock.post(url, json=data)
    assert group_project_checkpoint_client.create_checkpoint(
        name="my-ckpt",
        model_form_category=ModelFormCategory.MEGATRON,
        vendor=StorageType.S3,
        region='us-east-1',
        credential_id="3fa85f64-5717-4562-b3fc-2c963f66afa6",
        iteration=1000,
        storage_name="my-ckpt",
        files=[
            {
                "name": "new_ckpt_1000.pth",
                "path": "ckpt/new_ckpt_1000.pth",
                "mtime": "2022-04-20T06:27:37.907Z",
                "size": 2048,
            }
        ],
        dist_config={"k": "v"},
        data_config={"k": "v"},
        job_setting_config={"k": "v"}
    ) == data
    assert requests_mock.request_history[-1].json() == {
        "job_id": None,
        "vendor": "s3",
        "region": "us-east-1",
        "storage_name": "my-ckpt",
        "model_category": "USER",
        "form_category": "MEGATRON",
        "name": "my-ckpt",
        "dist_json": {"k": "v"},
        "attributes": {
            "job_setting_json": {"k": "v"},
            "data_json": {"k": "v"},
        },
        "credential_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "iteration": 1000,
        "user_id": "22222222-2222-2222-2222-222222222222",  # TODO: change after PFA integration
        "files": [
            {
                "name": "new_ckpt_1000.pth",
                "path": "ckpt/new_ckpt_1000.pth",
                "mtime": "2022-04-20T06:27:37.907Z",
                "size": 2048,
            }
        ]
    }


    # Failed due to invalid region
    with pytest.raises(typer.Exit):
        group_project_checkpoint_client.create_checkpoint(
            name="my-ckpt",
            model_form_category=ModelFormCategory.MEGATRON,
            vendor=StorageType.S3,
            region='busan',
            credential_id="3fa85f64-5717-4562-b3fc-2c963f66afa6",
            iteration=1000,
            storage_name="my-ckpt",
            files=[
                {
                    "name": "new_ckpt_1000.pth",
                    "path": "ckpt/new_ckpt_1000.pth",
                    "mtime": "2022-04-20T06:27:37.907Z",
                    "size": 2048,
                }
            ],
            dist_config={"k": "v"},
            data_config={"k": "v"},
            job_setting_config={"k": "v"}
        )

    # Failed due to HTTP error
    requests_mock.post(url, status_code=400)
    with pytest.raises(typer.Exit):
        group_project_checkpoint_client.create_checkpoint(
            name="my-ckpt",
            model_form_category=ModelFormCategory.MEGATRON,
            vendor=StorageType.S3,
            region='us-east-1',
            credential_id="3fa85f64-5717-4562-b3fc-2c963f66afa6",
            iteration=1000,
            storage_name="my-ckpt",
            files=[
                {
                    "name": "new_ckpt_1000.pth",
                    "path": "ckpt/new_ckpt_1000.pth",
                    "mtime": "2022-04-20T06:27:37.907Z",
                    "size": 2048,
                }
            ],
            dist_config={"k": "v"},
            data_config={"k": "v"},
            job_setting_config={"k": "v"}
        )
