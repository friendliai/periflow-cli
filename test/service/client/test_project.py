# Copyright (C) 2022 FriendliAI

"""Test ProjectClient Service"""

from copy import deepcopy
from uuid import UUID

import pytest
import requests_mock
import typer

from pfcli.service import CloudType, CredType, ServiceType, StorageType
from pfcli.service.client import build_client
from pfcli.service.client.project import (
    PFTProjectVMConfigClientService,
    PFTProjectVMQuotaClientService,
    ProjectCredentialClientService,
    ProjectDataClientService,
    ProjectVMLockClientService,
)


@pytest.fixture
def project_credential_client(
    user_project_group_context,
) -> ProjectCredentialClientService:
    return build_client(ServiceType.PROJECT_CREDENTIAL)


@pytest.fixture
def project_data_client(user_project_group_context) -> ProjectDataClientService:
    return build_client(ServiceType.PROJECT_DATA)


@pytest.fixture
def project_vm_quota_client(
    user_project_group_context,
) -> PFTProjectVMQuotaClientService:
    return build_client(ServiceType.PFT_PROJECT_VM_QUOTA)


@pytest.fixture
def project_vm_config_client(
    user_project_group_context,
) -> PFTProjectVMConfigClientService:
    return build_client(ServiceType.PFT_PROJECT_VM_CONFIG)


@pytest.fixture
def project_vm_lock_client(
    user_project_group_context,
) -> ProjectVMLockClientService:
    return build_client(ServiceType.PROJECT_VM_LOCK)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_project_data_client_list_datasets(
    requests_mock: requests_mock.Mocker, project_data_client: ProjectDataClientService
):
    # Success
    requests_mock.get(
        project_data_client.url_template.render(**project_data_client.url_kwargs),
        json=[{"id": 0, "name": "wikitext"}, {"id": 1, "name": "imagenet"}],
    )
    assert project_data_client.list_datasets() == [
        {"id": 0, "name": "wikitext"},
        {"id": 1, "name": "imagenet"},
    ]

    # Failed due to HTTP error
    requests_mock.get(
        project_data_client.url_template.render(**project_data_client.url_kwargs),
        status_code=404,
    )
    with pytest.raises(typer.Exit):
        project_data_client.list_datasets()


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_project_data_client_get_id_by_name(
    requests_mock: requests_mock.Mocker, project_data_client: ProjectDataClientService
):
    # Success
    requests_mock.get(
        project_data_client.url_template.render(**project_data_client.url_kwargs),
        json=[{"id": 0, "name": "wikitext"}, {"id": 1, "name": "imagenet"}],
    )
    assert project_data_client.get_id_by_name("wikitext") == 0
    assert project_data_client.get_id_by_name("imagenet") == 1
    assert project_data_client.get_id_by_name("openwebtext") is None

    # Failed due to HTTP error
    requests_mock.get(
        project_data_client.url_template.render(**project_data_client.url_kwargs),
        status_code=404,
    )
    with pytest.raises(typer.Exit):
        project_data_client.get_id_by_name("glue")


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_project_data_client_create_dataset(
    requests_mock: requests_mock.Mocker, project_data_client: ProjectDataClientService
):
    # Success
    requests_mock.post(
        project_data_client.url_template.render(**project_data_client.url_kwargs),
        json={"id": 0, "name": "cifar100"},
    )
    assert project_data_client.create_dataset(
        name="cifar100",
        vendor=StorageType.FAI,
        region="",
        storage_name="",
        credential_id=UUID("f5609b48-5e7e-4431-81d3-23b141847211"),
        metadata={"k": "v"},
        files=[],
        active=False,
    ) == {"id": 0, "name": "cifar100"}

    # Failed at region validation
    with pytest.raises(typer.Exit):
        project_data_client.create_dataset(
            name="cifar100",
            vendor=StorageType.FAI,
            region="us-east-1",  # not supported by FAI storage type
            storage_name="",
            credential_id=UUID("f5609b48-5e7e-4431-81d3-23b141847211"),
            metadata={"k": "v"},
            files=[],
            active=False,
        )

    # Failed due to HTTP error
    requests_mock.post(
        project_data_client.url_template.render(**project_data_client.url_kwargs),
        status_code=400,
    )
    with pytest.raises(typer.Exit):
        project_data_client.create_dataset(
            name="cifar100",
            vendor=StorageType.FAI,
            region="",
            storage_name="",
            credential_id=UUID("f5609b48-5e7e-4431-81d3-23b141847211"),
            metadata={"k": "v"},
            files=[],
            active=False,
        )


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_project_vm_quota_client_list_vm_quotas(
    requests_mock: requests_mock.Mocker,
    project_vm_quota_client: PFTProjectVMQuotaClientService,
):
    example_data = [
        {
            "vm_config_type": {
                "id": 0,
                "code": "azure-v100",
                "vendor": "azure",
                "region": "eastus",
                "device_type": "V100",
            },
            "quota": 4,
        },
        {
            "vm_config_type": {
                "id": 1,
                "code": "azure-a100",
                "vendor": "azure",
                "region": "westus2",
                "device_type": "A100",
            },
            "quota": 8,
        },
        {
            "vm_config_type": {
                "id": 2,
                "code": "aws-a100",
                "vendor": "aws",
                "region": "us-east-1",
                "device_type": "A100",
            },
            "quota": 16,
        },
    ]

    # Success
    requests_mock.get(
        project_vm_quota_client.url_template.render(
            **project_vm_quota_client.url_kwargs
        ),
        json=example_data,
    )

    # List VMs without filters
    assert project_vm_quota_client.list_vm_quotas() == example_data

    # List VMs filtered by vendor
    assert project_vm_quota_client.list_vm_quotas(vendor=CloudType.AWS) == [
        {
            "vm_config_type": {
                "id": 2,
                "code": "aws-a100",
                "vendor": "aws",
                "region": "us-east-1",
                "device_type": "A100",
            },
            "quota": 16,
        }
    ]
    assert project_vm_quota_client.list_vm_quotas(vendor=CloudType.AZURE) == [
        {
            "vm_config_type": {
                "id": 0,
                "code": "azure-v100",
                "vendor": "azure",
                "region": "eastus",
                "device_type": "V100",
            },
            "quota": 4,
        },
        {
            "vm_config_type": {
                "id": 1,
                "code": "azure-a100",
                "vendor": "azure",
                "region": "westus2",
                "device_type": "A100",
            },
            "quota": 8,
        },
    ]

    # List VMs filtered by device type
    assert project_vm_quota_client.list_vm_quotas(device_type="A100") == [
        {
            "vm_config_type": {
                "id": 1,
                "code": "azure-a100",
                "vendor": "azure",
                "region": "westus2",
                "device_type": "A100",
            },
            "quota": 8,
        },
        {
            "vm_config_type": {
                "id": 2,
                "code": "aws-a100",
                "vendor": "aws",
                "region": "us-east-1",
                "device_type": "A100",
            },
            "quota": 16,
        },
    ]

    # List VMs filtered by vendor, region and device type
    assert project_vm_quota_client.list_vm_quotas(
        vendor=CloudType.AZURE, device_type="A100"
    ) == [
        {
            "vm_config_type": {
                "id": 1,
                "code": "azure-a100",
                "vendor": "azure",
                "region": "westus2",
                "device_type": "A100",
            },
            "quota": 8,
        }
    ]

    # Failed due to HTTP error
    requests_mock.get(
        project_vm_quota_client.url_template.render(
            **project_vm_quota_client.url_kwargs
        ),
        status_code=400,
    )
    with pytest.raises(typer.Exit):
        project_vm_quota_client.list_vm_quotas()


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_project_credential_client_service(
    requests_mock: requests_mock.Mocker,
    project_credential_client: ProjectCredentialClientService,
):
    # Sucess
    requests_mock.get(
        project_credential_client.url_template.render(
            **project_credential_client.url_kwargs
        ),
        json=[{"id": 0, "name": "our-docker-secret", "type": "docker"}],
    )
    assert project_credential_client.list_credentials(CredType.DOCKER) == [
        {"id": 0, "name": "our-docker-secret", "type": "docker"}
    ]

    # Failed due to HTTP error
    requests_mock.get(
        project_credential_client.url_template.render(
            **project_credential_client.url_kwargs
        ),
        status_code=400,
    )
    with pytest.raises(typer.Exit):
        project_credential_client.list_credentials(CredType.SLACK)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_project_vm_config_client_get_active_vm_count(
    requests_mock: requests_mock.Mocker,
    project_vm_config_client: PFTProjectVMConfigClientService,
):
    # Success
    url_template = deepcopy(project_vm_config_client.url_template)
    url_template.attach_pattern("$vm_config_id/vm_lock/")
    requests_mock.get(
        url_template.render(**project_vm_config_client.url_kwargs, vm_config_id=0),
        json=[
            {"status": "active", "vm_config_id": 0, "job_id": 0},
            {"status": "active", "vm_config_id": 0, "job_id": 0},
            {"status": "active", "vm_config_id": 0, "job_id": 1},
            {"status": "active", "vm_config_id": 0, "job_id": 2},
        ],
    )
    assert project_vm_config_client.get_vm_count_in_use(0) == 4

    # Failed due to HTTP error
    requests_mock.get(
        url_template.render(**project_vm_config_client.url_kwargs, vm_config_id=0),
        status_code=404,
    )
    with pytest.raises(typer.Exit):
        project_vm_config_client.get_vm_count_in_use(0)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_get_project_vm_locks(
    requests_mock: requests_mock.Mocker,
    project_vm_lock_client: ProjectVMLockClientService,
):
    requests_mock.get(
        project_vm_lock_client.url_template.render(**project_vm_lock_client.url_kwargs),
        json=[{"vm_config_id": 0}, {"vm_config_id": 1}, {"vm_config_id": 1}],
    )
    assert project_vm_lock_client.get_vm_availabilities() == {
        0: 1,
        1: 2,
    }

    requests_mock.get(
        project_vm_lock_client.url_template.render(**project_vm_lock_client.url_kwargs),
        status_code=404,
    )
    with pytest.raises(typer.Exit):
        project_vm_lock_client.get_vm_availabilities()
