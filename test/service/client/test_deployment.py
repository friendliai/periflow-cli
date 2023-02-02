# Copyright (C) 2022 FriendliAI

"""Test DeploymentClient Service"""

import pytest
import requests_mock
import typer

from pfcli.service import DeploymentType, ServiceType
from pfcli.service.client import build_client
from pfcli.service.client.deployment import (
    DeploymentClientService,
    DeploymentMetricsClientService,
    PFSProjectUsageClientService,
)
from datetime import datetime


@pytest.fixture
def deployment_client() -> DeploymentClientService:
    return build_client(ServiceType.DEPLOYMENT)


@pytest.fixture
def deployment_metrics_client() -> DeploymentMetricsClientService:
    return build_client(ServiceType.DEPLOYMENT_METRICS)


@pytest.fixture
def project_usage_client(user_project_group_context) -> PFSProjectUsageClientService:
    return build_client(ServiceType.PFS_PROJECT_USAGE)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_deployment_client_get_deployment(
    requests_mock: requests_mock.Mocker, deployment_client: DeploymentClientService
):
    assert isinstance(deployment_client, DeploymentClientService)

    # Success
    requests_mock.get(
        deployment_client.url_template.render(
            **deployment_client.url_kwargs,
            pk=1,
        ),
        json={"id": "periflow-deployment-05246a6e"},
    )
    assert deployment_client.get_deployment(1) == {"id": "periflow-deployment-05246a6e"}

    # Failed due to HTTP error
    requests_mock.get(
        deployment_client.url_template.render(
            **deployment_client.url_kwargs,
            pk=1,
        ),
        status_code=404,
    )
    with pytest.raises(typer.Exit):
        deployment_client.get_deployment(1)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_deployment_client_list_deployment(
    requests_mock: requests_mock.Mocker, deployment_client: DeploymentClientService
):
    assert isinstance(deployment_client, DeploymentClientService)
    results = {
        "deployments": [
            {"id": 1, "config": {"name": "one", "gpu_type": "t4", "total_gpus": 1}},
            {"id": 2, "config": {"name": "two", "gpu_type": "t4", "total_gpus": 2}},
        ]
    }

    # Success
    requests_mock.get(
        deployment_client.url_template.render(
            **deployment_client.url_kwargs,
            params={"project_id": "22222222-2222-2222-2222-222222222222"},
        ),
        json=results,
    )
    assert deployment_client.list_deployments(project_id=1) == results

    # Failed due to HTTP error
    requests_mock.get(
        deployment_client.url_template.render(
            **deployment_client.url_kwargs,
            params={"project_id": "22222222-2222-2222-2222-222222222222"},
        ),
        status_code=404,
    )
    with pytest.raises(typer.Exit):
        deployment_client.list_deployments(project_id=1)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_deployment_client_create_deployment(
    requests_mock: requests_mock.Mocker, deployment_client: DeploymentClientService
):
    assert isinstance(deployment_client, DeploymentClientService)
    result = {"id": "1", "endpoint": "https://friendli.ai/test/endpoint/"}

    config = {
        "project_id": "22222222-2222-2222-2222-222222222222",
        "model_id": "ffffffff-ffff-ffff-ffff-ffffffffffff",
        "deployment_type": DeploymentType.DEVELOPMENT,
        "name": "test_deployment",
        "gpu_type": "t4",
        "cloud": "aws",
        "region": "test_region",
    }

    # Success
    requests_mock.post(
        deployment_client.url_template.render(
            **deployment_client.url_kwargs,
            config=config,
        ),
        json=result,
    )
    assert deployment_client.create_deployment(config) == result

    # Failed due to HTTP error
    requests_mock.post(
        deployment_client.url_template.render(
            **deployment_client.url_kwargs,
            config=config,
        ),
        status_code=404,
    )
    with pytest.raises(typer.Exit):
        deployment_client.create_deployment(config)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_deployment_client_delete_deployment(
    requests_mock: requests_mock.Mocker, deployment_client: DeploymentClientService
):
    assert isinstance(deployment_client, DeploymentClientService)

    # Success
    requests_mock.delete(
        deployment_client.url_template.render(**deployment_client.url_kwargs, pk=1),
    )
    deployment_client.delete_deployment(1)

    # Failed due to HTTP error
    requests_mock.delete(
        deployment_client.url_template.render(**deployment_client.url_kwargs, pk=1),
        status_code=404,
    )
    with pytest.raises(typer.Exit):
        deployment_client.delete_deployment(1)


@pytest.mark.skip
@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_deployment_metrics_client(
    requests_mock: requests_mock.Mocker,
    deployment_metrics_client: DeploymentMetricsClientService,
):
    # TODO: Add testcase.
    pass


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_deployment_usage_client(
    requests_mock: requests_mock.Mocker,
    project_usage_client: PFSProjectUsageClientService,
):
    assert isinstance(project_usage_client, PFSProjectUsageClientService)

    result = {
        "periflow-deployment-05246a6e": {
            "deployment_type": "dev",
            "duration": "13",
        }
    }

    # Success
    requests_mock.get(
        project_usage_client.url_template.render(
            **project_usage_client.url_kwargs,
        ),
        json=result,
    )

    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 2, 1)
    assert project_usage_client.get_usage(start_date, end_date) == result

    # Failed due to HTTP error
    requests_mock.get(
        project_usage_client.url_template.render(
            **project_usage_client.url_kwargs,
        ),
        status_code=404,
    )
    with pytest.raises(typer.Exit):
        project_usage_client.get_usage(start_date, end_date)
