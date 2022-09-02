# Copyright (C) 2022 FriendliAI

"""Test ServeClient Service"""


from copy import deepcopy

import pytest
import requests_mock
import typer

from pfcli.service import ServiceType
from pfcli.service.client import build_client
from pfcli.service.client.serve import ServeClientService


@pytest.fixture
def serve_client() -> ServeClientService:
    return build_client(ServiceType.SERVE)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_serve_client_get_serve(
    requests_mock: requests_mock.Mocker, serve_client: ServeClientService
):
    assert isinstance(serve_client, ServeClientService)

    url_template = deepcopy(serve_client.url_template)
    url_template.attach_pattern("$serve_id/")

    # Success
    requests_mock.get(
        url_template.render(serve_id=0),
        json={
            "id": 0,
            "name": "name",
            "status": "running",
            "vm": "aws-g4dn.12xlarge",
            "gpu_type": "t4",
            "num_gpus": 1,
            "start": "2022-04-18T05:55:14.365021Z",
            "endpoint": "http:0.0.0.0:8000/0/v1/completions",
        },
    )
    assert serve_client.get_serve(0) == {
        "id": 0,
        "name": "name",
        "status": "running",
        "vm": "aws-g4dn.12xlarge",
        "gpu_type": "t4",
        "num_gpus": 1,
        "start": "2022-04-18T05:55:14.365021Z",
        "endpoint": "http:0.0.0.0:8000/0/v1/completions",
    }

    requests_mock.get(url_template.render(serve_id=0), status_code=404)
    with pytest.raises(typer.Exit):
        serve_client.get_serve(0)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_serve_client_create_serve(
    requests_mock: requests_mock.Mocker, serve_client: ServeClientService
):
    assert isinstance(serve_client, ServeClientService)

    # Success
    requests_mock.post(
        serve_client.url_template.render(),
        json={
            "id": 0,
            "name": "name",
            "status": "running",
            "vm": "aws-g4dn.12xlarge",
            "gpu_type": "t4",
            "num_gpus": 1,
            "start": "2022-04-18T05:55:14.365021Z",
            "endpoint": "http:0.0.0.0:8000/0/v1/completions",
        },
    )

    assert serve_client.create_serve({}) == {
        "id": 0,
        "name": "name",
        "status": "running",
        "vm": "aws-g4dn.12xlarge",
        "gpu_type": "t4",
        "num_gpus": 1,
        "start": "2022-04-18T05:55:14.365021Z",
        "endpoint": "http:0.0.0.0:8000/0/v1/completions",
    }

    requests_mock.post(serve_client.url_template.render(), status_code=404)
    with pytest.raises(typer.Exit):
        serve_client.create_serve({})


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_serve_client_list_serve(
    requests_mock: requests_mock.Mocker, serve_client: ServeClientService
):
    assert isinstance(serve_client, ServeClientService)

    # Success
    requests_mock.get(
        serve_client.url_template.render(),
        json=[
            {
                "id": 0,
                "name": "name",
                "status": "running",
                "vm": "aws-g4dn.12xlarge",
                "gpu_type": "t4",
                "num_gpus": 1,
                "start": "2022-04-18T05:55:14.365021Z",
                "endpoint": "http:0.0.0.0:8000/0/v1/completions",
            }
        ],
    )

    assert serve_client.list_serves() == [
        {
            "id": 0,
            "name": "name",
            "status": "running",
            "vm": "aws-g4dn.12xlarge",
            "gpu_type": "t4",
            "num_gpus": 1,
            "start": "2022-04-18T05:55:14.365021Z",
            "endpoint": "http:0.0.0.0:8000/0/v1/completions",
        }
    ]

    requests_mock.get(serve_client.url_template.render(), status_code=404)
    with pytest.raises(typer.Exit):
        serve_client.list_serves()


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_serve_client_delete_serve(
    requests_mock: requests_mock.Mocker, serve_client: ServeClientService
):
    assert isinstance(serve_client, ServeClientService)

    url_template = deepcopy(serve_client.url_template)
    url_template.attach_pattern("$serve_id/")

    # Success
    requests_mock.delete(url_template.render(serve_id=0), status_code=204)
    try:
        serve_client.delete_serve(0)
    except typer.Exit:
        raise pytest.fail("serve client test failed.")

    requests_mock.delete(url_template.render(serve_id=0), status_code=404)
    with pytest.raises(typer.Exit):
        serve_client.delete_serve(0)
