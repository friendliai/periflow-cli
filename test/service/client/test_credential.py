# Copyright (C) 2022 FriendliAI

"""Test CredentialClient Service"""


from __future__ import annotations

from copy import deepcopy

import pytest
import requests_mock
import typer

from pfcli.service import CredType, ServiceType
from pfcli.service.client import build_client
from pfcli.service.client.credential import (
    CredentialClientService,
    CredentialTypeClientService,
)


@pytest.fixture
def credential_client() -> CredentialClientService:
    return build_client(ServiceType.CREDENTIAL)


@pytest.fixture
def credential_type_client() -> CredentialTypeClientService:
    return build_client(ServiceType.CREDENTIAL_TYPE)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_credential_client_get_credential(
    requests_mock: requests_mock.Mocker, credential_client: CredentialClientService
):
    assert isinstance(credential_client, CredentialClientService)

    # Success
    url_template = deepcopy(credential_client.url_template)
    url_template.attach_pattern("$credential_id")
    requests_mock.get(
        url_template.render(credential_id=0),
        json={"id": 0, "name": "my-docker-secret", "type": "docker"},
    )
    assert credential_client.get_credential(0) == {
        "id": 0,
        "name": "my-docker-secret",
        "type": "docker",
    }

    # Failed due to HTTP error
    requests_mock.get(url_template.render(credential_id=0), status_code=404)
    with pytest.raises(typer.Exit):
        credential_client.get_credential(0)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_credential_client_update_credential(
    requests_mock: requests_mock.Mocker, credential_client: CredentialClientService
):
    assert isinstance(credential_client, CredentialClientService)

    # Success
    url_template = deepcopy(credential_client.url_template)
    url_template.attach_pattern("$credential_id")
    requests_mock.patch(
        url_template.render(credential_id=0),
        json={"id": 0, "name": "my-docker-secret", "type": "docker"},
    )
    assert credential_client.update_credential(
        0, name="my-docker-secret", type_version=1, value={"k": "v"}
    ) == {"id": 0, "name": "my-docker-secret", "type": "docker"}
    assert credential_client.update_credential(0) == {  # no updated field
        "id": 0,
        "name": "my-docker-secret",
        "type": "docker",
    }

    # Failed due to HTTP error
    requests_mock.patch(url_template.render(credential_id=0), status_code=404)
    with pytest.raises(typer.Exit):
        credential_client.update_credential(
            0, name="my-gcs-secret", type_version=1, value={"k": "v"}
        )


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_credential_client_delete_credential(
    requests_mock: requests_mock.Mocker, credential_client: CredentialClientService
):
    assert isinstance(credential_client, CredentialClientService)

    # Success
    url_template = deepcopy(credential_client.url_template)
    url_template.attach_pattern("$credential_id")
    requests_mock.delete(url_template.render(credential_id=0), status_code=204)
    try:
        credential_client.delete_credential(0)
    except typer.Exit:
        raise pytest.fail("Credential delete test failed.")

    # Failed due to HTTP error
    requests_mock.delete(url_template.render(credential_id=0), status_code=404)
    with pytest.raises(typer.Exit):
        credential_client.delete_credential(0)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_credential_type_client_get_schema_by_type(
    requests_mock: requests_mock.Mocker,
    credential_type_client: CredentialTypeClientService,
):
    assert isinstance(credential_type_client, CredentialTypeClientService)

    data = [
        {
            "type_name": "docker",
            "versions": [
                {
                    "type_version": 1,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "username": {"type": "string"},
                            "password": {"type": "string"},
                        },
                        "required": ["username", "password"],
                    },
                }
            ],
        },
        {
            "type_name": "aws",
            "versions": [
                {
                    "type_version": 1,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "aws_access_key_id": {"type": "string", "minLength": 1},
                            "aws_secret_access_key": {"type": "string", "minLength": 1},
                            "aws_default_region": {
                                "type": "string",
                                "examples": [
                                    "us-east-1",
                                    "us-east-2",
                                    "us-west-1",
                                    "us-west-2",
                                    "eu-west-1",
                                    "eu-central-1",
                                    "ap-northeast-1",
                                    "ap-northeast-2",
                                    "ap-southeast-1",
                                    "ap-southeast-2",
                                    "ap-south-1",
                                    "sa-east-1",
                                ],
                            },
                        },
                        "required": [
                            "aws_access_key_id",
                            "aws_secret_access_key",
                            "aws_default_region",
                        ],
                    },
                }
            ],
        },
        {
            "type_name": "gcp",
            "versions": [
                {
                    "type_version": 1,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string", "default": "service_account"},
                            "project_id": {"type": "string", "minLength": 1},
                            "private_key_id": {"type": "string", "minLength": 1},
                            "private_key": {"type": "string", "minLength": 1},
                            "client_email": {"type": "string", "minLength": 1},
                            "client_id": {"type": "string", "minLength": 1},
                            "auth_uri": {"type": "string", "minLength": 1},
                            "token_uri": {"type": "string", "minLength": 1},
                            "auth_provider_x509_cert_url": {
                                "type": "string",
                                "minLength": 1,
                            },
                            "client_x509_cert_url": {"type": "string", "minLength": 1},
                        },
                        "required": [
                            "project_id",
                            "private_key_id",
                            "private_key",
                            "client_email",
                            "client_id",
                            "auth_uri",
                            "token_uri",
                            "auth_provider_x509_cert_url",
                            "client_x509_cert_url",
                        ],
                    },
                }
            ],
        },
        {
            "type_name": "azure.blob",
            "versions": [
                {
                    "type_version": 1,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "storage_account_name": {
                                "type": "string",
                                "minLength": 3,
                                "maxLength": 24,
                            },
                            "storage_account_key": {"type": "string", "minLength": 1},
                        },
                        "required": ["storage_account_name", "storage_account_key"],
                    },
                }
            ],
        },
        {
            "type_name": "slack",
            "versions": [
                {
                    "type_version": 1,
                    "schema": {
                        "type": "object",
                        "properties": {"token": {"type": "string"}},
                        "required": ["token"],
                    },
                }
            ],
        },
    ]

    # Success
    requests_mock.get(credential_type_client.url_template.render(), json=data)
    assert (
        credential_type_client.get_schema_by_type(CredType.DOCKER)
        == data[0]["versions"][-1]["schema"]
    )
    assert (
        credential_type_client.get_schema_by_type(CredType.S3)
        == data[1]["versions"][-1]["schema"]
    )
    assert (
        credential_type_client.get_schema_by_type(CredType.GCS)
        == data[2]["versions"][-1]["schema"]
    )
    assert (
        credential_type_client.get_schema_by_type(CredType.BLOB)
        == data[3]["versions"][-1]["schema"]
    )
    assert (
        credential_type_client.get_schema_by_type(CredType.SLACK)
        == data[4]["versions"][-1]["schema"]
    )
    assert credential_type_client.get_schema_by_type(CredType.WANDB) is None

    # Failed due to HTTP error
    requests_mock.get(credential_type_client.url_template.render(), status_code=404)
    with pytest.raises(typer.Exit):
        credential_type_client.get_schema_by_type(CredType.DOCKER)
