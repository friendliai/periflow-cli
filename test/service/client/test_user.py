# Copyright (C) 2022 FriendliAI

"""Test UserClient Service"""

import uuid
from copy import deepcopy

import pytest
import requests_mock
import typer

from pfcli.service import GroupRole, ProjectRole, ServiceType
from pfcli.service.client import build_client
from pfcli.service.client.user import (
    UserClientService, 
    UserGroupClientService,
    UserMFAService
)


@pytest.fixture
def user_client(user_project_group_context) -> UserClientService:
    return build_client(ServiceType.USER)


@pytest.fixture
def user_group_client(user_project_group_context) -> UserGroupClientService:
    return build_client(ServiceType.USER_GROUP)


@pytest.fixture
def user_mfa() -> UserMFAService:
    return build_client(ServiceType.MFA)


def test_user_initiate_mfa(
    requests_mock: requests_mock.Mocker, user_mfa: UserMFAService
):
    # Success
    url_template = deepcopy(user_mfa.url_template)
    url_template.attach_pattern("challenge/$mfa_type")
    requests_mock.post(url_template.render(mfa_type="totp"), status_code=204)
    try:
        user_mfa.initiate_mfa(mfa_type="totp", mfa_token="MFA_TOKEN")
    except typer.Exit:
        raise pytest.fail("Test initiate MFA failed.")

    # Failed
    requests_mock.post(url_template.render(mfa_type="totp"), status_code=400)
    with pytest.raises(typer.Exit):
        user_mfa.initiate_mfa(mfa_type="totp", mfa_token="MFA_TOKEN")


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_user_client_change_password(
    requests_mock: requests_mock.Mocker, user_client: UserClientService
):
    # Success
    url_template = deepcopy(user_client.url_template)
    url_template.attach_pattern(f"{user_client.user_id}/password")
    requests_mock.put(url_template.render(**user_client.url_kwargs), status_code=204)
    try:
        user_client.change_password("1234", "5678")
    except typer.Exit:
        raise pytest.fail("Test change password failed.")

    # Failed
    requests_mock.put(url_template.render(**user_client.url_kwargs), status_code=400)
    with pytest.raises(typer.Exit):
        user_client.change_password("1234", "5678")


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_user_client_set_group_privilege(
    requests_mock: requests_mock.Mocker, user_client: UserClientService
):
    # Success
    url_template = deepcopy(user_client.url_template)
    url_template.attach_pattern("$pf_user_id/pf_group/$pf_group_id/privilege_level")

    user_id = str(uuid.uuid4())
    group_id = str(uuid.uuid4())

    requests_mock.patch(
        url_template.render(pf_user_id=user_id, pf_group_id=group_id), status_code=204
    )
    try:
        user_client.set_group_privilege(group_id, user_id, GroupRole.OWNER)
    except typer.Exit:
        raise pytest.fail("Test set group privilege failed.")

    # Failed
    requests_mock.patch(
        url_template.render(pf_user_id=user_id, pf_group_id=group_id), status_code=404
    )
    with pytest.raises(typer.Exit):
        user_client.set_group_privilege(group_id, user_id, GroupRole.OWNER)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_user_client_get_project_membership(
    requests_mock: requests_mock.Mocker, user_client: UserClientService
):
    # Success
    url_template = deepcopy(user_client.url_template)
    url_template.attach_pattern(f"{user_client.user_id}/pf_project/$pf_project_id")

    project_id = str(uuid.uuid4())
    user_data = {
        "id": str(user_client.user_id),
        "name": "test",
        "access_level": "admin",
        "created_at": "2022-06-30T06:30:46.896Z",
        "updated_at": "2022-06-30T06:30:46.896Z",
    }

    requests_mock.get(url_template.render(pf_project_id=project_id), json=user_data)

    assert user_client.get_project_membership(project_id) == user_data

    # Failed
    requests_mock.get(url_template.render(pf_project_id=project_id), status_code=404)
    with pytest.raises(typer.Exit):
        user_client.get_project_membership(project_id)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_user_client_add_to_project(
    requests_mock: requests_mock.Mocker, user_client: UserClientService
):
    # Success
    url_template = deepcopy(user_client.url_template)
    url_template.attach_pattern("$pf_user_id/pf_project/$pf_project_id")

    user_id = str(uuid.uuid4())
    project_id = str(uuid.uuid4())

    requests_mock.post(
        url_template.render(pf_user_id=user_id, pf_project_id=project_id),
        status_code=204,
    )
    try:
        user_client.add_to_project(user_id, project_id, ProjectRole.ADMIN)
    except typer.Exit:
        raise pytest.fail("Test add to project failed.")

    # Failed
    requests_mock.post(
        url_template.render(pf_user_id=user_id, pf_project_id=project_id),
        status_code=404,
    )
    with pytest.raises(typer.Exit):
        user_client.add_to_project(user_id, project_id, ProjectRole.ADMIN)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_user_client_set_project_privilege(
    requests_mock: requests_mock.Mocker, user_client: UserClientService
):
    # Success
    url_template = deepcopy(user_client.url_template)
    url_template.attach_pattern("$pf_user_id/pf_project/$pf_project_id/access_level")

    user_id = str(uuid.uuid4())
    project_id = str(uuid.uuid4())

    requests_mock.patch(
        url_template.render(pf_user_id=user_id, pf_project_id=project_id),
        status_code=204,
    )
    try:
        user_client.set_project_privilege(user_id, project_id, ProjectRole.ADMIN)
    except typer.Exit:
        raise pytest.fail("Test set project privilege failed.")

    # Failed
    requests_mock.patch(
        url_template.render(pf_user_id=user_id, pf_project_id=project_id),
        status_code=404,
    )
    with pytest.raises(typer.Exit):
        user_client.set_project_privilege(user_id, project_id, ProjectRole.ADMIN)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_user_group_client_get_group_info(
    requests_mock: requests_mock.Mocker, user_group_client: UserGroupClientService
):
    assert isinstance(user_group_client, UserGroupClientService)

    # Success
    url_template = deepcopy(user_group_client.url_template)
    # url_template.attach_pattern('group/')
    requests_mock.get(
        url_template.render(**user_group_client.url_kwargs),
        json=[{"id": "00000000-0000-0000-0000-000000000000", "name": "my-group"}],
    )
    assert user_group_client.get_group_info() == [
        {"id": "00000000-0000-0000-0000-000000000000", "name": "my-group"}
    ]

    # Failed
    requests_mock.get(
        url_template.render(**user_group_client.url_kwargs), status_code=404
    )
    with pytest.raises(typer.Exit):
        user_group_client.get_group_info()
