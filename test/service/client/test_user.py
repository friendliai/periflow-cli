# Copyright (C) 2022 FriendliAI

"""Test UserClient Service"""

from copy import deepcopy

import pytest
import requests_mock
import typer

from pfcli.service import ServiceType
from pfcli.service.client import build_client
from pfcli.service.client.user import UserClientService, UserGroupClientService


@pytest.fixture
def user_client(user_project_group_context) -> UserClientService:
    return build_client(ServiceType.USER)


@pytest.fixture
def user_group_client(user_project_group_context) -> UserGroupClientService:
    return build_client(ServiceType.USER_GROUP)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_user_client_change_password(requests_mock: requests_mock.Mocker,
                                     user_client: UserClientService):
    # Success
    url_template = deepcopy(user_client.url_template)
    url_template.attach_pattern(f'{user_client.user_id}/password')
    requests_mock.put(url_template.render(**user_client.url_kwargs), status_code=204)
    try:
        user_client.change_password('1234', '5678')
    except typer.Exit:
        raise pytest.fail("Test change password failed.")

    # Failed
    requests_mock.put(url_template.render(**user_client.url_kwargs), status_code=400)
    with pytest.raises(typer.Exit):
        user_client.change_password('1234', '5678')


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_user_group_client_get_group_info(requests_mock: requests_mock.Mocker,
                                          user_group_client: UserGroupClientService):
    assert isinstance(user_group_client, UserGroupClientService)

    # Success
    url_template = deepcopy(user_group_client.url_template)
    # url_template.attach_pattern('group/')
    requests_mock.get(
        url_template.render(**user_group_client.url_kwargs),
        json=[
            {
                'id': '00000000-0000-0000-0000-000000000000',
                'name': 'my-group'
            }
        ]
    )
    assert user_group_client.get_group_info() == [
        {
            'id': '00000000-0000-0000-0000-000000000000',
            'name': 'my-group'
        }
    ]

    # Failed
    requests_mock.get(url_template.render(**user_group_client.url_kwargs), status_code=404)
    with pytest.raises(typer.Exit):
        user_group_client.get_group_info()
