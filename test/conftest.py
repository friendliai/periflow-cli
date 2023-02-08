# Copyright (C) 2021 FriendliAI

from __future__ import annotations

import uuid
from unittest.mock import patch

import pytest
import requests_mock

from pfcli.utils.url import get_uri


@pytest.fixture
def patch_auto_token_refresh(requests_mock: requests_mock.Mocker):
    requests_mock.post(get_uri("token/refresh"))


@pytest.fixture
def user_project_group_context():
    with patch(
        "pfcli.service.client.base.UserRequestMixin.get_current_user_id",
        return_value=uuid.UUID("22222222-2222-2222-2222-222222222222"),
    ), patch(
        "pfcli.service.client.base.get_current_group_id",
        return_value=uuid.UUID("00000000-0000-0000-0000-000000000000"),
    ), patch(
        "pfcli.service.client.base.get_current_project_id",
        return_value=uuid.UUID("11111111-1111-1111-1111-111111111111"),
    ):
        yield
