# Copyright (C) 2021 FriendliAI

import pytest 
import requests_mock

from pfcli.utils import get_uri


@pytest.fixture
def patch_auto_token_refresh(requests_mock: requests_mock.Mocker):
    requests_mock.post(get_uri('token/refresh'))
