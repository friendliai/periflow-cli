# Copyright (C) 2022 FriendliAI

"""Test CheckpointClient Service"""

from copy import deepcopy
from uuid import UUID

import pytest
import requests_mock
import typer

from pfcli.service import ServiceType
from pfcli.service.client import build_client
from pfcli.service.client.checkpoint import CheckpointClientService


@pytest.fixture
def checkpoint_client() -> CheckpointClientService:
    return build_client(ServiceType.CHECKPOINT)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_checkpoint_client_get_checkpoint(requests_mock: requests_mock.Mocker,
                                          checkpoint_client: CheckpointClientService):
    assert isinstance(checkpoint_client, CheckpointClientService)

    url_template = deepcopy(checkpoint_client.url_template)
    url_template.attach_pattern('$checkpoint_id/')

    # Success
    requests_mock.get(url_template.render(checkpoint_id=0), json={'id': 0})
    assert checkpoint_client.get_checkpoint(0) == {'id': 0}

    # Failed due to HTTP error
    requests_mock.get(url_template.render(checkpoint_id=0), status_code=404)
    with pytest.raises(typer.Exit):
        assert checkpoint_client.get_checkpoint(0)



@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_checkpoint_client_delete_checkpoint(requests_mock: requests_mock.Mocker,
                                             checkpoint_client: CheckpointClientService):
    assert isinstance(checkpoint_client, CheckpointClientService)

    url_template = deepcopy(checkpoint_client.url_template)
    url_template.attach_pattern('$checkpoint_id/')

    # Success
    requests_mock.delete(url_template.render(checkpoint_id=0), status_code=204)
    try:
        checkpoint_client.delete_checkpoint(0)
    except typer.Exit:
        raise pytest.fail("Checkpoint delete test failed.")

    # Failed due to HTTP error
    requests_mock.delete(url_template.render(checkpoint_id=0), status_code=404)
    with pytest.raises(typer.Exit):
        assert checkpoint_client.delete_checkpoint(0)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_checkpoint_client_get_checkpoint_download_urls(requests_mock: requests_mock.Mocker,
                                                        checkpoint_client: CheckpointClientService):
    assert isinstance(checkpoint_client, CheckpointClientService)

    base_url = checkpoint_client.url_template.get_base_url()

    data = {
        "files": [
            {
                "name": "new_ckpt_1000.pth",
                "path": "ckpt/new_ckpt_1000.pth",
                "mtime": "2022-04-20T06:27:37.907Z",
                "size": 2048,
                "download_url": "https://s3.download.url.com"
            }
        ]
    }

    # Success
    requests_mock.get(
        f"{base_url}/models/ffffffff-ffff-ffff-ffff-ffffffffffff/",
        # Subset of response
        json={
            "id": "ffffffff-ffff-ffff-ffff-ffffffffffff",
            "forms": [{"id": "cccccccc-cccc-cccc-cccc-cccccccccccc"}, {"id": "dddddddd-dddd-dddd-dddd-dddddddddddd"}],
        }
    )
    requests_mock.get(
        f"{base_url}/model_forms/cccccccc-cccc-cccc-cccc-cccccccccccc/download/",
        json=data
    )
    assert checkpoint_client.get_checkpoint_download_urls(UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")) == data['files']

    # Failed due to HTTP error
    requests_mock.get(f"{base_url}/models/ffffffff-ffff-ffff-ffff-ffffffffffff/", status_code=404)
    with pytest.raises(typer.Exit):
        assert checkpoint_client.get_checkpoint_download_urls(UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"))
