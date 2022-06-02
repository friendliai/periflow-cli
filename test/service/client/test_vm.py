# Copyright (C) 2022 FriendliAI

"""Test VMClient Service"""

from copy import deepcopy

import pytest
import requests_mock
import typer

from pfcli.service import ServiceType
from pfcli.service.client import build_client
from pfcli.service.client.vm import VMConfigClientService


@pytest.fixture
def vm_config_client() -> VMConfigClientService:
    return build_client(ServiceType.VM_CONFIG)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_vm_config_client_get_active_vm_count(requests_mock: requests_mock.Mocker,
                                              vm_config_client: VMConfigClientService):
    assert isinstance(vm_config_client, VMConfigClientService)

    # Success
    url_template = deepcopy(vm_config_client.url_template)
    url_template.attach_pattern('$vm_config_id/vm_lock/')
    requests_mock.get(
        url_template.render(vm_config_id=0),
        json=[
            {'lock_type': 'active', 'vm_config_id': 0, 'job_id': 0},
            {'lock_type': 'active', 'vm_config_id': 0, 'job_id': 0},
            {'lock_type': 'active', 'vm_config_id': 0, 'job_id': 1},
            {'lock_type': 'active', 'vm_config_id': 0, 'job_id': 2},
        ]
    )
    assert vm_config_client.get_active_vm_count(0) == 4

    # Failed due to HTTP error
    requests_mock.get(url_template.render(vm_config_id=0), status_code=404)
    with pytest.raises(typer.Exit):
        vm_config_client.get_active_vm_count(0)
