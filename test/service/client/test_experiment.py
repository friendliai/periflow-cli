# Copyright (C) 2022 FriendliAI

"""Test ExperimentClient Service"""


from copy import deepcopy

import pytest
import requests_mock
import typer

from pfcli.service import ServiceType
from pfcli.service.client import build_client
from pfcli.service.client.experiment import ExperimentClientService


@pytest.fixture
def experiment_client() -> ExperimentClientService:
    return build_client(ServiceType.EXPERIMENT)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_experiment_client_list_jobs_in_experiment(requests_mock: requests_mock.Mocker,
                                                   experiment_client: ExperimentClientService):
    assert isinstance(experiment_client, ExperimentClientService)

    # Success
    url_template = deepcopy(experiment_client.url_template)
    url_template.attach_pattern('$experiment_id/job/')
    requests_mock.get(
        url_template.render(experiment_id=1),
        json={'results': [{'id': 0}, {'id': 1}]}
    )
    assert experiment_client.list_jobs_in_experiment(1) == [{'id': 0}, {'id': 1}]

    # Failed due to HTTP error
    requests_mock.get(url_template.render(experiment_id=1), status_code=404)
    with pytest.raises(typer.Exit):
        experiment_client.list_jobs_in_experiment(1)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_experiment_client_delete_experiment(requests_mock: requests_mock.Mocker,
                                             experiment_client: ExperimentClientService):
    assert isinstance(experiment_client, ExperimentClientService)

    # Success
    url_template = deepcopy(experiment_client.url_template)
    url_template.attach_pattern('$experiment_id/')
    requests_mock.delete(url_template.render(experiment_id=1), status_code=204)
    try:
        experiment_client.delete_experiment(1)
    except typer.Exit:
        raise pytest.fail("Test delete experiment failed.")

    # Failed due to HTTP error
    requests_mock.delete(url_template.render(experiment_id=1), status_code=404)
    with pytest.raises(typer.Exit):
        experiment_client.delete_experiment(1)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_experiment_client_update_experiment(requests_mock: requests_mock.Mocker,
                                             experiment_client: ExperimentClientService):
    assert isinstance(experiment_client, ExperimentClientService)

    # Success
    url_template = deepcopy(experiment_client.url_template)
    url_template.attach_pattern('$experiment_id/')
    requests_mock.patch(url_template.render(experiment_id=1), json={'id': 0, 'name': 'my-exp'})
    assert experiment_client.update_experiment_name(1, 'my-exp') == {'id': 0, 'name': 'my-exp'}

    # Failed due to HTTP error
    requests_mock.patch(url_template.render(experiment_id=1), status_code=404)
    with pytest.raises(typer.Exit):
        experiment_client.update_experiment_name(1, 'new-exp')
