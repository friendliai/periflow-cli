# Copyright (C) 2021 FriendliAI

"""Test Client Service"""

import json
from copy import deepcopy
from string import Template
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch

import pytest
import typer
import requests_mock
from websockets.client import WebSocketClientProtocol

from pfcli.service import (
    CloudType,
    LogType,
    ServiceType,
    StorageType,
)
from pfcli.service.auth import TokenType
from pfcli.service.client import (
    CheckpointClientService,
    ClientService,
    CredentialClientService,
    CredentialTypeClientService,
    DataClientService,
    ExperimentClientService,
    GroupCheckpointClinetService,
    GroupCredentialClientService,
    GroupDataClientService,
    GroupExperimentClientService,
    GroupJobClientService,
    GroupVMClientService,
    GroupVMQuotaClientService,
    JobArtifactClientService,
    JobCheckpointClientService,
    JobClientService,
    JobTemplateClientService,
    JobWebSocketClientService,
    URLTemplate,
    UserGroupClientService,
    build_client,
)


@pytest.fixture
def base_url() -> str:
    return 'https://test.periflow.com/'


@pytest.fixture
def user_group_client() -> UserGroupClientService:
    return build_client(ServiceType.USER_GROUP)


@pytest.fixture
def experiment_client() -> ExperimentClientService:
    return build_client(ServiceType.EXPERIMENT)


@pytest.fixture
def group_experiment_client() -> GroupExperimentClientService:
    return build_client(ServiceType.GROUP_EXPERIMENT)


@pytest.fixture
def job_client() -> JobClientService:
    return build_client(ServiceType.JOB)


@pytest.fixture
def job_checkpoint_client() -> JobCheckpointClientService:
    return build_client(ServiceType.JOB_CHECKPOINT, job_id=1)


@pytest.fixture
def job_artifact_client() -> JobArtifactClientService:
    return build_client(ServiceType.JOB_ARTIFACT, job_id=1)


@pytest.fixture
def group_job_client() -> GroupJobClientService:
    return build_client(ServiceType.GROUP_JOB)


@pytest.fixture
def job_template_client() -> JobTemplateClientService:
    return build_client(ServiceType.JOB_TEMPLATE)


@pytest.fixture
def credential_client() -> CredentialClientService:
    return build_client(ServiceType.CREDENTIAL)


@pytest.fixture
def group_credential_client() -> GroupCredentialClientService:
    return build_client(ServiceType.GROUP_CREDENTIAL)


@pytest.fixture
def credential_type_client() -> CredentialTypeClientService:
    return build_client(ServiceType.CREDENTIAL_TYPE)


@pytest.fixture
def data_client() -> DataClientService:
    return build_client(ServiceType.DATA)


@pytest.fixture
def group_data_client() -> GroupDataClientService:
    return build_client(ServiceType.GROUP_DATA)


@pytest.fixture
def group_vm_client() -> GroupVMClientService:
    return build_client(ServiceType.GROUP_VM)


@pytest.fixture
def group_vm_quota_client() -> GroupVMQuotaClientService:
    return build_client(ServiceType.GROUP_VM_QUOTA)


@pytest.fixture
def checkpoint_client() -> CheckpointClientService:
    return build_client(ServiceType.CHECKPOINT)


@pytest.fixture
def group_checkpoint_client() -> GroupCheckpointClinetService:
    return build_client(ServiceType.GROUP_CHECKPOINT)


@pytest.fixture
def job_ws_client() -> JobWebSocketClientService:
    return build_client(ServiceType.JOB_WS)


@pytest.fixture
@pytest.mark.usefixtures('patch_auto_token_refresh')
def patch_init_group(requests_mock: requests_mock.Mocker, user_group_client: UserGroupClientService):
    url_template = deepcopy(user_group_client.url_template)
    url_template.attach_pattern('group/')
    requests_mock.get(
        url_template.render(),
        json={
            'results': [
                {
                    'id': 0,
                    'name': 'my-group'
                }
            ]
        }
    )


def test_url_template_render(base_url: str):
    url_pattern = f'{base_url}test/'
    template = URLTemplate(Template(url_pattern))
    assert template.render() == url_pattern
    assert template.render(pk=1) == f"{url_pattern}1/"
    assert template.render(pk="abcd") == f"{url_pattern}abcd/"


def test_url_template_render_complex_pattern(base_url: str):
    url_pattern = f'{base_url}test/$test_id/job/'
    template = URLTemplate(Template(url_pattern))

    # Missing an url param
    with pytest.raises(KeyError):
        template.render()

    assert template.render(test_id=1) == f'{base_url}test/1/job/'
    assert template.render('abcd', test_id=1) == f'{base_url}test/1/job/abcd/'


def test_url_template_attach_pattern(base_url: str):
    url_pattern = f'{base_url}test/$test_id/job/'
    template = URLTemplate(Template(url_pattern))

    template.attach_pattern('$job_id/export/')

    with pytest.raises(KeyError):
        template.render(test_id=1)

    assert template.render(test_id=1, job_id='abcd') == f'{base_url}test/1/job/abcd/export/'
    assert template.render(0, test_id=1, job_id='abcd') == f'{base_url}test/1/job/abcd/export/0/'


def test_client_service_base(requests_mock: requests_mock.Mocker, base_url: str):
    url_pattern = f'{base_url}test/$test_id/job/'

    # Mock CRUD requests
    template = URLTemplate(Template(url_pattern))
    requests_mock.get(template.render(test_id=1), json=[{'data': 'value'}])
    requests_mock.get(template.render('abcd', test_id=1), json={'data': 'value'})
    requests_mock.post(template.render(test_id=1), json={'data': 'value'}, status_code=201)
    requests_mock.patch(template.render('abcd', test_id=1), json={'data': 'value'})
    requests_mock.delete(template.render('abcd', test_id=1), status_code=204)

    client = ClientService(Template(url_pattern), test_id=1)

    resp = client.list()
    assert resp.json() == [{'data': 'value'}]
    assert resp.status_code == 200

    resp = client.retrieve('abcd')
    assert resp.json() == {'data': 'value'}
    assert resp.status_code == 200

    resp = client.create()
    assert resp.json() == {'data': 'value'}
    assert resp.status_code == 201

    resp = client.partial_update('abcd')
    assert resp.json() == {'data': 'value'}
    assert resp.status_code == 200

    resp = client.delete('abcd')
    assert resp.status_code == 204


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_user_group_client_get_group_id(requests_mock: requests_mock.Mocker,
                                        user_group_client: UserGroupClientService):
    assert isinstance(user_group_client, UserGroupClientService)

    # Success
    url_template = deepcopy(user_group_client.url_template)
    url_template.attach_pattern('group/')
    requests_mock.get(
        url_template.render(),
        json={
            'results': [
                {
                    'id': 0,
                    'name': 'my-group'
                }
            ]
        }
    )
    assert user_group_client.get_group_id() == 0

    # Failed due to HTTP error
    requests_mock.get(url_template.render(), status_code=404)
    with pytest.raises(typer.Exit):
        user_group_client.get_group_id()

    # Failed due to no involved group
    requests_mock.get(
        url_template.render(),
        json={
            'results': []
        }
    )
    with pytest.raises(typer.Exit):
        user_group_client.get_group_id()

    # Failed due to multiple group (TODO: multi-group will be supported very soon.)
    requests_mock.get(
        url_template.render(),
        json={
            'results': [
                {
                    'id': 0,
                    'name': 'group-1'
                },
                {
                    'id': 1,
                    'name': 'group-2'
                }
            ]
        }
    )
    with pytest.raises(typer.Exit):
        user_group_client.get_group_id()


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_user_group_client_get_group_info(requests_mock: requests_mock.Mocker,
                                          user_group_client: UserGroupClientService):
    assert isinstance(user_group_client, UserGroupClientService)

    # Success
    url_template = deepcopy(user_group_client.url_template)
    url_template.attach_pattern('group/')
    requests_mock.get(
        url_template.render(),
        json={
            'results': [
                {
                    'id': 0,
                    'name': 'my-group'
                }
            ]
        }
    )
    assert user_group_client.get_group_info() == [
        {
            'id': 0,
            'name': 'my-group'
        }
    ]

    # Failed
    requests_mock.get(url_template.render(), status_code=404)
    with pytest.raises(typer.Exit):
        user_group_client.get_group_info()


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_user_group_client_get_user_info(requests_mock: requests_mock.Mocker,
                                         user_group_client: UserGroupClientService):
    assert isinstance(user_group_client, UserGroupClientService)

    # Success
    url_template = deepcopy(user_group_client.url_template)
    url_template.attach_pattern('self/')
    requests_mock.get(
        url_template.render(),
        json={
            'id': 0,
            'username': 'alice',
            'email': 'alice@periflow.com'
        }
    )
    assert user_group_client.get_user_info() == {
        'id': 0,
        'username': 'alice',
        'email': 'alice@periflow.com'
    }

    # Failed
    requests_mock.get(url_template.render(), status_code=404)
    with pytest.raises(typer.Exit):
        user_group_client.get_user_info()


@pytest.mark.usefixture('patch_auto_token_refresh')
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


@pytest.mark.usefixture('patch_auto_token_refresh')
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


@pytest.mark.usefixture('patch_auto_token_refresh')
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


@pytest.mark.usefixtures('patch_auto_token_refresh', 'patch_init_group')
def test_group_experiment_client_list_experiments(requests_mock: requests_mock.Mocker,
                                                  group_experiment_client: GroupExperimentClientService):
    assert isinstance(group_experiment_client, GroupExperimentClientService)

    # Success
    requests_mock.get(
        group_experiment_client.url_template.render(group_id=0),
        json=[{'id': 0, 'name': 'exp-0'}, {'id': 1, 'name': 'exp-1'}]
    )
    assert group_experiment_client.list_experiments() == [{'id': 0, 'name': 'exp-0'}, {'id': 1, 'name': 'exp-1'}]

    # Failed due to HTTP error
    requests_mock.get(group_experiment_client.url_template.render(group_id=0), status_code=404)
    with pytest.raises(typer.Exit):
        group_experiment_client.list_experiments()


@pytest.mark.usefixtures('patch_auto_token_refresh', 'patch_init_group')
def test_group_experiment_client_get_id_by_name(requests_mock: requests_mock.Mocker,
                                                group_experiment_client: GroupExperimentClientService):
    assert isinstance(group_experiment_client, GroupExperimentClientService)

    # Success
    requests_mock.get(
        group_experiment_client.url_template.render(group_id=0),
        json=[{'id': 0, 'name': 'exp-0'}, {'id': 1, 'name': 'exp-1'}]
    )
    assert group_experiment_client.get_id_by_name('exp-0') == 0
    assert group_experiment_client.get_id_by_name('exp-1') == 1
    assert group_experiment_client.get_id_by_name('exp-2') is None

    # Failed due to HTTP error
    requests_mock.get(group_experiment_client.url_template.render(group_id=0), status_code=404)
    with pytest.raises(typer.Exit):
        group_experiment_client.get_id_by_name('exp-3')


@pytest.mark.usefixtures('patch_auto_token_refresh', 'patch_init_group')
def test_group_experiment_client_create_experiment(requests_mock: requests_mock.Mocker,
                                                   group_experiment_client: GroupExperimentClientService):
    assert isinstance(group_experiment_client, GroupExperimentClientService)

    # Success
    requests_mock.post(
        group_experiment_client.url_template.render(group_id=0),
        json={'id': 0, 'name': 'exp-0'}
    )
    assert group_experiment_client.create_experiment('exp-0') == {'id': 0, 'name': 'exp-0'}

    # Failed due to HTTP error
    requests_mock.post(group_experiment_client.url_template.render(group_id=0), status_code=400)
    with pytest.raises(typer.Exit):
        group_experiment_client.create_experiment('exp-0')


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_job_client_list_jobs(requests_mock: requests_mock.Mocker, job_client: JobClientService):
    assert isinstance(job_client, JobClientService)

    # Success
    requests_mock.get(
        job_client.url_template.render(),
        json={'results': [{'id': 1}, {'id': 2}]}
    )
    assert job_client.list_jobs() == [{'id': 1}, {'id': 2}]

    # Failed due to HTTP error
    requests_mock.get(job_client.url_template.render(), status_code=404)
    with pytest.raises(typer.Exit):
        job_client.list_jobs()


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_job_client_get_job(requests_mock: requests_mock.Mocker, job_client: JobClientService):
    assert isinstance(job_client, JobClientService)

    # Success
    requests_mock.get(
        job_client.url_template.render(1),
        json={'id': 1}
    )
    assert job_client.get_job(1) == {'id': 1}

    # Failed due to HTTP error
    requests_mock.get(job_client.url_template.render(1), status_code=404)
    with pytest.raises(typer.Exit):
        job_client.get_job(1)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_job_client_run_job(requests_mock: requests_mock.Mocker, job_client: JobClientService):
    assert isinstance(job_client, JobClientService)

    # Success wo workspace dir
    requests_mock.post(
        job_client.url_template.render(),
        json={'id': 1}
    )
    assert job_client.run_job({'k': 'v'}, None) == {'id': 1}

    # Success w workspace dir
    with TemporaryDirectory() as dir:
        ws_dir = Path(dir)
        assert job_client.run_job({'k': 'v'}, ws_dir) == {'id': 1}

    # Failed due to HTTP error
    requests_mock.post(job_client.url_template.render(), status_code=500)
    with pytest.raises(typer.Exit):
        job_client.run_job({'k': 'v'}, None)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_job_client_cancel_job(requests_mock: requests_mock.Mocker, job_client: JobClientService):
    url_template = deepcopy(job_client.url_template)
    url_template.attach_pattern('$job_id/cancel/')

    # Success
    requests_mock.post(url_template.render(job_id=1))
    try:
        job_client.cancel_job(1)
    except typer.Exit as exc:
        raise pytest.failed(f"Test failed: {exc!r}") from exc

    # Failed
    requests_mock.post(url_template.render(job_id=1), status_code=500)
    with pytest.raises(typer.Exit):
        job_client.cancel_job(1)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_job_client_terminate_job(requests_mock: requests_mock.Mocker, job_client: JobClientService):
    url_template = deepcopy(job_client.url_template)
    url_template.attach_pattern('$job_id/terminate/')

    # Success
    requests_mock.post(url_template.render(job_id=1))
    try:
        job_client.terminate_job(1)
    except typer.Exit as exc:
        raise pytest.failed(f"Test failed: {exc!r}") from exc

    # Failed
    requests_mock.post(url_template.render(job_id=1), status_code=500)
    with pytest.raises(typer.Exit):
        job_client.terminate_job(1)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_job_client_get_text_logs(requests_mock: requests_mock.Mocker, job_client: JobClientService):
    url_template = deepcopy(job_client.url_template)
    url_template.attach_pattern('$job_id/text_log/')

    # Success
    requests_mock.get(
        url_template.render(job_id=1),
        json={
            'results': [
                {
                    'content': 'hello\n',
                    'timestamp': '2022-04-18T05:55:14.365021Z',
                    'type': 'stdout',
                    'node_rank': 0
                },
                {
                    'content': 'world\n',
                    'timestamp': '2022-04-18T05:55:14.364470Z',
                    'type': 'stdout',
                    'node_rank': 0
                }
            ]
        }
    )
    try:
        job_client.get_text_logs(1, 2)
    except typer.Exit as exc:
        raise pytest.failed(f"Test failed: {exc!r}") from exc

    # Failed
    requests_mock.get(url_template.render(job_id=1), status_code=500)
    with pytest.raises(typer.Exit):
        job_client.get_text_logs(1, 2)


@pytest.mark.asyncio
@pytest.mark.usefixtures('patch_auto_token_refresh')
async def test_job_ws_client(job_ws_client: JobWebSocketClientService):
    ws_mock = AsyncMock(WebSocketClientProtocol)
    with patch(
        'pfcli.service.client.get_token', return_value='ACCESS_TOKEN'
    ) as get_token_mock, patch(
        'websockets.connect', 
    ) as ws_connect_mock:
        ws_connect_mock.return_value.__aenter__.return_value = ws_mock
        ws_mock.recv.side_effect = [
            json.dumps(
                {
                    'response_type': 'subscribe',
                    'sources': [f'process.{x.value}' for x in LogType]
                }
            ),
            json.dumps(
                {
                    'content': 'hello\n',
                    'timestamp': '2022-04-18T05:55:14.365021Z',
                    'type': 'stdout',
                    'node_rank': 0
                }
            ),
            json.dumps(
                {
                    'content': 'world\n',
                    'timestamp': '2022-04-18T05:55:14.364470Z',
                    'type': 'stdout',
                    'node_rank': 0
                }
            )
        ]

        resp_list = []
        async with job_ws_client.open_connection(job_id=1, log_types=None, machines=None):
            async for resp in job_ws_client:
                resp_list.append(resp)

        get_token_mock.assert_called_once_with(TokenType.ACCESS)
        ws_connect_mock.assert_called_once_with(f'{job_ws_client.url_template.render(1)}?token=ACCESS_TOKEN')
        ws_connect_mock.return_value.__aenter__.assert_awaited_once()
        ws_connect_mock.return_value.__aexit__.assert_awaited_once()
        ws_mock.send.assert_awaited_once_with(
            json.dumps(
                {
                    'type': 'subscribe',
                    'sources': [f'process.{x.value}' for x in LogType],
                    'node_ranks': []
                }
            )
        )
        assert ws_mock.recv.call_count == 4

        assert resp_list[0] == {
            'content': 'hello\n',
            'timestamp': '2022-04-18T05:55:14.365021Z',
            'type': 'stdout',
            'node_rank': 0
        }
        assert resp_list[1] == {
            'content': 'world\n',
            'timestamp': '2022-04-18T05:55:14.364470Z',
            'type': 'stdout',
            'node_rank': 0
        }


@pytest.mark.asyncio
@pytest.mark.usefixtures('patch_auto_token_refresh')
async def test_job_ws_client_errors(job_ws_client: JobWebSocketClientService):
    ws_mock = AsyncMock(WebSocketClientProtocol)
    with patch(
        'pfcli.service.client.get_token', return_value='ACCESS_TOKEN'
    ) as get_token_mock, patch(
        'websockets.connect', 
    ) as ws_connect_mock:
        ws_connect_mock.return_value.__aenter__.return_value = ws_mock

        # Invalid json in ws reponse
        ws_mock.recv.side_effect = ['not_a_json']
        with pytest.raises(typer.Exit):
            async with job_ws_client.open_connection(job_id=1, log_types=None, machines=None):
                pass

        # Invalid ws response contents (invalid response_type)
        ws_mock.recv.side_effect = [
            json.dumps(
                {
                    'response_type': 'not_subscribe',
                    'sources': [f'process.{x.value}' for x in LogType]
                }
            )
        ]
        with pytest.raises(typer.Exit):
            async with job_ws_client.open_connection(job_id=1, log_types=None, machines=None):
                pass

        # Invalid ws response contents (sources are not matched)
        ws_mock.recv.side_effect = [
            json.dumps(
                {
                    'response_type': 'not_subscribe',
                    'sources': [f'process.{x.value}' for x in LogType]
                }
            )
        ]
        with pytest.raises(typer.Exit):
            async with job_ws_client.open_connection(job_id=1, log_types=[LogType.STDOUT], machines=None):
                pass

        # Errors while subscribing contents
        ws_mock.recv.side_effect = [
            json.dumps(
                {
                    'response_type': 'subscribe',
                    'sources': [f'process.{x.value}' for x in LogType]
                }
            ),
            'not_a_json'
        ]
        with pytest.raises(typer.Exit):
            async with job_ws_client.open_connection(job_id=1, log_types=None, machines=None):
                async for _ in job_ws_client:
                    pass


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_job_checkpoint_client_list_checkpoints(requests_mock: requests_mock.Mocker,
                                                job_checkpoint_client: JobCheckpointClientService):
    assert isinstance(job_checkpoint_client, JobCheckpointClientService)

    # Success
    requests_mock.get(
        job_checkpoint_client.url_template.render(job_id=1),
        json=[{'id': 1}]
    )
    assert job_checkpoint_client.list_checkpoints() == [{'id' : 1}]

    # Failed due to HTTP error
    requests_mock.get(job_checkpoint_client.url_template.render(job_id=1), status_code=404)
    with pytest.raises(typer.Exit):
        job_checkpoint_client.list_checkpoints()


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_job_artifact_client_list_artifacts(requests_mock: requests_mock.Mocker,
                                            job_artifact_client: JobCheckpointClientService):
    assert isinstance(job_artifact_client, JobArtifactClientService)

    # Success
    requests_mock.get(
        job_artifact_client.url_template.render(job_id=1),
        json=[{'id': 1}]
    )
    assert job_artifact_client.list_artifacts() == [{'id' : 1}]

    # Failed due to HTTP error
    requests_mock.get(job_artifact_client.url_template.render(job_id=1), status_code=404)
    with pytest.raises(typer.Exit):
        job_artifact_client.list_artifacts()


@pytest.mark.usefixtures('patch_auto_token_refresh', 'patch_init_group')
def test_group_job_client_list_jobs(requests_mock: requests_mock.Mocker,
                                    group_job_client: GroupJobClientService):
    assert isinstance(group_job_client, GroupJobClientService)

    # Success
    requests_mock.get(
        group_job_client.url_template.render(group_id=0),
        json={'results': [{'id': 0}, {'id': 1}]}
    )
    assert group_job_client.list_jobs() == [{'id' : 0}, {'id': 1}]

    # Failed due to HTTP error
    requests_mock.get(group_job_client.url_template.render(group_id=0), status_code=404)
    with pytest.raises(typer.Exit):
        group_job_client.list_jobs()


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_job_template_client_list_job_template_names(requests_mock: requests_mock.Mocker,
                                                     job_template_client: JobTemplateClientService):
    assert isinstance(job_template_client, JobTemplateClientService)

    # Success
    requests_mock.get(
        job_template_client.url_template.render(),
        json=[
            {
                'id': 0,
                'name': 'gpt-template',
            },
            {
                'id': 1,
                'name': 'bert-template'
            }
        ]
    )
    assert job_template_client.list_job_template_names() == ['gpt-template', 'bert-template']

    requests_mock.get(job_template_client.url_template.render(), status_code=404)
    with pytest.raises(typer.Exit):
        job_template_client.list_job_template_names()


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_job_template_client_get_job_template_by_name(requests_mock: requests_mock.Mocker,
                                                      job_template_client: JobTemplateClientService):
    assert isinstance(job_template_client, JobTemplateClientService)

    # Success
    requests_mock.get(
        job_template_client.url_template.render(),
        json=[
            {
                'id': 0,
                'name': 'gpt-template',
            },
            {
                'id': 1,
                'name': 'bert-template'
            }
        ]
    )
    assert job_template_client.get_job_template_by_name('gpt-template') == {
        'id': 0,
        'name': 'gpt-template'
    }
    assert job_template_client.get_job_template_by_name('bert-template') == {
        'id': 1,
        'name': 'bert-template'
    }
    assert job_template_client.get_job_template_by_name('dalle-template') is None

    requests_mock.get(job_template_client.url_template.render(), status_code=404)
    with pytest.raises(typer.Exit):
        job_template_client.get_job_template_by_name('some-template')


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_data_client_get_datastore(requests_mock: requests_mock.Mocker, data_client: DataClientService):
    assert isinstance(data_client, DataClientService)

    # Success
    requests_mock.get(data_client.url_template.render(0), json={'id': 0, 'name': 'cifar100'})
    assert data_client.get_datastore(0) == {'id': 0, 'name': 'cifar100'}

    # Failed due to HTTP error
    requests_mock.get(data_client.url_template.render(0), status_code=404)
    with pytest.raises(typer.Exit):
        data_client.get_datastore(0)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_data_client_update_datastore(requests_mock: requests_mock.Mocker, data_client: DataClientService):
    assert isinstance(data_client, DataClientService)

    # Success
    requests_mock.get(
        data_client.url_template.render(0),
        json={
            'id': 0,
            'name': 'cifar10',
            'vendor': 'aws',
            'region': 'us-west-2'
        }
    )
    requests_mock.patch(data_client.url_template.render(0), json={'id': 0, 'name': 'cifar100'})
    assert data_client.update_datastore(
        0,
        name='cifar100',
        vendor=StorageType.S3,
        region='us-east-1',
        storage_name='my-bucket',
        credential_id='f5609b48-5e7e-4431-81d3-23b141847211',
        metadata={'k': 'v'},
        files=[
            {'name': 'cifar100', 'path': '/path/to/cifar100'}
        ],
        active=True
    ) == {'id': 0, 'name': 'cifar100'}

    # Failed at region validation
    requests_mock.get(
        data_client.url_template.render(0),
        json={
            'id': 0,
            'name': 'cifar10',
            'vendor': 'aws',
            'region': 'us-west-2'
        }
    )
    with pytest.raises(typer.Exit):
        data_client.update_datastore(
            0,
            name='cifar100',
            vendor=StorageType.S3,
            region='busan',     # region not available in AWS S3
            storage_name='my-bucket',
            credential_id='f5609b48-5e7e-4431-81d3-23b141847211',
            metadata={'k': 'v'},
            files=[
                {'name': 'cifar100', 'path': '/path/to/cifar100'}
            ],
            active=True
        )

    # Failed due to HTTP error
    requests_mock.patch(data_client.url_template.render(0), status_code=400)
    with pytest.raises(typer.Exit):
        data_client.update_datastore(0)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_data_client_delete_datastore(requests_mock: requests_mock.Mocker, data_client: DataClientService):
    assert isinstance(data_client, DataClientService)

    # Success
    requests_mock.delete(data_client.url_template.render(0), status_code=204)
    try:
        data_client.delete_datastore(0)
    except typer.Exit:
        raise pytest.fail("Data client test failed.")

    # Failed due to HTTP error
    requests_mock.delete(data_client.url_template.render(0), status_code=404)
    with pytest.raises(typer.Exit):
        data_client.delete_datastore(0)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_data_client_get_upload_urls(requests_mock: requests_mock.Mocker, data_client: DataClientService):
    assert isinstance(data_client, DataClientService)

    url_template = deepcopy(data_client.url_template)
    url_template.attach_pattern('$datastore_id/upload/')

    # Success
    requests_mock.post(
        url_template.render(datastore_id=0),
        json=[
            {'path': '/path/to/local/file', 'upload_url': 'https://s3.bucket.com'}
        ]
    )
    with TemporaryDirectory() as dir:
        (Path(dir) / 'file').touch()
        assert data_client.get_upload_urls(0, Path(dir)) == [
            {'path': '/path/to/local/file', 'upload_url': 'https://s3.bucket.com'}
        ]

    # Failed when uploading empty directory.
    with TemporaryDirectory() as dir:
        with pytest.raises(typer.Exit):
            data_client.get_upload_urls(0, Path(dir))

    # Failed due to HTTP error
    requests_mock.post(url_template.render(datastore_id=0), status_code=500)
    with TemporaryDirectory() as dir:
        (Path(dir) / 'file').touch()
        with pytest.raises(typer.Exit):
            data_client.get_upload_urls(0, Path(dir))


@pytest.mark.usefixtures('patch_auto_token_refresh', 'patch_init_group')
def test_group_data_client_list_datastores(requests_mock: requests_mock.Mocker,
                                           group_data_client: GroupDataClientService):
    assert isinstance(group_data_client, GroupDataClientService)

    # Success
    requests_mock.get(
        group_data_client.url_template.render(group_id=0),
        json=[{'id': 0, 'name': 'wikitext'}, {'id': 1, 'name': 'imagenet'}]
    )
    assert group_data_client.list_datastores() == [{'id': 0, 'name': 'wikitext'}, {'id': 1, 'name': 'imagenet'}]

    # Failed due to HTTP error
    requests_mock.get(group_data_client.url_template.render(group_id=0), status_code=404)
    with pytest.raises(typer.Exit):
        group_data_client.list_datastores()


@pytest.mark.usefixtures('patch_auto_token_refresh', 'patch_init_group')
def test_group_data_client_get_id_by_name(requests_mock: requests_mock.Mocker,
                                          group_data_client: GroupDataClientService):
    assert isinstance(group_data_client, GroupDataClientService)

    # Success
    requests_mock.get(
        group_data_client.url_template.render(group_id=0),
        json=[{'id': 0, 'name': 'wikitext'}, {'id': 1, 'name': 'imagenet'}]
    )
    assert group_data_client.get_id_by_name('wikitext') == 0
    assert group_data_client.get_id_by_name('imagenet') == 1
    assert group_data_client.get_id_by_name('openwebtext') is None

    # Failed due to HTTP error
    requests_mock.get(group_data_client.url_template.render(group_id=0), status_code=404)
    with pytest.raises(typer.Exit):
        group_data_client.get_id_by_name('glue')


@pytest.mark.usefixtures('patch_auto_token_refresh', 'patch_init_group')
def test_group_data_client_create_datastore(requests_mock: requests_mock.Mocker,
                                            group_data_client: GroupDataClientService):
    assert isinstance(group_data_client, GroupDataClientService)

    # Success
    requests_mock.post(
        group_data_client.url_template.render(group_id=0),
        json={'id': 0, 'name': 'cifar100'}
    )
    assert group_data_client.create_datastore(
        name='cifar100',
        vendor=StorageType.FAI,
        region='',
        storage_name='',
        credential_id='f5609b48-5e7e-4431-81d3-23b141847211',
        metadata={'k': 'v'},
        files=[],
        active=False
    ) == {'id': 0, 'name': 'cifar100'}

    # Failed at region validation
    with pytest.raises(typer.Exit):
        group_data_client.create_datastore(
            name='cifar100',
            vendor=StorageType.FAI,
            region='us-east-1',   # not supported by FAI storage type
            storage_name='',
            credential_id='f5609b48-5e7e-4431-81d3-23b141847211',
            metadata={'k': 'v'},
            files=[],
            active=False
        )

    # Failed due to HTTP error
    requests_mock.post(group_data_client.url_template.render(group_id=0), status_code=400)
    with pytest.raises(typer.Exit):
        group_data_client.create_datastore(
            name='cifar100',
            vendor=StorageType.FAI,
            region='',
            storage_name='',
            credential_id='f5609b48-5e7e-4431-81d3-23b141847211',
            metadata={'k': 'v'},
            files=[],
            active=False
        )


@pytest.mark.usefixtures('patch_auto_token_refresh', 'patch_init_group')
def test_group_vm_client_get_id_by_name(requests_mock: requests_mock.Mocker,
                                        group_vm_client: GroupVMClientService):
    assert isinstance(group_vm_client, GroupVMClientService)

    example_data = [
        {
            "id": 0,
            "vm_config_type": {
                "id": 0,
                "name": "azure-v100",
                "code": "azure-v100",
                "vm_instance_type": {
                    "id": 0,
                    "name": "azure-v100",
                    "code": "azure-v100",
                    "vendor": "azure",
                    "region": "eastus",
                    "device_type": "V100"
                }
            }
        },
        {
            "id": 1,
            "vm_config_type": {
                "id": 1,
                "name": "aws-a100",
                "code": "aws-a100",
                "vm_instance_type": {
                    "id": 1,
                    "name": "aws-a100",
                    "code": "aws-a100",
                    "vendor": "aws",
                    "region": "us-east-1",
                    "device_type": "A100"
                }
            }
        },
    ]

    # Success
    requests_mock.get(group_vm_client.url_template.render(group_id=0), json=example_data)
    assert group_vm_client.get_id_by_name('azure-v100') == 0
    assert group_vm_client.get_id_by_name('aws-a100') == 1
    assert group_vm_client.get_id_by_name('gcp-k80') is None

    # Failed due to HTTP error
    requests_mock.get(group_vm_client.url_template.render(group_id=0), status_code=404)
    with pytest.raises(typer.Exit):
        group_vm_client.get_id_by_name('azure-a100')


@pytest.mark.usefixtures('patch_auto_token_refresh', 'patch_init_group')
def test_group_vm_quota_client_list_vm_quotas(requests_mock: requests_mock.Mocker,
                                              group_vm_quota_client: GroupVMQuotaClientService):
    assert isinstance(group_vm_quota_client, GroupVMQuotaClientService)

    example_data = [
        {
            "vm_instance_type": {
                "id": 0,
                "name": "azure-v100",
                "code": "azure-v100",
                "vendor": "azure",
                "region": "eastus",
                "device_type": "V100"
            },
            "quota": 4
        },
        {
            "vm_instance_type": {
                "id": 0,
                "name": "azure-a100",
                "code": "azure-a100",
                "vendor": "azure",
                "region": "westus2",
                "device_type": "A100"
            },
            "quota": 8
        },
        {
            "vm_instance_type": {
                "id": 1,
                "name": "aws-a100",
                "code": "aws-a100",
                "vendor": "aws",
                "region": "us-east-1",
                "device_type": "A100"
            },
            "quota": 16
        }
    ]

    # Success
    requests_mock.get(group_vm_quota_client.url_template.render(group_id=0), json=example_data)

    # List VMs without filters
    assert group_vm_quota_client.list_vm_quotas() == example_data

    # List VMs filtered by vendor
    assert group_vm_quota_client.list_vm_quotas(vendor=CloudType.AWS) == [
        {
            "vm_instance_type": {
                "id": 1,
                "name": "aws-a100",
                "code": "aws-a100",
                "vendor": "aws",
                "region": "us-east-1",
                "device_type": "A100"
            },
            "quota": 16
        }
    ]
    assert group_vm_quota_client.list_vm_quotas(vendor=CloudType.AZURE) == [
        {
            "vm_instance_type": {
                "id": 0,
                "name": "azure-v100",
                "code": "azure-v100",
                "vendor": "azure",
                "region": "eastus",
                "device_type": "V100"
            },
            "quota": 4
        },
        {
            "vm_instance_type": {
                "id": 0,
                "name": "azure-a100",
                "code": "azure-a100",
                "vendor": "azure",
                "region": "westus2",
                "device_type": "A100"
            },
            "quota": 8
        }
    ]

    # List VMs filtered by region
    assert group_vm_quota_client.list_vm_quotas(region='us-east-1') == [
        {
            "vm_instance_type": {
                "id": 1,
                "name": "aws-a100",
                "code": "aws-a100",
                "vendor": "aws",
                "region": "us-east-1",
                "device_type": "A100"
            },
            "quota": 16
        }
    ]

    # List VMs filtered by device type
    assert group_vm_quota_client.list_vm_quotas(device_type='A100') == [
        {
            "vm_instance_type": {
                "id": 0,
                "name": "azure-a100",
                "code": "azure-a100",
                "vendor": "azure",
                "region": "westus2",
                "device_type": "A100"
            },
            "quota": 8
        },
        {
            "vm_instance_type": {
                "id": 1,
                "name": "aws-a100",
                "code": "aws-a100",
                "vendor": "aws",
                "region": "us-east-1",
                "device_type": "A100"
            },
            "quota": 16
        }
    ]

    # List VMs filtered by vendor, region and device type
    assert group_vm_quota_client.list_vm_quotas(vendor='azure', region='westus2', device_type='A100') == [
        {
            "vm_instance_type": {
                "id": 0,
                "name": "azure-a100",
                "code": "azure-a100",
                "vendor": "azure",
                "region": "westus2",
                "device_type": "A100"
            },
            "quota": 8
        }
    ]
