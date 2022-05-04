# Copyright (C) 2021 FriendliAI

"""Test Client Service"""

import json
from copy import deepcopy
from string import Template
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest
import typer
import requests_mock
from websockets.client import WebSocketClientProtocol

from pfcli.service import (
    CheckpointCategory,
    CloudType,
    CredType,
    LogType,
    ModelFormCategory,
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
    GroupCheckpointClientService,
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
    ServeClientService,
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
def group_checkpoint_client() -> GroupCheckpointClientService:
    return build_client(ServiceType.GROUP_CHECKPOINT)


@pytest.fixture
def job_ws_client() -> JobWebSocketClientService:
    return build_client(ServiceType.JOB_WS)

@pytest.fixture
def serve_client() -> ServeClientService:
    return build_client(ServiceType.SERVE)


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

    data = {
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

    # Success
    requests_mock.get(
        url_template.render(job_id=1),
        json=data
    )
    assert job_client.get_text_logs(1, 2) == list(reversed(data['results']))

    # Success w options
    assert job_client.get_text_logs(
        1,
        2,
        head=True,
        log_types=['stdout'],
        machines=[0],
        content='2022'
    ) == data['results']

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
        async with job_ws_client.open_connection(job_id=1, log_types=['stdout', 'stderr', 'vmlog'], machines=[0]):
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
                    'node_ranks': [0]
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
        assert data_client.get_upload_urls(0, Path(dir), True) == [
            {'path': '/path/to/local/file', 'upload_url': 'https://s3.bucket.com'}
        ]

    # Failed when uploading empty directory.
    with TemporaryDirectory() as dir:
        with pytest.raises(typer.Exit):
            data_client.get_upload_urls(0, Path(dir), True)

    # Failed due to HTTP error
    requests_mock.post(url_template.render(datastore_id=0), status_code=500)
    with TemporaryDirectory() as dir:
        (Path(dir) / 'file').touch()
        with pytest.raises(typer.Exit):
            data_client.get_upload_urls(0, Path(dir), True)


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

    # Failed due to HTTP error
    requests_mock.get(group_vm_quota_client.url_template.render(group_id=0), status_code=400)
    with pytest.raises(typer.Exit):
        group_vm_quota_client.list_vm_quotas()


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_credential_client_list_credentials(requests_mock: requests_mock.Mocker,
                                            credential_client: CredentialClientService):
    assert isinstance(credential_client, CredentialClientService)

    # Success
    requests_mock.get(
        credential_client.url_template.render(),
        json=[
            {
                'id': 0,
                'name': 'my-docker-secret',
                'type': 'docker'
            }
        ]
    )
    assert credential_client.list_credentials(CredType.DOCKER) == [
        {
            'id': 0,
            'name': 'my-docker-secret',
            'type': 'docker'
        }
    ]

    # Failed due to HTTP error
    requests_mock.get(credential_client.url_template.render(credential_id=0), status_code=404)
    with pytest.raises(typer.Exit):
        credential_client.list_credentials(CredType.BLOB)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_credential_client_get_credential(requests_mock: requests_mock.Mocker,
                                          credential_client: CredentialClientService):
    assert isinstance(credential_client, CredentialClientService)

    # Success
    url_template = deepcopy(credential_client.url_template)
    url_template.attach_pattern('$credential_id/')
    requests_mock.get(
        url_template.render(credential_id=0),
        json={
            'id': 0,
            'name': 'my-docker-secret',
            'type': 'docker'
        }
    )
    assert credential_client.get_credential(0) == {
        'id': 0,
        'name': 'my-docker-secret',
        'type': 'docker'
    }

    # Failed due to HTTP error
    requests_mock.get(url_template.render(credential_id=0), status_code=404)
    with pytest.raises(typer.Exit):
        credential_client.get_credential(0)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_credential_client_create_credential(requests_mock: requests_mock.Mocker,
                                             credential_client: CredentialClientService):
    assert isinstance(credential_client, CredentialClientService)

    # Success
    requests_mock.post(
        credential_client.url_template.render(),
        json={
            'id': 0,
            'name': 'my-slack-token',
            'type': 'slack'
        }
    )
    assert credential_client.create_credential(CredType.SLACK, 'my-slack-token', 1, {'k': 'v'}) == {
        'id': 0,
        'name': 'my-slack-token',
        'type': 'slack'
    }

    # Failed due to HTTP error
    requests_mock.post(credential_client.url_template.render(), status_code=404)
    with pytest.raises(typer.Exit):
        credential_client.create_credential(CredType.S3, 'aws-secret', 1, {'k': 'v'})


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_credential_client_update_credential(requests_mock: requests_mock.Mocker,
                                          credential_client: CredentialClientService):
    assert isinstance(credential_client, CredentialClientService)

    # Success
    url_template = deepcopy(credential_client.url_template)
    url_template.attach_pattern('$credential_id/')
    requests_mock.patch(
        url_template.render(credential_id=0),
        json={
            'id': 0,
            'name': 'my-docker-secret',
            'type': 'docker'
        }
    )
    assert credential_client.update_credential(
        0,
        name='my-docker-secret',
        type_version=1,
        value={'k': 'v'}
    ) == {
        'id': 0,
        'name': 'my-docker-secret',
        'type': 'docker'
    }
    assert credential_client.update_credential(0) == {  # no updated field
        'id': 0,
        'name': 'my-docker-secret',
        'type': 'docker'
    }

    # Failed due to HTTP error
    requests_mock.patch(url_template.render(credential_id=0), status_code=404)
    with pytest.raises(typer.Exit):
        credential_client.update_credential(
            0,
            name='my-gcs-secret',
            type_version=1,
            value={'k': 'v'}
        )


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_credential_client_delete_credential(requests_mock: requests_mock.Mocker,
                                             credential_client: CredentialClientService):
    assert isinstance(credential_client, CredentialClientService)

    # Success
    url_template = deepcopy(credential_client.url_template)
    url_template.attach_pattern('$credential_id/')
    requests_mock.delete(url_template.render(credential_id=0), status_code=204)
    try:
        credential_client.delete_credential(0)
    except typer.Exit:
        raise pytest.fail("Credential delete test failed.")

    # Failed due to HTTP error
    requests_mock.delete(url_template.render(credential_id=0), status_code=404)
    with pytest.raises(typer.Exit):
        credential_client.delete_credential(0)


@pytest.mark.usefixtures('patch_auto_token_refresh', 'patch_init_group')
def test_group_credential_client_service(requests_mock: requests_mock.Mocker,
                                         group_credential_client: GroupCredentialClientService):
    assert isinstance(group_credential_client, GroupCredentialClientService)

    # Sucess
    requests_mock.get(
        group_credential_client.url_template.render(group_id=0),
        json=[
            {
                'id': 0,
                'name': 'our-docker-secret',
                'type': 'docker'
            }
        ]
    )
    assert group_credential_client.list_credentials(CredType.DOCKER) == [
        {
            'id': 0,
            'name': 'our-docker-secret',
            'type': 'docker'
        }
    ]

    # Failed due to HTTP error
    requests_mock.get(group_credential_client.url_template.render(group_id=0), status_code=400)
    with pytest.raises(typer.Exit):
        group_credential_client.list_credentials(CredType.SLACK)


@pytest.mark.usefixtures('patch_auto_token_refresh', 'patch_init_group')
def test_group_credential_client_service(requests_mock: requests_mock.Mocker,
                                         group_credential_client: GroupCredentialClientService):
    assert isinstance(group_credential_client, GroupCredentialClientService)

    # Sucess
    requests_mock.post(
        group_credential_client.url_template.render(group_id=0),
        json={
            'id': 0,
            'name': 'our-docker-secret',
            'type': 'docker'
        }
    )
    assert group_credential_client.create_credential(
        cred_type=CredType.DOCKER,
        name='our-docker-secret',
        type_version=1,
        value={'k': 'v'}
    ) == {
        'id': 0,
        'name': 'our-docker-secret',
        'type': 'docker'
    }

    # Failed due to HTTP error
    requests_mock.post(group_credential_client.url_template.render(group_id=0), status_code=400)
    with pytest.raises(typer.Exit):
        group_credential_client.create_credential(
            cred_type=CredType.DOCKER,
            name='our-docker-secret',
            type_version=1,
            value={'k': 'v'}
        )


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_credential_type_client_get_schema_by_type(requests_mock: requests_mock.Mocker,
                                                   credential_type_client: CredentialTypeClientService):
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
                            "username": {
                                "type": "string"
                            },
                            "password": {
                                "type": "string"
                            }
                        },
                        "required": [
                            "username",
                            "password"
                        ]
                    }
                }
            ]
        },
        {
            "type_name": "aws",
            "versions": [
                {
                    "type_version": 1,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "aws_access_key_id": {
                                "type": "string",
                                "minLength": 1
                            },
                            "aws_secret_access_key": {
                                "type": "string",
                                "minLength": 1
                            },
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
                                    "sa-east-1"
                                ]
                            }
                        },
                        "required": [
                            "aws_access_key_id",
                            "aws_secret_access_key",
                            "aws_default_region"
                        ]
                    }
                }
            ]
        },
        {
            "type_name": "gcp",
            "versions": [
                {
                    "type_version": 1,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "default": "service_account"
                            },
                            "project_id": {
                                "type": "string",
                                "minLength": 1
                            },
                            "private_key_id": {
                                "type": "string",
                                "minLength": 1
                            },
                            "private_key": {
                                "type": "string",
                                "minLength": 1
                            },
                            "client_email": {
                                "type": "string",
                                "minLength": 1
                            },
                            "client_id": {
                                "type": "string",
                                "minLength": 1
                            },
                            "auth_uri": {
                                "type": "string",
                                "minLength": 1
                            },
                            "token_uri": {
                                "type": "string",
                                "minLength": 1
                            },
                            "auth_provider_x509_cert_url": {
                                "type": "string",
                                "minLength": 1
                            },
                            "client_x509_cert_url": {
                                "type": "string",
                                "minLength": 1
                            }
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
                            "client_x509_cert_url"
                        ]
                    }
                }
            ]
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
                                "maxLength": 24
                            },
                            "storage_account_key": {
                                "type": "string",
                                "minLength": 1
                            }
                        },
                        "required": [
                            "storage_account_name",
                            "storage_account_key"
                        ]
                    }
                }
            ]
        },
        {
            "type_name": "slack",
            "versions": [
                {
                    "type_version": 1,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "token": {
                                "type": "string"
                            }
                        },
                        "required": [
                            "token"
                        ]
                    }
                }
            ]
        }
    ]

    # Success
    requests_mock.get(credential_type_client.url_template.render(), json=data)
    assert credential_type_client.get_schema_by_type(CredType.DOCKER) == data[0]['versions'][-1]['schema']
    assert credential_type_client.get_schema_by_type(CredType.S3) == data[1]['versions'][-1]['schema']
    assert credential_type_client.get_schema_by_type(CredType.GCS) == data[2]['versions'][-1]['schema']
    assert credential_type_client.get_schema_by_type(CredType.BLOB) == data[3]['versions'][-1]['schema']
    assert credential_type_client.get_schema_by_type(CredType.SLACK) == data[4]['versions'][-1]['schema']
    assert credential_type_client.get_schema_by_type(CredType.WANDB) is None

    # Failed due to HTTP error
    requests_mock.get(credential_type_client.url_template.render(), status_code=404)
    with pytest.raises(typer.Exit):
        credential_type_client.get_schema_by_type(CredType.DOCKER)


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


@pytest.mark.usefixtures('patch_auto_token_refresh', 'patch_init_group')
def test_group_checkpoint_list_checkpoints(requests_mock: requests_mock.Mocker,
                                           group_checkpoint_client: GroupCheckpointClientService):
    assert isinstance(group_checkpoint_client, GroupCheckpointClientService)

    def build_response_item(category: str, vendor: str, region: str) -> dict:
        return {
            "id": "22222222-2222-2222-2222-222222222222",
            "user_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
            "ownerships": [
                {
                "organization_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                "project_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6"
                }
            ],
            "model_category": category,
            "job_id": 2147483647,
            "name": "string",
            "attributes": {
                "job_setting_json": {},
                "data_json": {},
            },
            "forms": [
                {
                    "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                    "form_category": "MEGATRON",
                    "credential_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                    "vendor": vendor,
                    "region": region,
                    "storage_name": "STORAGE_NAME",
                    "dist_json": {
                    },
                    "files": [
                        {
                            "name": "NAME",
                            "path": "PATH",
                            "mtime": "2022-04-19T09:03:47.352Z",
                            "size": 9
                        }
                    ]
                }
            ],
            "iteration": 922,
            "created_at": "2022-04-19T09:03:47.352Z",
        }

    data = {
        "results": [
            build_response_item("USER", "aws", "us-east-1"),
            build_response_item("JOB", "aws", "us-east-2"),
        ],
        "next_cursor": "NEXT_CURSOR",
    }

    # Success
    url = group_checkpoint_client.url_template.render(group_id="00000000-0000-0000-0000-000000000000", project_id="11111111-1111-1111-1111-111111111111")
    requests_mock.get(url, json=data)
    assert group_checkpoint_client.list_checkpoints(CheckpointCategory.USER_PROVIDED) == data['results']
    assert requests_mock.request_history[-1].query == 'category=USER'
    assert group_checkpoint_client.list_checkpoints(CheckpointCategory.JOB_GENERATED) == data['results']
    assert requests_mock.request_history[-1].query == 'category=JOB'

    # Failed due to HTTP error
    requests_mock.get(url, status_code=400)
    with pytest.raises(typer.Exit):
        group_checkpoint_client.list_checkpoints(CheckpointCategory.USER_PROVIDED)


@pytest.mark.usefixtures('patch_auto_token_refresh', 'patch_init_group')
def test_group_checkpoint_create_checkpoints(requests_mock: requests_mock.Mocker,
                                           group_checkpoint_client: GroupCheckpointClientService):
    assert isinstance(group_checkpoint_client, GroupCheckpointClientService)

    data = {
        "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "category": "user_provided",
        "vendor": "aws",
        "region": "us-east-1",
        "credential_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "storage_name": "my-ckpt",
        "iteration": 1000,
        "files": [
            {
                "name": "new_ckpt_1000.pth",
                "path": "ckpt/new_ckpt_1000.pth",
                "mtime": "2022-04-20T06:27:37.907Z",
                "size": 2048,
            }
        ]
    }

    # Success
    # TODO: change after PFA integration
    url = group_checkpoint_client.url_template.render(
        group_id="00000000-0000-0000-0000-000000000000",
        project_id="11111111-1111-1111-1111-111111111111"
    )
    requests_mock.post(url, json=data)
    assert group_checkpoint_client.create_checkpoint(
        name="my-ckpt",
        model_form_category=ModelFormCategory.MEGATRON,
        vendor=StorageType.S3,
        region='us-east-1',
        credential_id="3fa85f64-5717-4562-b3fc-2c963f66afa6",
        iteration=1000,
        storage_name="my-ckpt",
        files=[
            {
                "name": "new_ckpt_1000.pth",
                "path": "ckpt/new_ckpt_1000.pth",
                "mtime": "2022-04-20T06:27:37.907Z",
                "size": 2048,
            }
        ],
        dist_config={"k": "v"},
        data_config={"k": "v"},
        job_setting_config={"k": "v"}
    ) == data
    assert requests_mock.request_history[-1].json() == {
        "job_id": None,
        "vendor": "s3",
        "region": "us-east-1",
        "storage_name": "my-ckpt",
        "model_category": "USER",
        "form_category": "MEGATRON",
        "name": "my-ckpt",
        "dist_json": {"k": "v"},
        "attributes": {
            "job_setting_json": {"k": "v"},
            "data_json": {"k": "v"},
        },
        "credential_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "iteration": 1000,
        "user_id": "22222222-2222-2222-2222-222222222222",  # TODO: change after PFA integration
        "files": [
            {
                "name": "new_ckpt_1000.pth",
                "path": "ckpt/new_ckpt_1000.pth",
                "mtime": "2022-04-20T06:27:37.907Z",
                "size": 2048,
            }
        ]
    }


    # Failed due to invalid region
    with pytest.raises(typer.Exit):
        group_checkpoint_client.create_checkpoint(
            name="my-ckpt",
            model_form_category=ModelFormCategory.MEGATRON,
            vendor=StorageType.S3,
            region='busan',
            credential_id="3fa85f64-5717-4562-b3fc-2c963f66afa6",
            iteration=1000,
            storage_name="my-ckpt",
            files=[
                {
                    "name": "new_ckpt_1000.pth",
                    "path": "ckpt/new_ckpt_1000.pth",
                    "mtime": "2022-04-20T06:27:37.907Z",
                    "size": 2048,
                }
            ],
            dist_config={"k": "v"},
            data_config={"k": "v"},
            job_setting_config={"k": "v"}
        )

    # Failed due to HTTP error
    requests_mock.post(url, status_code=400)
    with pytest.raises(typer.Exit):
        group_checkpoint_client.create_checkpoint(
            name="my-ckpt",
            model_form_category=ModelFormCategory.MEGATRON,
            vendor=StorageType.S3,
            region='us-east-1',
            credential_id="3fa85f64-5717-4562-b3fc-2c963f66afa6",
            iteration=1000,
            storage_name="my-ckpt",
            files=[
                {
                    "name": "new_ckpt_1000.pth",
                    "path": "ckpt/new_ckpt_1000.pth",
                    "mtime": "2022-04-20T06:27:37.907Z",
                    "size": 2048,
                }
            ],
            dist_config={"k": "v"},
            data_config={"k": "v"},
            job_setting_config={"k": "v"}
        )

@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_serve_client_get_serve(requests_mock: requests_mock.Mocker,
                                serve_client: ServeClientService):
    assert isinstance(serve_client, ServeClientService)

    url_template = deepcopy(serve_client.url_template)
    url_template.attach_pattern('$serve_id/')

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
            "start": '2022-04-18T05:55:14.365021Z',
            "endpoint": "http:0.0.0.0:8000/0/v1/completions"
        }
    )
    assert serve_client.get_serve(0) == {
        "id": 0,
        "name": "name",
        "status": "running",
        "vm": "aws-g4dn.12xlarge",
        "gpu_type": "t4",
        "num_gpus": 1,
        "start": '2022-04-18T05:55:14.365021Z',
        "endpoint": "http:0.0.0.0:8000/0/v1/completions"
    }

    requests_mock.get(url_template.render(serve_id=0), status_code=404)
    with pytest.raises(typer.Exit):
        serve_client.get_serve(0)

@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_serve_client_create_serve(requests_mock: requests_mock.Mocker,
                                   serve_client: ServeClientService):
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
            "start": '2022-04-18T05:55:14.365021Z',
            "endpoint": "http:0.0.0.0:8000/0/v1/completions"
        }
    )

    assert serve_client.create_serve({}) == {
        "id": 0,
        "name": "name",
        "status": "running",
        "vm": "aws-g4dn.12xlarge",
        "gpu_type": "t4",
        "num_gpus": 1,
        "start": '2022-04-18T05:55:14.365021Z',
        "endpoint": "http:0.0.0.0:8000/0/v1/completions"
    }

    requests_mock.post(serve_client.url_template.render(), status_code=404)
    with pytest.raises(typer.Exit):
        serve_client.create_serve({})

@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_serve_client_list_serve(requests_mock: requests_mock.Mocker,
                                 serve_client: ServeClientService):
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
                "start": '2022-04-18T05:55:14.365021Z',
                "endpoint": "http:0.0.0.0:8000/0/v1/completions"
            }
        ]
    )

    assert serve_client.list_serves() == [
        {
            "id": 0,
            "name": "name",
            "status": "running",
            "vm": "aws-g4dn.12xlarge",
            "gpu_type": "t4",
            "num_gpus": 1,
            "start": '2022-04-18T05:55:14.365021Z',
            "endpoint": "http:0.0.0.0:8000/0/v1/completions"
        }
    ]

    requests_mock.get(serve_client.url_template.render(), status_code=404)
    with pytest.raises(typer.Exit):
        serve_client.list_serves()

@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_serve_client_delete_serve(requests_mock: requests_mock.Mocker,
                                 serve_client: ServeClientService):
    assert isinstance(serve_client, ServeClientService)

    url_template = deepcopy(serve_client.url_template)
    url_template.attach_pattern('$serve_id/')

    # Success
    requests_mock.delete(url_template.render(serve_id=0), status_code=204)
    try:
        serve_client.delete_serve(0)
    except typer.Exit:
        raise pytest.fail("serve client test failed.")
    
    requests_mock.delete(url_template.render(serve_id=0), status_code=404)
    with pytest.raises(typer.Exit):
        serve_client.delete_serve(0)
