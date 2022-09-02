# Copyright (C) 2022 FriendliAI

"""Test JobClient Service"""


import json
from copy import deepcopy
from unittest.mock import AsyncMock, patch

import pytest
import requests_mock
import typer
from websockets.legacy.client import WebSocketClientProtocol

from pfcli.service import LogType, ServiceType
from pfcli.service.auth import TokenType
from pfcli.service.client import build_client
from pfcli.service.client.job import (
    JobArtifactClientService,
    JobCheckpointClientService,
    JobClientService,
    JobTemplateClientService,
    JobWebSocketClientService,
)


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
def job_template_client() -> JobTemplateClientService:
    return build_client(ServiceType.JOB_TEMPLATE)


@pytest.fixture
def job_ws_client() -> JobWebSocketClientService:
    return build_client(ServiceType.JOB_WS)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_job_client_list_jobs(
    requests_mock: requests_mock.Mocker, job_client: JobClientService
):
    assert isinstance(job_client, JobClientService)

    # Success
    requests_mock.get(
        job_client.url_template.render(),
        json={"results": [{"id": 1}, {"id": 2}], "next_cursor": None},
    )
    assert job_client.list_jobs() == [{"id": 1}, {"id": 2}]

    # Failed due to HTTP error
    requests_mock.get(job_client.url_template.render(), status_code=404)
    with pytest.raises(typer.Exit):
        job_client.list_jobs()


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_job_client_get_job(
    requests_mock: requests_mock.Mocker, job_client: JobClientService
):
    assert isinstance(job_client, JobClientService)

    # Success
    requests_mock.get(job_client.url_template.render(1), json={"id": 1})
    assert job_client.get_job(1) == {"id": 1}

    # Failed due to HTTP error
    requests_mock.get(job_client.url_template.render(1), status_code=404)
    with pytest.raises(typer.Exit):
        job_client.get_job(1)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_job_client_cancel_job(
    requests_mock: requests_mock.Mocker, job_client: JobClientService
):
    url_template = deepcopy(job_client.url_template)
    url_template.attach_pattern("$job_id/cancel/")

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


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_job_client_terminate_job(
    requests_mock: requests_mock.Mocker, job_client: JobClientService
):
    url_template = deepcopy(job_client.url_template)
    url_template.attach_pattern("$job_id/terminate/")

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


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_job_client_get_text_logs(
    requests_mock: requests_mock.Mocker, job_client: JobClientService
):
    url_template = deepcopy(job_client.url_template)
    url_template.attach_pattern("$job_id/text_log/")

    data = {
        "results": [
            {
                "content": "hello\n",
                "timestamp": "2022-04-18T05:55:14.365021Z",
                "type": "stdout",
                "node_rank": 0,
            },
            {
                "content": "world\n",
                "timestamp": "2022-04-18T05:55:14.364470Z",
                "type": "stdout",
                "node_rank": 0,
            },
        ],
        "next_cursor": None,
    }

    # Success
    requests_mock.get(url_template.render(job_id=1), json=data)
    assert job_client.get_text_logs(1, 2) == list(reversed(data["results"]))

    # Success w options
    assert (
        job_client.get_text_logs(
            1, 2, head=True, log_types=["stdout"], machines=[0], content="2022"
        )
        == data["results"]
    )

    # Failed
    requests_mock.get(url_template.render(job_id=1), status_code=500)
    with pytest.raises(typer.Exit):
        job_client.get_text_logs(1, 2)


@pytest.mark.asyncio
@pytest.mark.usefixtures("patch_auto_token_refresh")
async def test_job_ws_client(job_ws_client: JobWebSocketClientService):
    ws_mock = AsyncMock(WebSocketClientProtocol)
    with patch(
        "pfcli.service.client.job.get_token", return_value="ACCESS_TOKEN"
    ) as get_token_mock, patch(
        "websockets.connect",
    ) as ws_connect_mock:
        ws_connect_mock.return_value.__aenter__.return_value = ws_mock
        ws_mock.recv.side_effect = [
            json.dumps(
                {
                    "response_type": "subscribe",
                    "sources": [f"process.{x.value}" for x in LogType],
                }
            ),
            json.dumps(
                {
                    "content": "hello\n",
                    "timestamp": "2022-04-18T05:55:14.365021Z",
                    "type": "stdout",
                    "node_rank": 0,
                }
            ),
            json.dumps(
                {
                    "content": "world\n",
                    "timestamp": "2022-04-18T05:55:14.364470Z",
                    "type": "stdout",
                    "node_rank": 0,
                }
            ),
        ]

        resp_list = []
        async with job_ws_client.open_connection(
            job_id=1, log_types=["stdout", "stderr", "vmlog"], machines=[0]
        ):
            async for resp in job_ws_client:
                resp_list.append(resp)

        get_token_mock.assert_called_once_with(TokenType.ACCESS)
        ws_connect_mock.assert_called_once_with(
            f"{job_ws_client.url_template.render(1)}?token=ACCESS_TOKEN"
        )
        ws_connect_mock.return_value.__aenter__.assert_awaited_once()
        ws_connect_mock.return_value.__aexit__.assert_awaited_once()
        ws_mock.send.assert_awaited_once_with(
            json.dumps(
                {
                    "type": "subscribe",
                    "sources": [f"process.{x.value}" for x in LogType],
                    "node_ranks": [0],
                }
            )
        )
        assert ws_mock.recv.call_count == 4

        assert resp_list[0] == {
            "content": "hello\n",
            "timestamp": "2022-04-18T05:55:14.365021Z",
            "type": "stdout",
            "node_rank": 0,
        }
        assert resp_list[1] == {
            "content": "world\n",
            "timestamp": "2022-04-18T05:55:14.364470Z",
            "type": "stdout",
            "node_rank": 0,
        }


@pytest.mark.asyncio
@pytest.mark.usefixtures("patch_auto_token_refresh")
async def test_job_ws_client_errors(job_ws_client: JobWebSocketClientService):
    ws_mock = AsyncMock(WebSocketClientProtocol)
    with patch(
        "pfcli.service.auth.get_token", return_value="ACCESS_TOKEN"
    ) as get_token_mock, patch(
        "websockets.connect",
    ) as ws_connect_mock:
        ws_connect_mock.return_value.__aenter__.return_value = ws_mock

        # Invalid json in ws reponse
        ws_mock.recv.side_effect = ["not_a_json"]
        with pytest.raises(typer.Exit):
            async with job_ws_client.open_connection(
                job_id=1, log_types=None, machines=None
            ):
                pass

        # Invalid ws response contents (invalid response_type)
        ws_mock.recv.side_effect = [
            json.dumps(
                {
                    "response_type": "not_subscribe",
                    "sources": [f"process.{x.value}" for x in LogType],
                }
            )
        ]
        with pytest.raises(typer.Exit):
            async with job_ws_client.open_connection(
                job_id=1, log_types=None, machines=None
            ):
                pass

        # Invalid ws response contents (sources are not matched)
        ws_mock.recv.side_effect = [
            json.dumps(
                {
                    "response_type": "not_subscribe",
                    "sources": [f"process.{x.value}" for x in LogType],
                }
            )
        ]
        with pytest.raises(typer.Exit):
            async with job_ws_client.open_connection(
                job_id=1, log_types=[LogType.STDOUT], machines=None
            ):
                pass

        # Errors while subscribing contents
        ws_mock.recv.side_effect = [
            json.dumps(
                {
                    "response_type": "subscribe",
                    "sources": [f"process.{x.value}" for x in LogType],
                }
            ),
            "not_a_json",
        ]
        with pytest.raises(typer.Exit):
            async with job_ws_client.open_connection(
                job_id=1, log_types=None, machines=None
            ):
                async for _ in job_ws_client:
                    pass


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_job_checkpoint_client_list_checkpoints(
    requests_mock: requests_mock.Mocker,
    job_checkpoint_client: JobCheckpointClientService,
):
    assert isinstance(job_checkpoint_client, JobCheckpointClientService)

    # Success
    requests_mock.get(
        job_checkpoint_client.url_template.render(job_id=1), json=[{"id": 1}]
    )
    assert job_checkpoint_client.list_checkpoints() == [{"id": 1}]

    # Failed due to HTTP error
    requests_mock.get(
        job_checkpoint_client.url_template.render(job_id=1), status_code=404
    )
    with pytest.raises(typer.Exit):
        job_checkpoint_client.list_checkpoints()


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_job_artifact_client_list_artifacts(
    requests_mock: requests_mock.Mocker, job_artifact_client: JobCheckpointClientService
):
    assert isinstance(job_artifact_client, JobArtifactClientService)

    # Success
    requests_mock.get(
        job_artifact_client.url_template.render(job_id=1), json=[{"id": 1}]
    )
    assert job_artifact_client.list_artifacts() == [{"id": 1}]

    # Failed due to HTTP error
    requests_mock.get(
        job_artifact_client.url_template.render(job_id=1), status_code=404
    )
    with pytest.raises(typer.Exit):
        job_artifact_client.list_artifacts()


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_job_artifact_client_get_download_urls(
    requests_mock: requests_mock.Mocker, job_artifact_client: JobCheckpointClientService
):
    assert isinstance(job_artifact_client, JobArtifactClientService)

    url_template = job_artifact_client.url_template.copy()
    url_template.attach_pattern("1/download/")
    # Success
    requests_mock.get(
        url_template.render(job_id=1), json=[{"url": "https://hello.artifact.com"}]
    )
    assert job_artifact_client.get_artifact_download_url(1) == [
        {"url": "https://hello.artifact.com"}
    ]

    # Failed due to HTTP error
    requests_mock.get(url_template.render(job_id=1), status_code=404)
    with pytest.raises(typer.Exit):
        job_artifact_client.get_artifact_download_url(1)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_job_template_client_list_job_template_names(
    requests_mock: requests_mock.Mocker, job_template_client: JobTemplateClientService
):
    assert isinstance(job_template_client, JobTemplateClientService)

    # Success
    requests_mock.get(
        job_template_client.url_template.render(),
        json=[
            {
                "id": 0,
                "name": "gpt-template",
            },
            {"id": 1, "name": "bert-template"},
        ],
    )
    assert job_template_client.list_job_template_names() == [
        "gpt-template",
        "bert-template",
    ]

    requests_mock.get(job_template_client.url_template.render(), status_code=404)
    with pytest.raises(typer.Exit):
        job_template_client.list_job_template_names()


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_job_template_client_get_job_template_by_name(
    requests_mock: requests_mock.Mocker, job_template_client: JobTemplateClientService
):
    assert isinstance(job_template_client, JobTemplateClientService)

    # Success
    requests_mock.get(
        job_template_client.url_template.render(),
        json=[
            {
                "id": 0,
                "name": "gpt-template",
            },
            {"id": 1, "name": "bert-template"},
        ],
    )
    assert job_template_client.get_job_template_by_name("gpt-template") == {
        "id": 0,
        "name": "gpt-template",
    }
    assert job_template_client.get_job_template_by_name("bert-template") == {
        "id": 1,
        "name": "bert-template",
    }
    assert job_template_client.get_job_template_by_name("dalle-template") is None

    requests_mock.get(job_template_client.url_template.render(), status_code=404)
    with pytest.raises(typer.Exit):
        job_template_client.get_job_template_by_name("some-template")
