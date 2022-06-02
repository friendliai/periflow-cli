# Copyright (C) 2022 FriendliAI

"""PeriFlow JobClient Service"""


import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Iterator, List, Optional

import websockets
from rich.filesize import decimal
from websockets.client import WebSocketClientProtocol
from websockets.exceptions import ConnectionClosed

from pfcli.service import LogType
from pfcli.service.auth import TokenType, get_token
from pfcli.service.client.base import ClientService, safe_request
from pfcli.utils import get_path_size, secho_error_and_exit, zip_dir


class JobClientService(ClientService):
    def list_jobs(self) -> dict:
        response = safe_request(self.list, prefix="Failed to list jobs.")()
        return response.json()['results']

    def get_job(self, job_id: int) -> dict:
        response = safe_request(self.retrieve, prefix="Failed to list jobs.")(
            pk=job_id
        )
        return response.json()

    def run_job(self, config: dict, workspace_dir: Optional[Path]) -> dict:
        job_request = safe_request(self.post, prefix="Failed to run job.")
        if workspace_dir is not None:
            workspace_size = get_path_size(workspace_dir)
            if workspace_size <= 0 or workspace_size > 100 * 1024 * 1024:
                secho_error_and_exit(
                    f"Workspace directory size ({decimal(workspace_size)}) should be 0 < size <= 100MB."
                )
            workspace_zip = Path(workspace_dir.parent / (workspace_dir.name + ".zip"))
            with zip_dir(workspace_dir, workspace_zip) as zip_file:
                files = {'workspace_zip': ('workspace.zip', zip_file)}
                response = job_request(
                    data={"data": json.dumps(config)}, files=files
                )
        else:
            response = job_request(json=config)
        return response.json()

    def cancel_job(self, job_id: int) -> None:
        safe_request(self.post, prefix=f"Failed to cancel job ({job_id}).")(
            path=f"{job_id}/cancel/"
        )

    def terminate_job(self, job_id: int) -> None:
        safe_request(self.post, prefix=f"Failed to terminate job ({job_id}).")(
            path=f"{job_id}/terminate/"
        )

    def get_text_logs(self,
                      job_id: int,
                      num_records: int,
                      head: bool = False,
                      log_types: Optional[List[str]] = None,
                      machines: Optional[List[int]] = None,
                      content: Optional[str] = None) -> List[dict]:
        request_data = {'limit': num_records}
        if head:
            request_data['ascending'] = 'true'
        else:
            request_data['ascending'] = 'false'
        if content is not None:
            request_data['content'] = content
        if log_types is not None:
            request_data['log_types'] = ",".join(log_types)
        if machines is not None:
            request_data['node_ranks'] = ",".join([str(machine) for machine in machines])

        response = safe_request(self.list, prefix="Failed to fetch text logs.")(
            path=f"{job_id}/text_log/"
        )
        logs = response.json()['results']
        if not head:
            logs.reverse()

        return logs


class JobWebSocketClientService(ClientService):
    @asynccontextmanager
    async def _connect(self, job_id: int) -> Iterator[WebSocketClientProtocol]:
        access_token = get_token(TokenType.ACCESS)
        base_url = self.url_template.render(job_id)
        url = f'{base_url}?token={access_token}'
        async with websockets.connect(url) as websocket:
            yield websocket

    async def _subscribe(self,
                         websocket: WebSocketClientProtocol,
                         sources: List[str],
                         node_ranks: List[int]):
        subscribe_json = {
            "type": "subscribe",
            "sources": sources,
            "node_ranks": node_ranks
        }
        await websocket.send(json.dumps(subscribe_json))
        response = await websocket.recv()
        try:
            decoded_response = json.loads(response)
            assert decoded_response.get("response_type") == "subscribe" and \
                set(sources) == set(decoded_response["sources"])
        except json.JSONDecodeError:
            secho_error_and_exit("Error occurred while decoding websocket response...")
        except AssertionError:
            secho_error_and_exit(f"Invalid websocket response... {response}")

    @asynccontextmanager
    async def open_connection(self,
                              job_id: int,
                              log_types: Optional[List[str]],
                              machines: Optional[List[int]]):
        if log_types is None:
            sources = [f"process.{x.value}" for x in LogType]
        else:
            sources = [f"process.{log_type}" for log_type in log_types]

        if machines is None:
            node_ranks = []
        else:
            node_ranks = machines

        async with self._connect(job_id) as websocket:
            websocket: WebSocketClientProtocol
            await self._subscribe(websocket, sources, node_ranks)
            self._websocket = websocket
            yield

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            response = await self._websocket.recv()
        except ConnectionClosed as exc:
            raise StopAsyncIteration from exc   # pragma: no cover

        try:
            return json.loads(response)
        except json.JSONDecodeError:
            secho_error_and_exit("Error occurred while decoding websocket response...")


class JobCheckpointClientService(ClientService):
    def list_checkpoints(self) -> dict:
        response = safe_request(self.list, prefix="Failed to list checkpoints.")()
        return response.json()


class JobArtifactClientService(ClientService):
    def list_artifacts(self) -> dict:
        response = safe_request(self.list, prefix="Failed to list artifacts.")()
        return response.json()


class JobTemplateClientService(ClientService):
    def list_job_template_names(self) -> List[str]:
        response = safe_request(self.list, prefix="Failed to list job template names.")()
        return [ template['name'] for template in response.json() ]

    def get_job_template_by_name(self, name: str) -> Optional[dict]:
        response = safe_request(self.list, prefix="Failed to list job template names.")()
        for template in response.json():
            if template['name'] == name:
                return template
        return None
