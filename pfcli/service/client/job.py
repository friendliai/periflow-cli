# Copyright (C) 2022 FriendliAI

"""PeriFlow JobClient Service"""

import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from string import Template
from typing import Any, Dict, Iterator, List, Optional, Tuple
from uuid import UUID

import requests
import typer

import websockets
from requests.models import Response
from rich.filesize import decimal
from websockets.client import WebSocketClientProtocol
from websockets.exceptions import ConnectionClosed

from pfcli.service import JobStatus, LogType
from pfcli.service.auth import TokenType, auto_token_refresh, get_auth_header, get_token
from pfcli.service.client.base import ClientService, ProjectRequestMixin, safe_request
from pfcli.service.formatter import TreeFormatter
from pfcli.utils.format import secho_error_and_exit
from pfcli.utils.fs import get_workspace_files, zip_dir
from pfcli.utils.request import paginated_get


class JobWebSocketClientService(ClientService):
    @asynccontextmanager
    async def _connect(self, job_id: UUID) -> Iterator[WebSocketClientProtocol]:
        access_token = get_token(TokenType.ACCESS)
        base_url = self.url_template.render(**self.url_kwargs, pk=job_id)
        url = f"{base_url}?token={access_token}"
        async with websockets.connect(url) as websocket:
            yield websocket

    async def _subscribe(
        self,
        websocket: WebSocketClientProtocol,
        sources: List[str],
        node_ranks: List[int],
    ):
        subscribe_json = {
            "type": "subscribe",
            "sources": sources,
            "node_ranks": node_ranks,
        }
        await websocket.send(json.dumps(subscribe_json))
        response = await websocket.recv()
        try:
            decoded_response = json.loads(response)
            assert decoded_response.get("response_type") == "subscribe" and set(
                sources
            ) == set(decoded_response["sources"])
        except json.JSONDecodeError:
            secho_error_and_exit("Error occurred while decoding websocket response...")
        except AssertionError:
            secho_error_and_exit(f"Invalid websocket response... {response}")

    @asynccontextmanager
    async def open_connection(
        self,
        job_id: UUID,
        log_types: Optional[List[str]],
        machines: Optional[List[int]],
    ):
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
            raise StopAsyncIteration from exc  # pragma: no cover

        try:
            return json.loads(response)
        except json.JSONDecodeError:
            secho_error_and_exit("Error occurred while decoding websocket response...")


class ProjectJobCheckpointClientService(ClientService, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_checkpoints(self) -> List[dict]:
        response = safe_request(self.list, err_prefix="Failed to list checkpoints.")()
        return response.json()


class ProjectJobArtifactClientService(ClientService, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_artifacts(self) -> List[dict]:
        response = safe_request(self.list, err_prefix="Failed to list artifacts.")()
        return response.json()

    @auto_token_refresh
    def download(self, artifact_id: int) -> Response:
        url_template = self.url_template.copy()
        url_template.attach_pattern(f"{artifact_id}/download/")
        return requests.get(
            url_template.render(**self.url_kwargs), headers=get_auth_header()
        )

    def get_artifact_download_url(self, artifact_id: int) -> dict:
        response = safe_request(
            self.download, err_prefix="Failed to get artifact download url"
        )(artifact_id)
        return response.json()


class JobTemplateClientService(ClientService[UUID]):
    def list_job_template_names(self) -> List[str]:
        response = safe_request(
            self.list, err_prefix="Failed to list job template names."
        )()
        return [template["name"] for template in response.json()]

    def get_job_template_by_name(self, name: str) -> Optional[dict]:
        response = safe_request(
            self.list, err_prefix="Failed to list job template names."
        )()
        for template in response.json():
            if template["name"] == name:
                return template
        return None

    def get_job_template(self, job_template_id: UUID) -> dict:
        response = safe_request(
            self.retrieve,
            err_prefix=f"Job template (ID: {job_template_id}) is not found.",
        )(pk=job_template_id)
        return response.json()


class ProjectJobClientService(ClientService, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_jobs(
        self,
        since: Optional[str] = None,
        until: Optional[str] = None,
        job_name: Optional[str] = None,
        vm: Optional[str] = None,
        statuses: Optional[Tuple[JobStatus]] = None,
        user_ids: Optional[List[UUID]] = None,
    ) -> List[dict]:
        params = {
            "created_at.since": since,
            "created_at.until": until,
            "job_name": job_name,
            "vm_code": vm,
            "status": ",".join(statuses) if statuses is not None else None,
            "user_ids": ",".join(str(user_id) for user_id in user_ids)
            if user_ids is not None
            else None,
        }
        return paginated_get(
            safe_request(self.list, err_prefix="Failed to list jobs in project."),
            **params,
        )

    def run_job(self, config: Dict, workspace_dir: Optional[Path]) -> dict:
        job_request = safe_request(self.post, err_prefix="Failed to run job.")
        if workspace_dir is not None:
            typer.secho("Preparing workspace directory...", fg=typer.colors.MAGENTA)
            workspace_dir = workspace_dir.resolve()
            workspace_files = get_workspace_files(workspace_dir)
            workspace_size = sum(f.stat().st_size for f in workspace_files)
            if workspace_size <= 0 or workspace_size > 100 * 1024 * 1024:
                secho_error_and_exit(
                    f"Workspace directory size ({decimal(workspace_size)}) should be 0 < size <= 100MB."
                )
            tree_formatter = TreeFormatter(
                name="Job Workspace",
                root=os.path.join(
                    config["job_setting"]["workspace"]["mount_path"], workspace_dir.name
                ),
            )
            typer.secho(
                "Workspace is prepared and will be mounted as the following structure.",
                fg=typer.colors.MAGENTA,
            )
            tree_formatter.render(
                [
                    {"path": f.relative_to(workspace_dir), "size": f.stat().st_size}
                    for f in workspace_files
                ]
            )
            workspace_zip = Path(workspace_dir.parent / (workspace_dir.name + ".zip"))
            with zip_dir(workspace_dir, workspace_files, workspace_zip) as zip_file:
                files = {"workspace_zip": ("workspace.zip", zip_file)}
                response = job_request(data={"data": json.dumps(config)}, files=files)
        else:
            response = job_request(json=config)
        return response.json()

    def delete_job(self, job_number: int) -> None:
        safe_request(self.delete, err_prefix=f"Failed to delete job ({job_number}).")(
            pk=job_number
        )

    def get_job(self, job_number: int) -> dict:
        response = safe_request(
            self.retrieve, err_prefix=f"Failed to get job ({job_number})."
        )(pk=job_number)
        return response.json()

    def cancel_job(self, job_number: int) -> None:
        safe_request(self.post, err_prefix=f"Failed to cancel job ({job_number}).")(
            path=f"{job_number}/cancel/"
        )

    def terminate_job(self, job_number: int) -> None:
        safe_request(self.post, err_prefix=f"Failed to terminate job ({job_number}).")(
            path=f"{job_number}/terminate/"
        )

    def get_text_logs(
        self,
        job_number: int,
        num_records: int,
        head: bool = False,
        log_types: Optional[List[str]] = None,
        machines: Optional[List[int]] = None,
        content: Optional[str] = None,
    ) -> List[dict]:
        request_data: Dict[str, Any] = {"limit": num_records}
        if head:
            request_data["ascending"] = "true"
        else:
            request_data["ascending"] = "false"
        if content is not None:
            request_data["content"] = content
        if log_types is not None:
            request_data["log_types"] = ",".join(log_types)
        if machines is not None:
            request_data["node_ranks"] = ",".join(
                [str(machine) for machine in machines]
            )

        logs = paginated_get(
            safe_request(
                self.list,
                err_prefix=f"Failed to fetch text logs of job ({job_number}).",
            ),
            path=f"{job_number}/text_log/",
            **request_data,
        )
        if not head:
            logs.reverse()

        return logs
