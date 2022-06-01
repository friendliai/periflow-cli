# Copyright (C) 2021 FriendliAI

"""PeriFlow Client Service"""

from __future__ import annotations

import copy
import json
from string import Template
from typing import (
    Iterator,
    TypeVar,
    Type,
    List,
    Dict,
    Tuple,
    Optional,
    Union
)
from dataclasses import dataclass
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urljoin, urlparse
import uuid

import requests
import websockets
from websockets.client import WebSocketClientProtocol
from websockets.exceptions import ConnectionClosed
from requests import HTTPError
from requests import Response
from rich.filesize import decimal

from pfcli.context import get_current_group_id, get_current_project_id
from pfcli.service.auth import (
    TokenType,
    get_auth_header,
    get_token,
    auto_token_refresh,
)
from pfcli.utils import (
    decode_http_err,
    get_auth_uri,
    get_path_size,
    get_uri,
    get_wss_uri,
    get_mr_uri,
    get_pfs_uri,
    secho_error_and_exit,
    validate_storage_region,
    zip_dir,
)
from pfcli.service import (
    CheckpointCategory,
    CloudType,
    ModelFormCategory,
    LockType,
    StorageType,
    CredType,
    ServiceType,
    LogType,
    cred_type_map,
    storage_type_map,
    storage_type_map_inv,
)


T = TypeVar('T', bound=Union[int, str, uuid.UUID])


@dataclass
class URLTemplate:
    pattern: Template

    def render(self, pk: Optional[T] = None, path: Optional[str] = None, **kwargs) -> str:
        """render URLTemplate

        Args:
            pk: primary key
            path: additional path to attach
        """
        if pk is None and path is None:
            return self.pattern.substitute(**kwargs)

        pattern = copy.deepcopy(self.pattern)
        need_trailing_slash = pattern.template.endswith('/')

        if pk is not None:
            pattern.template = urljoin(pattern.template + '/', str(pk))
            if need_trailing_slash:
                pattern.template += '/'

        if path is not None:
            pattern.template = urljoin(pattern.template + '/', path.rstrip('/'))
            if need_trailing_slash:
                pattern.template += '/'

        return pattern.substitute(**kwargs)

    def get_base_url(self) -> str:
        result = urlparse(self.pattern.template)
        return f"{result.scheme}://{result.hostname}"

    def attach_pattern(self, pattern: str) -> None:
        self.pattern.template = urljoin(self.pattern.template + '/', pattern)

    def replace_path(self, path: str):
        result = urlparse(self.pattern.template)
        result = result._replace(path=path)
        self.pattern.template = result.geturl()

    def copy(self) -> "URLTemplate":
        return URLTemplate(pattern=Template(self.pattern.template))


class ClientService:
    def __init__(self, template: Template, **kwargs):
        self.url_template = URLTemplate(template)
        self.url_kwargs = kwargs

    @auto_token_refresh
    def list(self, path: Optional[str] = None, **kwargs) -> Response:
        return requests.get(
            self.url_template.render(path=path, **self.url_kwargs),
            headers=get_auth_header(),
            **kwargs 
        )

    @auto_token_refresh
    def retrieve(self, pk: T, path: Optional[str] = None, **kwargs) -> Response:
        return requests.get(
            self.url_template.render(pk=pk, path=path, **self.url_kwargs),
            headers=get_auth_header(),
            **kwargs
        )

    @auto_token_refresh
    def post(self, path: Optional[str] = None, **kwargs) -> Response:
        return requests.post(
            self.url_template.render(path=path, **self.url_kwargs),
            headers=get_auth_header(),
            **kwargs
        )

    @auto_token_refresh
    def partial_update(self, pk: T, path: Optional[str] = None, **kwargs) -> Response:
        return requests.patch(
            self.url_template.render(pk=pk, path=path, **self.url_kwargs),
            headers=get_auth_header(),
            **kwargs
        )

    @auto_token_refresh
    def delete(self, pk: T, path: Optional[str] = None, **kwargs) -> Response:
        return requests.delete(
            self.url_template.render(pk=pk, path=path, **self.url_kwargs),
            headers=get_auth_header(),
            **kwargs
        )

    @auto_token_refresh
    def update(self, pk: T, path: Optional[str] = None, **kwargs) -> Response:
        return requests.put(
            self.url_template.render(pk=pk, path=path, **self.url_kwargs),
            headers=get_auth_header(),
            **kwargs
        )


class UserRequestMixin:
    user_id: uuid.UUID

    @auto_token_refresh
    def _userinfo(self) -> requests.Response:
        return requests.get(get_auth_uri("oauth2/userinfo"), headers=get_auth_header())

    def get_current_userinfo(self) -> dict:
        try:
            response = self._userinfo()
        except requests.HTTPError as exc:
            secho_error_and_exit(f"Failed to get userinfo.\n{decode_http_err(exc)}")
        return response.json()

    def get_current_user_id(self) -> uuid.UUID:
        userinfo = self.get_current_userinfo()
        return uuid.UUID(userinfo['sub'].split('|')[1])

    def initialize_user(self):
        self.user_id = self.get_current_user_id()


class GroupRequestMixin:
    group_id: uuid.UUID

    def initialize_group(self):
        self.group_id = get_current_group_id()


class ProjectRequestMixin:
    project_id: uuid.UUID

    def initialize_project(self):
        self.project_id = get_current_project_id()


class UserClientService(ClientService, UserRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_user()
        super().__init__(template, **kwargs)

    def change_password(self, old_password: str, new_password: str) -> None:
        try:
            self.update(pk=self.user_id,
                        path="password",
                        json={
                            "old_password": old_password,
                            "new_password": new_password
                        })
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to change password.\n{decode_http_err(exc)}")


class UserGroupClientService(ClientService, UserRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_user()
        super().__init__(template, pf_user_id=self.user_id, **kwargs)

    def get_group_info(self) -> list:
        try:
            response = self.list()
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to get my group info.\n{decode_http_err(exc)}")
        return response.json()


class UserGroupProjectClientService(ClientService, UserRequestMixin, GroupRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_user()
        self.initialize_group()
        super().__init__(template, pf_user_id=self.user_id, pf_group_id=self.group_id, **kwargs)

    def list_project(self):
        def _get_response_dict(cursor: Optional[str]):
            try:
                response = self.list(params={"cursor": cursor})
            except HTTPError as exc:
                secho_error_and_exit(f"Failed to list projects.\n{decode_http_err(exc)}")

            return response.json()

        response_dict = _get_response_dict(None)
        projects = response_dict['results']
        next_cursor = response_dict['next_cursor']
        while next_cursor is not None:
            response_dict = _get_response_dict(next_cursor)
            projects.extend(response_dict['results'])
            next_cursor = response_dict['next_cursor']

        return projects


class GroupClientService(ClientService):
    def create_group(self, name: str):
        try:
            response = self.post(data=json.dumps({"name": name, "hosting_type": "hosted"}))
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to post an organization.\n{decode_http_err(exc)}")

        return response.json()

    def get_group(self, pf_group_id: uuid.UUID):
        try:
            response = self.retrieve(pf_group_id)
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to get an organization.\n{decode_http_err(exc)}")

        return response.json()


class GroupProjectClientService(ClientService, GroupRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_group()
        super().__init__(template, pf_group_id=self.group_id, **kwargs)

    def create_project(self, name: str):
        try:
            response = self.post(data=json.dumps({"name": name}))
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to post a project.\n{decode_http_err(exc)}")

        return response.json()

    def list_project(self):
        def _get_response_dict(cursor: Optional[str]):
            try:
                response = self.list(params={"cursor": cursor})
            except HTTPError as exc:
                secho_error_and_exit(f"Failed to list projects.\n{decode_http_err(exc)}")

            return response.json()

        response_dict = _get_response_dict(None)
        projects = response_dict['results']
        next_cursor = response_dict['next_cursor']
        while next_cursor is not None:
            response_dict = _get_response_dict(next_cursor)
            projects.extend(response_dict['results'])
            next_cursor = response_dict['next_cursor']

        return projects


class ProjectClientService(ClientService):
    def get_project(self, pf_project_id: uuid.UUID):
        try:
            response = self.retrieve(pk=pf_project_id)
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to get a project.\n{decode_http_err(exc)}")

        return response.json()


class ExperimentClientService(ClientService):
    def list_jobs_in_experiment(self, experiment_id: T) -> List[dict]:
        try:
            response = self.list(f'{experiment_id}/job/')
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to fetch jobs in the experiment.\n{decode_http_err(exc)}")
        return response.json()['results']

    def delete_experiment(self, experiment_id: T) -> None:
        try:
            response = self.delete(experiment_id)
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to delete experiment. {exc}")

    def update_experiment_name(self, experiment_id: T, name: str) -> dict:
        try:
            response = self.partial_update(experiment_id, json={'name': name})
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to update the name of experiment to {name}.\n{decode_http_err(exc)}")
        return response.json()


class ProjectExperimentClientService(ClientService, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_experiments(self):
        try:
            response = self.list()
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to list experiments.\n{decode_http_err(exc)}")
        return response.json()

    def get_id_by_name(self, name: str) -> Optional[T]:
        try:
            response = self.list()
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to get experiment info.\n{decode_http_err(exc)}")
        for experiment in response.json():
            if experiment['name'] == name:
                return experiment['id']
        return None

    def create_experiment(self, name: str) -> dict:
        try:
            response = self.post(data={'name': name})
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to post new experiment.\n{decode_http_err(exc)}")
        return response.json()


class JobClientService(ClientService):
    def list_jobs(self) -> dict:
        try:
            response = self.list()
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to list jobs.\n{decode_http_err(exc)}")
        return response.json()['results']

    def get_job(self, job_id: int) -> dict:
        try:
            response = self.retrieve(job_id)
        except HTTPError as exc:
            secho_error_and_exit(f"Job ({job_id}) is not found. You may enter wrong ID.\n{decode_http_err(exc)}")
        return response.json()

    def run_job(self, config: dict, workspace_dir: Optional[Path]) -> dict:
        try:
            if workspace_dir is not None:
                workspace_size = get_path_size(workspace_dir)
                if workspace_size <= 0 or workspace_size > 100 * 1024 * 1024:
                    secho_error_and_exit(f"Workspace directory size ({decimal(workspace_size)}) should be 0 < size <= 100MB.")
                workspace_zip = Path(workspace_dir.parent / (workspace_dir.name + ".zip"))
                with zip_dir(workspace_dir, workspace_zip) as zip_file:
                    files = {'workspace_zip': ('workspace.zip', zip_file)}
                    response = self.post(data={"data": json.dumps(config)}, files=files)
            else:
                response = self.post(json=config)
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to run job.\n{decode_http_err(exc)}")
        return response.json()

    def cancel_job(self, job_id: int) -> None:
        try:
            response = self.post(path=f"{job_id}/cancel/")
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to cancel job ({job_id}).\n{decode_http_err(exc)}")

    def terminate_job(self, job_id: int) -> None:
        try:
            response = self.post(path=f"{job_id}/terminate/")
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to terminate job ({job_id}).\n{decode_http_err(exc)}")

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

        try:
            response = self.list(path=f'{job_id}/text_log/')
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to fetch text logs.\n{decode_http_err(exc)}")
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
            secho_error_and_exit(f"Error occurred while decoding websocket response...")


class JobCheckpointClientService(ClientService):
    def list_checkpoints(self) -> dict:
        try:
            response = self.list()
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to list checkpoints.\n{decode_http_err(exc)}")
        return response.json()


class JobArtifactClientService(ClientService):
    def list_artifacts(self) -> dict:
        try:
            response = self.list()
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to list artifacts.\n{decode_http_err(exc)}")
        return response.json()


class ProjectJobClientService(ClientService, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_jobs(self) -> dict:
        try:
            response = self.list()
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to list jobs in your group.\n{decode_http_err(exc)}")
        return response.json()['results']


class JobTemplateClientService(ClientService):
    def list_job_template_names(self) -> List[str]:
        try:
            response = self.list()
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to list job template names.\n{decode_http_err(exc)}")
        return [ template['name'] for template in response.json() ]

    def get_job_template_by_name(self, name: str) -> Optional[dict]:
        try:
            response = self.list()
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to get job template.\n{decode_http_err(exc)}")
        for template in response.json():
            if template['name'] == name:
                return template
        return None


class DataClientService(ClientService):
    def get_datastore(self, datastore_id: T) -> dict:
        try:
            response = self.retrieve(datastore_id)
        except HTTPError as exc:
            secho_error_and_exit(f"Datastore ({datastore_id}) is not found.\n{decode_http_err(exc)}")
        return response.json()

    def update_datastore(self,
                         datastore_id: T,
                         *,
                         name: Optional[str] = None,
                         vendor: Optional[StorageType] = None,
                         region: Optional[str] = None,
                         storage_name: Optional[str] = None,
                         credential_id: Optional[str] = None,
                         metadata: Optional[dict] = None,
                         files: Optional[List[dict]] = None,
                         active: Optional[bool] = None) -> dict:
        # Valdiate region
        if vendor is not None or region is not None:
            prev_info = self.get_datastore(datastore_id)
            validate_storage_region(
                vendor or storage_type_map_inv[prev_info['vendor']],
                region or prev_info['region']
            )

        request_data = {}
        if name is not None:
            request_data['name'] = name
        if vendor is not None:
            request_data['vendor'] = vendor
        if region is not None:
            request_data['region'] = region
        if storage_name is not None:
            request_data['storage_name'] = storage_name
        if credential_id is not None:
            request_data['credential_id'] = credential_id
        if metadata is not None:
            request_data['metadata'] = metadata
        if files is not None:
            request_data['files'] = files
        if active is not None:
            request_data['active'] = active
        try:
            response = self.partial_update(datastore_id, json=request_data)
        except HTTPError as exc:
            secho_error_and_exit(f"Cannot update datastore.\n{decode_http_err(exc)}")
        return response.json()

    def delete_datastore(self, datastore_id: T) -> None:
        try:
            response = self.delete(datastore_id)
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to delete datastore ({datastore_id}).\n{decode_http_err(exc)}")

    def get_upload_urls(self, datastore_id: T, src_path: Path, expand: bool) -> List[Dict[str, str]]:
        if src_path.is_file():
            paths = [str(src_path.name)]
        else:
            paths = list(src_path.rglob('*'))
            rel_path = src_path if expand else src_path.parent
            paths = [str(f.relative_to(rel_path)) for f in paths if f.is_file()]
        if len(paths) == 0:
            secho_error_and_exit(f"No file exists in this path ({src_path})")
        try:
            response = self.post(path=f'{datastore_id}/upload/', json={'paths': paths})
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to get presigned URLs.\n{decode_http_err(exc)}")
        return response.json()


class ProjectDataClientService(ClientService, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_datastores(self) -> dict:
        try:
            response = self.list()
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to list dataset info.\n{decode_http_err(exc)}")
        return response.json()

    def get_id_by_name(self, name: str) -> Optional[T]:
        datastores = self.list_datastores()
        for datastore in datastores:
            if datastore['name'] == name:
                return datastore['id']
        return None

    def create_datastore(self,
                         name: str,
                         vendor: StorageType,
                         region: str,
                         storage_name: str,
                         credential_id: Optional[T],
                         metadata: dict,
                         files: List[dict],
                         active: bool) -> dict:
        validate_storage_region(vendor, region)

        vendor_name = storage_type_map[vendor]
        request_data = {
            "name": name,
            "vendor": vendor_name,
            "region": region,
            "storage_name": storage_name,
            "credential_id": credential_id,
            "metadata": metadata,
            "files": files,
            "active": active,
        }
        try:
            response = self.post(json=request_data)
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to post a new datastore.\n{decode_http_err(exc)}")
        return response.json()


class ProjectVMQuotaClientService(ClientService, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_vm_quotas(self,
                       vendor: Optional[CloudType] = None,
                       region: Optional[str] = None,
                       device_type: Optional[str] = None) -> Optional[List[dict]]:
        try:
            response = self.list()
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to list VM quota info.\n{decode_http_err(exc)}")
        vm_dict_list = response.json()
        if vendor is not None:
            vm_dict_list = list(filter(lambda info: info['vm_instance_type']['vendor'] == vendor, vm_dict_list))
        if region is not None:
            vm_dict_list = list(filter(lambda info: info['vm_instance_type']['region'] == region, vm_dict_list))
        if device_type is not None:
            vm_dict_list = list(filter(lambda info: info['vm_instance_type']['device_type'] == device_type, vm_dict_list))
        return vm_dict_list


class VMConfigClientService(ClientService):
    def list_vm_locks(self, vm_config_id: T, lock_type: LockType) -> List[dict]:
        try:
            response = self.list(path=f'{vm_config_id}/vm_lock/', params={'lock_type': lock_type})
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to inspect locked VMs.\n{decode_http_err(exc)}")
        return response.json()

    def get_active_vm_count(self, vm_config_id: T) -> int:
        vm_locks = self.list_vm_locks(vm_config_id, LockType.ACTIVE)
        return len(vm_locks)


class GroupVMConfigClientService(ClientService, GroupRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_group()
        super().__init__(template, group_id=self.group_id, **kwargs)

    def list_vm_config(self) -> List[dict]:
        try:
            response = self.list()
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to list available VM list.\n{decode_http_err(exc)}")
        return response.json()

    def get_vm_config_id_map(self) -> Dict[str, T]:
        id_map = {}
        for vm_config in self.list_vm_config():
            id_map[vm_config['vm_config_type']['vm_instance_type']['code']] = vm_config['id']
        return id_map

    def get_id_by_name(self, name: str) -> Optional[T]:
        for vm_config in self.list_vm_config():
            if vm_config['vm_config_type']['vm_instance_type']['code'] == name:
                return vm_config['id']
        return None


class CredentialClientService(ClientService):
    def get_credential(self, credential_id: T) -> dict:
        try:
            response = self.retrieve(credential_id)
        except HTTPError as exc:
            secho_error_and_exit(f"Credential ({credential_id}) is not found.\n{decode_http_err(exc)}")
        return response.json()

    def update_credential(self,
                          credential_id: T,
                          *,
                          name: Optional[str] = None,
                          type_version: Optional[str] = None,
                          value: Optional[dict]= None) -> dict:
        request_data = {}
        if name is not None:
            request_data['name'] = name
        if type_version is not None:
            request_data['type_version'] = type_version
        if value is not None:
            request_data['value'] = value
        try:
            response = self.partial_update(credential_id, json=request_data)
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to update credential ({credential_id}).\n{decode_http_err(exc)}")
        return response.json()

    def delete_credential(self, credential_id) -> None:
        try:
            response = self.delete(credential_id)
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to delete credential ({credential_id}).\n{decode_http_err(exc)}")


class ProjectCredentialClientService(ClientService, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_credentials(self, cred_type: CredType) -> List[dict]:
        type_name = cred_type_map[cred_type]
        try:
            response = self.list(params={"type": type_name})
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to credential for {cred_type}.\n{decode_http_err(exc)}")
        return response.json()

    def create_credential(self,
                          cred_type: CredType,
                          name: str,
                          type_version: int,
                          value: dict) -> dict:
        type_name = cred_type_map[cred_type]
        request_data = {
            "type": type_name,
            "name": name,
            "type_version": type_version,
            "value": value
        }
        try:
            response = self.post(json=request_data)
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to creat user credential.\n{decode_http_err(exc)}")
        return response.json()


class CredentialTypeClientService(ClientService):
    def get_schema_by_type(self, cred_type: CredType) -> Optional[dict]:
        type_name = cred_type_map[cred_type]
        try:
            response = self.list()
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to get credential schema.\n{decode_http_err(exc)}")
        for cred_type_json in response.json():
            if cred_type_json['type_name'] == type_name:
                return cred_type_json['versions'][-1]['schema']     # use the latest version
        return None


class CheckpointClientService(ClientService):
    def get_checkpoint(self, checkpoint_id: T) -> dict:
        try:
            response = self.retrieve(checkpoint_id)
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to get info of checkpoint ({checkpoint_id}).\n{decode_http_err(exc)}")
        return response.json()

    def update_checkpoint(self,
                          checkpoint_id: T,
                          *,
                          vendor: Optional[StorageType] = None,
                          region: Optional[str] = None,
                          credential_id: Optional[str] = None,
                          iteration: Optional[int] = None,
                          storage_name: Optional[str] = None,
                          files: Optional[List[dict]] = None,
                          dist_config: Optional[dict] = None,
                          data_config: Optional[dict] = None,
                          job_setting_config: Optional[dict] = None) -> dict:
        request_data = {}
        if vendor is not None:
            request_data['vendor'] = vendor
        if region is not None:
            request_data['region'] = region
        if credential_id is not None:
            request_data['credential_id'] = credential_id
        if iteration is not None:
            request_data['iteration'] = iteration
        if storage_name is not None:
            request_data['storage_name'] = storage_name
        if files is not None:
            request_data['files'] = files
        if dist_config is not None:
            request_data['dist_json'] = dist_config
        if data_config is not None:
            request_data['data_json'] = data_config
        if job_setting_config is not None:
            request_data['job_setting_json'] = job_setting_config

        try:
            response = self.partial_update(checkpoint_id, json=request_data)
        except HTTPError as exc:
            secho_error_and_exit(f"Cannot update checkpoint.\n{decode_http_err(exc)}")
        return response.json()

    def delete_checkpoint(self, checkpoint_id: T) -> None:
        try:
            response = self.delete(checkpoint_id)
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to delete checkpoint ({checkpoint_id}).\n{decode_http_err(exc)}")
        return response

    @auto_token_refresh
    def download(self, checkpoint_id: T) -> Response:
        try:
            response = self.retrieve(checkpoint_id)
            model_form_id = response.json()['forms'][0]['id']
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to get info of checkpoint ({checkpoint_id}).\n{decode_http_err(exc)}")

        url_template = self.url_template.copy()
        url_template.replace_path('model_forms/$model_form_id/download/')
        return requests.get(
            url_template.render(model_form_id=model_form_id, **self.url_kwargs),
            headers=get_auth_header(),
        )

    def get_checkpoint_download_urls(self, checkpoint_id: T) -> List[dict]:
        try:
            response = self.download(checkpoint_id)
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to get download URLs of checkpoint files.\n{decode_http_err(exc)}")
        return response.json()['files']


class GroupProjectCheckpointClientService(ClientService, UserRequestMixin, GroupRequestMixin, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_user()
        self.initialize_group()
        self.initialize_project()
        super().__init__(
            template,
            group_id=self.group_id,
            project_id=self.project_id,
            **kwargs,
        )

    def list_checkpoints(self, category: Optional[CheckpointCategory]) -> dict:
        request_data = {}
        if category is not None:
            request_data['category'] = category.value

        # TODO (AC): Add pagination
        try:
            response = self.list(params=request_data)
        except HTTPError as exc:
            secho_error_and_exit(f"Cannot list checkpoints in your group.\n{decode_http_err(exc)}")
        return response.json()['results']

    def create_checkpoint(self,
                          name: str,
                          model_form_category: ModelFormCategory,
                          vendor: StorageType,
                          region: str,
                          credential_id: str,
                          iteration: int,
                          storage_name: str,
                          files: List[dict],
                          dist_config: dict,
                          data_config: dict,
                          job_setting_config: Optional[dict]) -> dict:
        validate_storage_region(vendor, region)

        request_data = {
            "job_id": None,
            "name": name,
            "attributes": {
                "data_json": data_config,
                "job_setting_json": job_setting_config,
            },
            "user_id": str(self.user_id),
            "credential_id": credential_id if credential_id else None,
            "model_category": "USER",
            "form_category": model_form_category.value,
            "dist_json": dist_config,
            "vendor": vendor,
            "region": region,
            "storage_name": storage_name,
            "iteration": iteration,
            "files": files
        }

        try:
            response = self.post(json=request_data)
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to post checkpoint.\n{decode_http_err(exc)}")
        return response.json() 


class ServeClientService(ClientService):
    def get_serve(self, serve_id: T) -> dict:
        try:
            response = self.retrieve(serve_id)
        except HTTPError as exc:
            secho_error_and_exit(f"Serve ({serve_id}) is not found. You may enter wrong ID.\n{decode_http_err(exc)}")
        return response.json()
    
    def create_serve(self, config = dict) -> dict:
        try:
            response = self.post(json=config)
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to post new serve.\n{decode_http_err(exc)}")
        return response.json()

    def list_serves(self) -> dict:
        try:
            response = self.list()
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to list serves.\n{decode_http_err(exc)}")
        return response.json()
    
    def delete_serve(self, serve_id: T) -> None:
        try:
            response = self.delete(serve_id)
        except HTTPError as exc:
            secho_error_and_exit(f"Failed to delete serve. {exc}")


client_template_map: Dict[ServiceType, Tuple[Type[ClientService], Template]] = {
    ServiceType.USER: (UserClientService, Template(get_auth_uri('pf_user'))),
    ServiceType.USER_GROUP: (UserGroupClientService, Template(get_auth_uri('pf_user/$pf_user_id/pf_group'))),
    ServiceType.USER_GROUP_PROJECT: (UserGroupProjectClientService, Template(get_auth_uri('pf_user/$pf_user_id/pf_group/$pf_group_id/pf_project'))),  # pylint: disable=line-too-long
    ServiceType.PROJECT: (ProjectClientService, Template(get_auth_uri('pf_project'))),
    ServiceType.GROUP: (GroupClientService, Template(get_auth_uri('pf_group'))),
    ServiceType.GROUP_PROJECT: (GroupProjectClientService, Template(get_auth_uri('pf_group/$pf_group_id/pf_project'))),
    ServiceType.EXPERIMENT: (ExperimentClientService, Template(get_uri('experiment/'))),
    ServiceType.PROJECT_EXPERIMENT: (ProjectExperimentClientService, Template(get_uri('project/$project_id/experiment/'))),  # pylint: disable=line-too-long
    ServiceType.JOB: (JobClientService, Template(get_uri('job/'))),
    ServiceType.JOB_CHECKPOINT: (JobCheckpointClientService, Template(get_uri('job/$job_id/checkpoint/'))),
    ServiceType.JOB_ARTIFACT: (JobArtifactClientService, Template(get_uri('job/$job_id/artifact/'))),
    ServiceType.PROJECT_JOB: (ProjectJobClientService, Template(get_uri('project/$project_id/job/'))),
    ServiceType.JOB_TEMPLATE: (JobTemplateClientService, Template(get_uri('job_template/'))),
    ServiceType.CREDENTIAL: (CredentialClientService, Template(get_auth_uri('credential'))),
    ServiceType.PROJECT_CREDENTIAL: (ProjectCredentialClientService, Template(get_auth_uri('pf_project/$project_id/credential'))),  # pylint: disable=line-too-long
    ServiceType.CREDENTIAL_TYPE: (CredentialTypeClientService, Template(get_uri('credential_type/'))),
    ServiceType.DATA: (DataClientService, Template(get_uri('datastore/'))),
    ServiceType.PROJECT_DATA: (ProjectDataClientService, Template(get_uri('project/$project_id/datastore/'))),
    ServiceType.PROJECT_VM_QUOTA: (ProjectVMQuotaClientService, Template(get_uri('project/$project_id/vm_quota/'))),
    ServiceType.CHECKPOINT: (CheckpointClientService, Template(get_mr_uri('models/'))),
    ServiceType.GROUP_PROJECT_CHECKPOINT: (GroupProjectCheckpointClientService, Template(get_mr_uri('orgs/$group_id/prjs/$project_id/models/'))),  # pylint: disable=line-too-long
    ServiceType.VM_CONFIG: (VMConfigClientService, Template(get_uri('vm_config/'))),
    ServiceType.GROUP_VM_CONFIG: (GroupVMConfigClientService, Template(get_uri('group/$group_id/vm_config/'))),
    ServiceType.JOB_WS: (JobWebSocketClientService, Template(get_wss_uri('job/'))),
    ServiceType.SERVE: (ServeClientService, Template(get_pfs_uri('deployment/'))),
}


_ClientService = TypeVar('_ClientService', bound=ClientService)


def build_client(request_type: ServiceType, **kwargs) -> _ClientService:
    """Factory function to post client service.

    Args:
        request_type (RequestAPI):

    Returns:
        ClientService: created client service
    """
    cls, template = client_template_map[request_type]
    return cls(template, **kwargs)
