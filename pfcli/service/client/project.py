# Copyright (C) 2022 FriendliAI

"""PeriFlow ProjectClient Service"""


import json
import uuid
from pathlib import Path
from string import Template
from typing import List, Optional
from requests import HTTPError

from rich.filesize import decimal

from pfcli.service import CloudType, CredType, LockType, StorageType, cred_type_map, storage_type_map
from pfcli.service.client.base import ClientService, ProjectRequestMixin, T, safe_request
from pfcli.utils import get_workspace_files, paginated_get, secho_error_and_exit, validate_storage_region, zip_dir


class ProjectClientService(ClientService):
    def get_project(self, pf_project_id: uuid.UUID) -> dict:
        response = safe_request(self.retrieve, err_prefix="Failed to get a project.")(
            pk=pf_project_id
        )
        return response.json()

    def check_project_membership(self, pf_project_id: uuid.UUID) -> bool:
        try:
            self.retrieve(pf_project_id)
        except HTTPError:
            return False
        else:
            return True

    def delete_project(self, pf_project_id: uuid.UUID) -> None:
        safe_request(self.delete, err_prefix="Failed to delete a project.")(
            pk=pf_project_id
        )

    def list_users(self, pf_project_id: str) -> List[dict]:
        get_response_dict = safe_request(self.list, err_prefix="Failed to list users in the current project")
        return paginated_get(get_response_dict, path=f"{pf_project_id}/pf_user")


class ProjectExperimentClientService(ClientService, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_experiments(self) -> List[dict]:
        response = safe_request(self.list, err_prefix="Failed to list experiments.")()
        return response.json()

    def get_id_by_name(self, name: str) -> Optional[T]:
        response = safe_request(self.list, err_prefix="Failed to get experiment info.")()
        for experiment in response.json():
            if experiment['name'] == name:
                return experiment['id']
        return None

    def create_experiment(self, name: str) -> dict:
        response = safe_request(self.post, err_prefix="Failed to post new experiment.")(
            data={"name": name}
        )
        return response.json()


class ProjectJobClientService(ClientService, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_jobs(self) -> List[dict]:
        return paginated_get(safe_request(self.list, err_prefix="Failed to list jobs in project."))

    def run_job(self, config: dict, workspace_dir: Optional[Path]) -> dict:
        job_request = safe_request(self.post, err_prefix="Failed to run job.")
        if workspace_dir is not None:
            workspace_dir = workspace_dir.resolve()
            workspace_files = get_workspace_files(workspace_dir)
            workspace_size = sum(f.stat().st_size for f in workspace_files)
            if workspace_size <= 0 or workspace_size > 100 * 1024 * 1024:
                secho_error_and_exit(
                    f"Workspace directory size ({decimal(workspace_size)}) should be 0 < size <= 100MB."
                )
            workspace_zip = Path(workspace_dir.parent / (workspace_dir.name + ".zip"))
            with zip_dir(workspace_dir, workspace_files, workspace_zip) as zip_file:
                files = {'workspace_zip': ('workspace.zip', zip_file)}
                response = job_request(
                    data={"data": json.dumps(config)}, files=files
                )
        else:
            response = job_request(json=config)
        return response.json()


class ProjectDataClientService(ClientService, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_datastores(self) -> List[dict]:
        response = safe_request(self.list, err_prefix="Failed to list dataset info.")()
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
        response = safe_request(self.post, err_prefix="Failed to create a new datastore.")(
            json=request_data
        )
        return response.json()


class ProjectVMQuotaClientService(ClientService, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_vm_quotas(self,
                       vendor: Optional[CloudType] = None,
                       region: Optional[str] = None,
                       device_type: Optional[str] = None) -> Optional[List[dict]]:
        response = safe_request(self.list, err_prefix="Failed to list VM quota info.")()
        vm_dict_list = response.json()
        if vendor is not None:
            vm_dict_list = list(filter(lambda info: info['vm_config_type']['vm_instance_type']['vendor'] == vendor, vm_dict_list))
        if region is not None:
            vm_dict_list = list(filter(lambda info: info['vm_config_type']['vm_instance_type']['region'] == region, vm_dict_list))
        if device_type is not None:
            vm_dict_list = list(filter(lambda info: info['vm_config_type']['vm_instance_type']['device_type'] == device_type, vm_dict_list))
        return vm_dict_list


class ProjectCredentialClientService(ClientService, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_credentials(self, cred_type: CredType) -> List[dict]:
        type_name = cred_type_map[cred_type]
        response = safe_request(self.list, err_prefix=f"Failed to list credential for {cred_type}.")(
            params={"type": type_name}
        )
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
        response = safe_request(self.post, err_prefix="Failed to create user credential.")(
            json=request_data
        )
        return response.json()


class ProjectVMConfigClientService(ClientService, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_vm_locks(self, vm_config_id: T, lock_type: LockType) -> List[dict]:
        response = safe_request(self.list, err_prefix="Failed to inspect locked VMs.")(
            path=f"{vm_config_id}/vm_lock/",
            params={"lock_type": lock_type}
        )
        return response.json()

    def get_active_vm_count(self, vm_config_id: T) -> int:
        vm_locks = self.list_vm_locks(vm_config_id, LockType.ACTIVE)
        return len(vm_locks)
