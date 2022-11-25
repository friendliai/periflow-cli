# Copyright (C) 2022 FriendliAI

"""PeriFlow ProjectClient Service"""


from requests import HTTPError
from string import Template
from typing import List, Optional
from uuid import UUID

from pfcli.service import (
    CloudType,
    CredType,
    LockStatus,
    StorageType,
    cred_type_map,
    storage_type_map,
)
from pfcli.service.client.base import (
    ClientService,
    ProjectRequestMixin,
    T,
    safe_request,
)
from pfcli.utils.format import secho_error_and_exit
from pfcli.utils.request import paginated_get
from pfcli.utils.validate import validate_storage_region


def find_project_id(projects: List[dict], project_name: str) -> UUID:
    for project in projects:
        if project["name"] == project_name:
            return UUID(project["id"])
    secho_error_and_exit(f"No project exists with name {project_name}.")


class ProjectClientService(ClientService[UUID]):
    def get_project(self, pf_project_id: UUID) -> dict:
        response = safe_request(self.retrieve, err_prefix="Failed to get a project.")(
            pk=pf_project_id
        )
        return response.json()

    def check_project_membership(self, pf_project_id: UUID) -> bool:
        try:
            self.retrieve(pf_project_id)
        except HTTPError:
            return False
        else:
            return True

    def delete_project(self, pf_project_id: UUID) -> None:
        safe_request(self.delete, err_prefix="Failed to delete a project.")(
            pk=pf_project_id
        )

    def list_users(self, pf_project_id: UUID) -> List[dict]:
        get_response_dict = safe_request(
            self.list, err_prefix="Failed to list users in the current project"
        )
        return paginated_get(get_response_dict, path=f"{pf_project_id}/pf_user")


class ProjectDataClientService(ClientService[int], ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_datasets(self) -> List[dict]:
        response = safe_request(self.list, err_prefix="Failed to list dataset info.")()
        return response.json()

    def get_id_by_name(self, name: str) -> Optional[int]:
        datasets = self.list_datasets()
        for dataset in datasets:
            if dataset["name"] == name:
                return dataset["id"]
        return None

    def create_dataset(
        self,
        name: str,
        vendor: StorageType,
        region: str,
        storage_name: str,
        credential_id: Optional[UUID],
        metadata: dict,
        files: List[dict],
        active: bool,
    ) -> dict:
        validate_storage_region(vendor, region)

        vendor_name = storage_type_map[vendor]
        request_data = {
            "name": name,
            "vendor": vendor_name,
            "region": region,
            "storage_name": storage_name,
            "credential_id": str(credential_id) if credential_id is not None else None,
            "metadata": metadata,
            "files": files,
            "active": active,
        }
        response = safe_request(
            self.post, err_prefix="Failed to create a new dataset."
        )(json=request_data)
        return response.json()


class PFTProjectVMQuotaClientService(ClientService, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_vm_quotas(
        self,
        vendor: Optional[CloudType] = None,
        device_type: Optional[str] = None,
    ) -> List[dict]:
        response = safe_request(self.list, err_prefix="Failed to list VM quota info.")()
        vm_dict_list = response.json()
        if vendor is not None:
            vm_dict_list = list(
                filter(
                    lambda info: info["vm_config_type"]["vendor"] == vendor,
                    vm_dict_list,
                )
            )
        if device_type is not None:
            vm_dict_list = list(
                filter(
                    lambda info: info["vm_config_type"]["device_type"] == device_type,
                    vm_dict_list,
                )
            )
        return vm_dict_list


class ProjectCredentialClientService(ClientService, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_credentials(self, cred_type: CredType) -> List[dict]:
        type_name = cred_type_map[cred_type]
        response = safe_request(
            self.list, err_prefix=f"Failed to list credential for {cred_type}."
        )(params={"type": type_name})
        return response.json()

    def create_credential(
        self, cred_type: CredType, name: str, type_version: int, value: dict
    ) -> dict:
        type_name = cred_type_map[cred_type]
        request_data = {
            "type": type_name,
            "name": name,
            "type_version": type_version,
            "value": value,
        }
        response = safe_request(
            self.post, err_prefix="Failed to create user credential."
        )(json=request_data)
        return response.json()


class PFTProjectVMConfigClientService(ClientService[int], ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_vm_locks(
        self, vm_config_id: int, lock_status_list: List[LockStatus]
    ) -> List[dict]:
        status_param = ",".join(lock_status_list)
        response = safe_request(self.list, err_prefix="Failed to inspect locked VMs.")(
            path=f"{vm_config_id}/vm_lock/", params={"status": status_param}
        )
        return response.json()

    def get_vm_count_in_use(self, vm_config_id: int) -> int:
        vm_locks = self.list_vm_locks(
            vm_config_id, [LockStatus.ACTIVE, LockStatus.DELETING]
        )
        return len(vm_locks)
