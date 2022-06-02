# Copyright (C) 2022 FriendliAI

"""PeriFlow ProjectClient Service"""


import uuid
from string import Template
from typing import List, Optional

from pfcli.service import CloudType, CredType, StorageType, cred_type_map, storage_type_map
from pfcli.service.client.base import ClientService, ProjectRequestMixin, T, safe_request
from pfcli.utils import validate_storage_region


class ProjectClientService(ClientService):
    def get_project(self, pf_project_id: uuid.UUID):
        response = safe_request(self.retrieve, prefix="Failed to get a project.")(
            pk=pf_project_id
        )
        return response.json()


class ProjectExperimentClientService(ClientService, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_experiments(self):
        response = safe_request(self.list, prefix="Failed to list experiments.")()
        return response.json()

    def get_id_by_name(self, name: str) -> Optional[T]:
        response = safe_request(self.list, prefix="Failed to get experiment info.")()
        for experiment in response.json():
            if experiment['name'] == name:
                return experiment['id']
        return None

    def create_experiment(self, name: str) -> dict:
        response = safe_request(self.post, prefix="Failed to post new experiment.")(
            data={"name": name}
        )
        return response.json()


class ProjectJobClientService(ClientService, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_jobs(self) -> dict:
        response = safe_request(self.list, prefix="Failed to list jobs in your group.")()
        return response.json()['results']


class ProjectDataClientService(ClientService, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_datastores(self) -> dict:
        response = safe_request(self.list, prefix="Failed to list dataset info.")()
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
        response = safe_request(self.post, prefix="Failed to create a new datastore.")(
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
        response = safe_request(self.list, prefix="Failed to list VM quota info.")()
        vm_dict_list = response.json()
        if vendor is not None:
            vm_dict_list = list(filter(lambda info: info['vm_instance_type']['vendor'] == vendor, vm_dict_list))
        if region is not None:
            vm_dict_list = list(filter(lambda info: info['vm_instance_type']['region'] == region, vm_dict_list))
        if device_type is not None:
            vm_dict_list = list(filter(lambda info: info['vm_instance_type']['device_type'] == device_type, vm_dict_list))
        return vm_dict_list


class ProjectCredentialClientService(ClientService, ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def list_credentials(self, cred_type: CredType) -> List[dict]:
        type_name = cred_type_map[cred_type]
        response = safe_request(self.list, prefix=f"Failed to list credential for {cred_type}.")(
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
        response = safe_request(self.post, prefix="Failed to create user credential.")(
            json=request_data
        )
        return response.json()
