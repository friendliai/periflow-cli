# Copyright (C) 2022 FriendliAI

"""PeriFlow GroupClient Service"""


import json
import uuid
from string import Template
from typing import Dict, List, Optional

from pfcli.service import CheckpointCategory, ModelFormCategory, StorageType
from pfcli.service.client.base import (
    ClientService,
    GroupRequestMixin,
    ProjectRequestMixin,
    T,
    UserRequestMixin,
    safe_request
)
from pfcli.utils import validate_storage_region


class GroupClientService(ClientService):
    def create_group(self, name: str):
        response = safe_request(self.post, prefix="Failed to post an organization.")(
            data=json.dumps({"name": name, "hosting_type": "hosted"})
        )
        return response.json()

    def get_group(self, pf_group_id: uuid.UUID):
        response = safe_request(self.retrieve, prefix="Failed to get an organization.")(
            pk=pf_group_id
        )
        return response.json()


class GroupProjectClientService(ClientService, GroupRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_group()
        super().__init__(template, pf_group_id=self.group_id, **kwargs)

    def create_project(self, name: str):
        response = safe_request(self.post, prefix="Failed to post a project.")(
            data=json.dumps({"name": name})
        )
        return response.json()

    def list_project(self):
        get_response_dict = safe_request(self.list, prefix="Failed to list projects.")

        response_dict = get_response_dict()
        projects = response_dict['results']
        next_cursor = response_dict['next_cursor']
        while next_cursor is not None:
            response_dict = get_response_dict(
                params={"cursor": next_cursor}
            )
            projects.extend(response_dict['results'])
            next_cursor = response_dict['next_cursor']

        return projects


class GroupVMConfigClientService(ClientService, GroupRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_group()
        super().__init__(template, group_id=self.group_id, **kwargs)

    def list_vm_config(self) -> List[dict]:
        response = safe_request(self.list, prefix="Failed to list available VM list.")()
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
        response = safe_request(self.list, prefix="Cannot list checkpoints in your group.")(
            params=request_data
        )
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

        response = safe_request(self.post, prefix="Failed to post checkpoint.")(
            json=request_data
        )
        return response.json()
