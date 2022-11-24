# Copyright (C) 2022 FriendliAI

"""PeriFlow GroupClient Service"""

import json
import uuid
from string import Template
from typing import Dict, List, Optional
from uuid import UUID

from pfcli.service import CheckpointCategory, ModelFormCategory, StorageType
from pfcli.service.client.base import (
    ClientService,
    GroupRequestMixin,
    ProjectRequestMixin,
    T,
    UserRequestMixin,
    safe_request,
)
from pfcli.utils.request import paginated_get
from pfcli.utils.validate import validate_storage_region


class GroupClientService(ClientService):
    def create_group(self, name: str) -> dict:
        response = safe_request(
            self.post, err_prefix="Failed to post an organization."
        )(data=json.dumps({"name": name, "hosting_type": "hosted"}))
        return response.json()

    def get_group(self, pf_group_id: uuid.UUID) -> dict:
        response = safe_request(
            self.retrieve, err_prefix="Failed to get an organization."
        )(pk=pf_group_id)
        return response.json()

    def invite_to_group(self, pf_group_id: uuid.UUID, email: str) -> None:
        safe_request(self.post, err_prefix="Failed to send invitation")(
            path=f"{pf_group_id}/invite", json={"email": email, "msg": ""}
        )

    def accept_invite(self, token: str, key: str) -> None:
        safe_request(self.post, err_prefix="Invalid code... Please Try again.")(
            path="invite/confirm", json={"email_token": token, "key": key}
        )

    def get_users(self, pf_group_id: uuid.UUID, username: str) -> List[dict]:
        get_response_dict = safe_request(
            self.list, err_prefix="Failed to get user in organization"
        )
        return paginated_get(
            get_response_dict, path=f"{pf_group_id}/pf_user", search=username
        )

    def list_users(self, pf_group_id: uuid.UUID) -> List[dict]:
        get_response_dict = safe_request(
            self.list, err_prefix="Failed to list users in organization"
        )
        return paginated_get(get_response_dict, path=f"{pf_group_id}/pf_user")


class GroupProjectClientService(ClientService, GroupRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_group()
        super().__init__(template, pf_group_id=self.group_id, **kwargs)

    def create_project(self, name: str) -> dict:
        response = safe_request(self.post, err_prefix="Failed to post a project.")(
            data=json.dumps({"name": name})
        )
        return response.json()

    def list_projects(self) -> List[dict]:
        get_response_dict = safe_request(
            self.list, err_prefix="Failed to list projects."
        )
        return paginated_get(get_response_dict)


class PFTGroupVMConfigClientService(ClientService, GroupRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_group()
        super().__init__(template, group_id=self.group_id, **kwargs)

    def list_vm_configs(self) -> List[dict]:
        response = safe_request(
            self.list, err_prefix="Failed to list available VM list."
        )()
        return response.json()

    def get_vm_config_id_map(self) -> Dict[str, int]:
        id_map = {}
        for vm_config in self.list_vm_configs():
            id_map[vm_config["vm_config_type"]["code"]] = vm_config["id"]
        return id_map

    def get_id_by_name(self, name: str) -> Optional[int]:
        for vm_config in self.list_vm_configs():
            if vm_config["vm_config_type"]["code"] == name:
                return vm_config["id"]
        return None


class GroupProjectCheckpointClientService(
    ClientService, UserRequestMixin, GroupRequestMixin, ProjectRequestMixin
):
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

    def list_checkpoints(self, category: Optional[CheckpointCategory]) -> List[dict]:
        request_data = {}
        if category is not None:
            request_data["category"] = category.value

        get_response_dict = safe_request(
            self.list, err_prefix="Failed to list checkpoints."
        )
        return paginated_get(get_response_dict, **request_data)

    def create_checkpoint(
        self,
        name: str,
        model_form_category: ModelFormCategory,
        vendor: StorageType,
        region: str,
        credential_id: UUID,
        iteration: int,
        storage_name: str,
        files: List[dict],
        dist_config: dict,
        data_config: dict,
        job_setting_config: Optional[dict],
    ) -> dict:
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
            "files": files,
        }

        response = safe_request(self.post, err_prefix="Failed to post checkpoint.")(
            json=request_data
        )
        return response.json()


class GroupProjectVMQuotaClientService(ClientService[UUID], GroupRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_group()
        super().__init__(template, group_id=self.group_id, **kwargs)

    def list_quota(self, vm_instance_name: str, project_id: Optional[T] = None):
        params = {"vm_config_type_code": vm_instance_name}
        if project_id is not None:
            params["project_id"] = str(project_id)
        response = safe_request(
            self.list, err_prefix="Failed to list Project VM Quotas."
        )(params=params)
        return response.json()

    def create_project_quota(self, vm_instance_name: str, project_id: UUID, quota: int):
        response = safe_request(
            self.post, err_prefix="Failed to create project quota."
        )(
            json={
                "group_id": str(self.group_id),
                "vm_config_type_code": vm_instance_name,
                "project_id": str(project_id),
                "quota": quota,
            }
        )
        return response.json()

    def update_project_quota(self, quota_id: UUID, new_quota: int):
        response = safe_request(
            self.post, err_prefix=f"Failed to update project quota to {new_quota}."
        )(pk=quota_id, json={"quota": new_quota})
        return response.json()

    def delete_quota(self, quota_id: UUID):
        safe_request(self.delete, err_prefix="Failed to delete project quota.")(
            pk=quota_id
        )
