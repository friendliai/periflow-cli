# Copyright (C) 2022 FriendliAI

"""PeriFlow CheckpointClient Service"""

from typing import List, Optional
from uuid import UUID

import requests
from requests.models import Response

from pfcli.service import StorageType
from pfcli.service.auth import auto_token_refresh, get_auth_header
from pfcli.service.client.base import ClientService, safe_request


class CheckpointClientService(ClientService[UUID]):
    def get_checkpoint(self, checkpoint_id: UUID) -> dict:
        response = safe_request(
            self.retrieve, err_prefix="Failed to get info of checkpoint"
        )(pk=checkpoint_id)
        return response.json()

    def update_checkpoint(
        self,
        checkpoint_id: UUID,
        *,
        vendor: Optional[StorageType] = None,
        region: Optional[str] = None,
        credential_id: Optional[str] = None,
        iteration: Optional[int] = None,
        storage_name: Optional[str] = None,
        files: Optional[List[dict]] = None,
        dist_config: Optional[dict] = None,
        data_config: Optional[dict] = None,
        job_setting_config: Optional[dict] = None
    ) -> dict:
        request_data = {}
        if vendor is not None:
            request_data["vendor"] = vendor
        if region is not None:
            request_data["region"] = region
        if credential_id is not None:
            request_data["credential_id"] = credential_id
        if iteration is not None:
            request_data["iteration"] = iteration
        if storage_name is not None:
            request_data["storage_name"] = storage_name
        if files is not None:
            request_data["files"] = files
        if dist_config is not None:
            request_data["dist_json"] = dist_config
        if data_config is not None:
            request_data["data_json"] = data_config
        if job_setting_config is not None:
            request_data["job_setting_json"] = job_setting_config

        response = safe_request(
            self.partial_update, err_prefix="Cannot update checkpoint."
        )(pk=checkpoint_id, json=request_data)
        return response.json()

    def delete_checkpoint(self, checkpoint_id: UUID) -> Response:
        response = safe_request(self.delete, err_prefix="Failed to delete checkpoint.")(
            pk=checkpoint_id
        )
        return response

    @auto_token_refresh
    def download(self, checkpoint_id: UUID) -> Response:
        response = safe_request(
            self.retrieve, err_prefix="Failed to get info of checkpoint."
        )(pk=checkpoint_id)
        model_form_id = response.json()["forms"][0]["id"]

        url_template = self.url_template.copy()
        url_template.replace_path("model_forms/$model_form_id/download/")
        return requests.get(
            url_template.render(model_form_id=model_form_id, **self.url_kwargs),
            headers=get_auth_header(),
        )

    def get_checkpoint_download_urls(self, checkpoint_id: UUID) -> List[dict]:
        response = safe_request(
            self.download, err_prefix="Failed to get download URLs of checkpoint files."
        )(checkpoint_id=checkpoint_id)
        return response.json()["files"]
