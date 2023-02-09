# Copyright (C) 2022 FriendliAI

"""PeriFlow CheckpointClient Service"""

from __future__ import annotations

from typing import Any, Dict, List
from uuid import UUID

from requests.models import Response

from pfcli.service.client.base import (
    ClientService,
    UploadableClientService,
    safe_request,
)


class CheckpointClientService(ClientService[UUID]):
    def get_checkpoint(self, checkpoint_id: UUID) -> Dict[str, Any]:
        response = safe_request(
            self.retrieve, err_prefix="Failed to get info of checkpoint"
        )(pk=checkpoint_id)
        return response.json()

    def get_first_checkpoint_form(self, checkpoint_id: UUID) -> UUID:
        response = safe_request(
            self.retrieve, err_prefix="Failed to get info of checkpoint."
        )(pk=checkpoint_id)
        return UUID(response.json()["forms"][0]["id"])

    def delete_checkpoint(self, checkpoint_id: UUID) -> Response:
        response = safe_request(self.delete, err_prefix="Failed to delete checkpoint.")(
            pk=checkpoint_id
        )
        return response

    def restore_checkpoint(self, checkpoint_id: UUID) -> Dict[str, Any]:
        response = safe_request(self.post, err_prefix="Fail to restore checkpoint.")(
            path=f"{checkpoint_id}/restore/"
        )
        return response.json()


class CheckpointFormClientService(UploadableClientService[UUID]):
    def update_checkpoint_files(
        self,
        ckpt_form_id: UUID,
        files: List[Dict[str, Any]],
    ) -> Dict[str, Any]:

        response = safe_request(
            self.partial_update, err_prefix="Cannot update checkpoint."
        )(pk=ckpt_form_id, json={"files": files})
        return response.json()

    def get_checkpoint_download_urls(self, ckpt_form_id: UUID) -> List[Dict[str, Any]]:
        response = safe_request(
            self.retrieve, err_prefix="Failed to get presigned URLs."
        )(pk=ckpt_form_id, path="download/")
        return response.json()["files"]
