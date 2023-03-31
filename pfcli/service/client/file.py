# Copyright (C) 2022 FriendliAI

"""PeriFlow File Service."""

from __future__ import annotations

from string import Template
from typing import Any, Dict
from uuid import UUID

from pfcli.service.client.base import (
    ClientService,
    GroupRequestMixin,
    ProjectRequestMixin,
    UserRequestMixin,
    safe_request,
)


class FileClientService(ClientService[UUID]):
    """File client service."""

    def get_misc_file_upload_url(self, misc_file_id: UUID) -> str:
        """Get an URL to upload file.

        Args:
            misc_file_id (UUID): Misc file ID to upload.

        Returns:
            str: An uploadable URL.

        """
        response = safe_request(self.post, err_prefix="Failed to get file upload URL.")(
            path=f"{misc_file_id}/upload/"
        )
        return response.json()["upload_url"]

    def get_misc_file_download_url(self, misc_file_id: UUID) -> str:
        """Get an URL to download file.

        Args:
            misc_file_id (UUID): Misc file ID to download.

        Returns:
            Dict[str, Any]: A downloadable URL.

        """
        response = safe_request(
            self.post, err_prefix="Failed to get file download URL."
        )(path=f"{misc_file_id}/download/")
        return response.json()["download_url"]

    def make_misc_file_uploaded(self, misc_file_id: UUID) -> Dict[str, Any]:
        """Request to mark the file as uploaded.

        Args:
            misc_file_id (UUID): Misc file ID to change status.

        Returns:
            Dict[str, Any]: The updated file info.

        """
        response = safe_request(
            self.partial_update, err_prefix="Failed to patch the file status."
        )(pk=misc_file_id, path="uploaded/")
        return response.json()


class GroupProjectFileClientService(
    ClientService, UserRequestMixin, GroupRequestMixin, ProjectRequestMixin
):
    """Group-shared file client."""

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

    def create_misc_file(self, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Request to create a misc file.

        Args:
            file_info (Dict[str, Any]): File info.

        Returns:
            Dict[str, Any]: Response body with the created file info.

        """
        request_data = {
            "user_id": str(self.user_id),
            **file_info,
        }
        response = safe_request(self.post, err_prefix="Failed to create file.")(
            json=request_data
        )
        return response.json()
