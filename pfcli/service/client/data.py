# Copyright (C) 2022 FriendliAI

"""PeriFlow DataClient Service"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pfcli.service import StorageType, storage_type_map_inv
from pfcli.service.client.base import UploadableClientService, safe_request
from pfcli.utils.validate import validate_storage_region

# The actual hard limit of a part size is 5 GiB, and we use 200 MiB part size.
# See https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html.
S3_MPU_PART_MAX_SIZE = 200 * 1024 * 1024  # 200 MiB
S3_UPLOAD_SIZE_LIMIT = 5 * 1024 * 1024 * 1024  # 5 GiB


class DataClientService(UploadableClientService[int]):
    def get_dataset(self, dataset_id: int) -> Dict[str, Any]:
        response = safe_request(
            self.retrieve, err_prefix=f"Dataset ({dataset_id}) is not found."
        )(pk=dataset_id)
        return response.json()

    def update_dataset(
        self,
        dataset_id: int,
        *,
        name: Optional[str] = None,
        vendor: Optional[StorageType] = None,
        region: Optional[str] = None,
        storage_name: Optional[str] = None,
        credential_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        files: Optional[List[Dict[str, Any]]] = None,
        active: Optional[bool] = None,
    ) -> Dict[str, Any]:
        # Valdiate region
        if vendor is not None or region is not None:
            prev_info = self.get_dataset(dataset_id)
            validate_storage_region(
                vendor or storage_type_map_inv[prev_info["vendor"]],
                region or prev_info["region"],
            )

        request_data = {}
        if name is not None:
            request_data["name"] = name
        if vendor is not None:
            request_data["vendor"] = vendor
        if region is not None:
            request_data["region"] = region
        if storage_name is not None:
            request_data["storage_name"] = storage_name
        if credential_id is not None:
            request_data["credential_id"] = credential_id
        if metadata is not None:
            request_data["metadata"] = metadata
        if files is not None:
            request_data["files"] = files
        if active is not None:
            request_data["active"] = active
        response = safe_request(
            self.partial_update, err_prefix="Failed to update dataset."
        )(pk=dataset_id, json=request_data)
        return response.json()

    def delete_dataset(self, dataset_id: int) -> None:
        safe_request(self.delete, err_prefix="Failed to delete dataset")(pk=dataset_id)
