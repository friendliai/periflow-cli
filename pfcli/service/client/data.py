# Copyright (C) 2022 FriendliAI

"""PeriFlow DataClient Service"""


from pathlib import Path
from typing import Dict, List, Optional

from pfcli.service import StorageType, storage_type_map_inv
from pfcli.service.client.base import ClientService, T, safe_request
from pfcli.utils import secho_error_and_exit, validate_storage_region


class DataClientService(ClientService):
    def get_datastore(self, datastore_id: T) -> dict:
        response = safe_request(self.retrieve, err_prefix=f"Datastore ({datastore_id}) is not found.")(
            pk=datastore_id
        )
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
        response = safe_request(self.partial_update, err_prefix="Failed to update datastore.")(
            pk=datastore_id,
            json=request_data
        )
        return response.json()

    def delete_datastore(self, datastore_id: T) -> None:
        safe_request(self.delete, err_prefix="Failed to delete datastore")(
            pk=datastore_id
        )

    def get_upload_urls(self, datastore_id: T, src_path: Path, expand: bool) -> List[Dict[str, str]]:
        if src_path.is_file():
            paths = [str(src_path.name)]
        else:
            paths = list(src_path.rglob('*'))
            rel_path = src_path if expand else src_path.parent
            paths = [str(f.relative_to(rel_path)) for f in paths if f.is_file()]
        if len(paths) == 0:
            secho_error_and_exit(f"No file exists in this path ({src_path})")

        response = safe_request(self.post, err_prefix="Failed to get presigned URLs.")(
            path=f"{datastore_id}/upload/",
            json={"paths": paths}
        )
        return response.json()
