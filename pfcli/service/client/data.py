# Copyright (C) 2022 FriendliAI

"""PeriFlow DataClient Service"""

import math
import os
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION
from pathlib import Path
from typing import Any, Dict, List, Optional

from requests import Request, Session
from tqdm import tqdm

from pfcli.service import StorageType, storage_type_map_inv
from pfcli.service.client.base import ClientService, safe_request
from pfcli.utils.format import secho_error_and_exit
from pfcli.utils.fs import (
    CustomCallbackIOWrapper,
    get_file_size,
    get_total_file_size,
    storage_path_to_local_path,
    upload_file,
)
from pfcli.utils.validate import validate_storage_region

# The actual hard limit of a part size is 5 GiB, and we use 200 MiB part size.
# See https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html.
S3_MPU_PART_MAX_SIZE = 200 * 1024 * 1024  # 200 MiB
S3_UPLOAD_SIZE_LIMIT = 5 * 1024 * 1024 * 1024  # 5 GiB


class DataClientService(ClientService):
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

    def get_spu_urls(self, dataset_id: int, paths: List[str]) -> List[Dict[str, Any]]:
        """Get single part upload URLs for multiple files.

        Args:
            dataset_id (T): Dataset Id
            paths (List[str]): A list of local dataset paths

        Returns:
            List[Dict[str, Any]]: _description_
        """
        response = safe_request(self.post, err_prefix="Failed to get presigned URLs.")(
            path=f"{dataset_id}/upload/", json={"paths": paths}
        )
        return response.json()

    def get_mpu_urls(
        self, dataset_id: int, paths: List[str], src_path: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get multipart upload URLs for multiple datasets

        Args:
            dataset_id (T): The ID of dataset
            paths (List[str]): A list of upload target paths

        Returns:
            List[Dict[str, Any]]: A list of multipart upload responses for multiple target files.
            {
                "path": "string",
                "upload_id": "string",
                "upload_urls": [
                    {
                        "upload_url": "string",
                        "part_number": 0
                    }
                ]
            }
        """
        start_mpu_resps = []
        for path in paths:
            if src_path is not None and not os.path.isfile(path):
                num_parts = math.ceil(
                    get_file_size(os.path.join(src_path, path)) / S3_MPU_PART_MAX_SIZE
                )
            else:
                num_parts = math.ceil(get_file_size(path) / S3_MPU_PART_MAX_SIZE)
            response = safe_request(
                self.post,
                err_prefix="Failed to get presigned URLs for multipart upload.",
            )(
                path=f"{dataset_id}/start_mpu/",
                json={
                    "path": path,
                    "num_parts": num_parts,
                },
            )
            start_mpu_resps.append(response.json())
        return start_mpu_resps

    def complete_mpu(
        self, dataset_id: int, path: str, upload_id: str, parts: List[Dict[str, Any]]
    ) -> None:
        safe_request(
            self.post, err_prefix=f"Failed to complete multipart upload for {path}"
        )(
            path=f"{dataset_id}/complete_mpu/",
            json={
                "path": path,
                "upload_id": upload_id,
                "parts": parts,
            },
        )

    def abort_mpu(self, dataset_id: int, path: str, upload_id: str) -> None:
        safe_request(
            self.post, err_prefix=f"Failed to abort multipart upload for {path}"
        )(
            path=f"{dataset_id}/abort_mpu/",
            json={
                "path": path,
                "upload_id": upload_id,
            },
        )

    def _multipart_upload_file(
        self, dataset_id: int, file_path: str, url_dict: Dict[str, Any], ctx: tqdm
    ) -> None:
        # TODO (ym): parallelize each part upload.
        parts = []
        upload_id = url_dict["upload_id"]
        object_path = url_dict["path"]
        try:
            with open(file_path, "rb") as f:
                fileno = f.fileno()
                total_file_size = os.fstat(fileno).st_size
                for idx, url_info in enumerate(url_dict["upload_urls"]):
                    cursor = idx * S3_MPU_PART_MAX_SIZE
                    f.seek(cursor)
                    # Only the last part can be smaller than ``S3_MPU_PART_MAX_SIZE``.
                    chunk_size = min(S3_MPU_PART_MAX_SIZE, total_file_size - cursor)
                    wrapped_object = CustomCallbackIOWrapper(
                        ctx.update, f, "read", chunk_size
                    )
                    s = Session()
                    req = Request("PUT", url_info["upload_url"], data=wrapped_object)
                    prep = req.prepare()
                    prep.headers["Content-Length"] = str(chunk_size)
                    response = s.send(prep)
                    response.raise_for_status()

                    etag = response.headers["ETag"]
                    parts.append(
                        {
                            "etag": etag,
                            "part_number": url_info["part_number"],
                        }
                    )
                assert not f.read(
                    S3_MPU_PART_MAX_SIZE
                ), "Some parts of your data is not uploaded. Please try again."
            self.complete_mpu(dataset_id, object_path, upload_id, parts)
        except FileNotFoundError:
            secho_error_and_exit(f"{file_path} is not found.")
        except Exception as exc:
            self.abort_mpu(dataset_id, object_path, upload_id)
            secho_error_and_exit(f"File upload is aborted: ({exc!r})")

    def upload_files(
        self,
        dataset_id: int,
        spu_url_dicts: List[Dict[str, str]],
        mpu_url_dicts: List[Dict[str, Any]],
        source_path: Path,
        expand: bool,
    ) -> None:
        spu_local_paths = [
            storage_path_to_local_path(url_info["path"], source_path, expand)
            for url_info in spu_url_dicts
        ]
        mpu_local_paths = [
            storage_path_to_local_path(url_info["path"], source_path, expand)
            for url_info in mpu_url_dicts
        ]
        total_size = get_total_file_size(spu_local_paths + mpu_local_paths)
        spu_urls = [url_info["upload_url"] for url_info in spu_url_dicts]

        with tqdm(total=total_size, unit="B", unit_scale=True, unit_divisor=1024) as t:
            with ThreadPoolExecutor() as executor:
                # Normal upload for files with size < 5 GiB
                futs = [
                    executor.submit(upload_file, local_path, upload_url, t)
                    for (local_path, upload_url) in zip(spu_local_paths, spu_urls)
                ]
                # Multipart upload for large files with sizes >= 5 GiB
                futs.extend(
                    [
                        executor.submit(
                            self._multipart_upload_file,
                            dataset_id,
                            local_path,
                            url_dict,
                            t,
                        )
                        for (local_path, url_dict) in zip(
                            mpu_local_paths, mpu_url_dicts
                        )
                    ]
                )
                wait(futs, return_when=FIRST_EXCEPTION)
                for fut in futs:
                    exc = fut.exception()
                    if exc is not None:
                        raise exc
