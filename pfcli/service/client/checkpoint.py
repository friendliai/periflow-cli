# Copyright (C) 2022 FriendliAI

"""PeriFlow CheckpointClient Service"""

from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION
import math
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import UUID

import requests
from requests import Request, Session
from requests.models import Response
from tqdm import tqdm

from pfcli.service import StorageType
from pfcli.service.auth import auto_token_refresh, get_auth_header
from pfcli.service.client.base import ClientService, safe_request
from pfcli.utils.format import secho_error_and_exit
from pfcli.utils.fs import (
    CustomCallbackIOWrapper,
    get_file_size,
    get_total_file_size,
    storage_path_to_local_path,
    S3_MPU_PART_MAX_SIZE,
    upload_file,
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

    def get_checkpoint_download_urls(self, checkpoint_id: UUID) -> List[Dict[str, Any]]:
        response = safe_request(
            self.download, err_prefix="Failed to get download URLs of checkpoint files."
        )(checkpoint_id=checkpoint_id)
        return response.json()["files"]


class CheckpointFormClientService(ClientService[UUID]):
    def update_checkpoint_files(
        self, ckpt_form_id: UUID, files: List[Dict[str, Any]],
    ) -> Dict[str, Any]:

        response = safe_request(
            self.partial_update, err_prefix="Cannot update checkpoint."
        )(pk=ckpt_form_id, json={"files": files})
        return response.json()

    def get_checkpoint_download_urls(self, ckpt_form_id: UUID) -> List[Dict[str, Any]]:
        response = safe_request(self.retrieve, err_prefix="Failed to get presigned URLs.")(
            pk=ckpt_form_id,
            path="download/"
        )
        return response.json()["files"]

    def get_spu_urls(self, ckpt_form_id: UUID, paths: List[str]) -> List[Dict[str, Any]]:
        """Get single part upload URLs for multiple files.

        Args:
            ckpt_form_id (UUID): Checkpoint Id
            paths (List[str]): A list of local dataset paths

        Returns:
            List[Dict[str, Any]]: _description_
        """
        response = safe_request(self.post, err_prefix="Failed to get presigned URLs.")(
            path=f"{ckpt_form_id}/upload/", json={"paths": paths}
        )
        return response.json()

    def get_mpu_urls(
        self, ckpt_form_id: UUID, paths: List[str], src_path: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get multipart upload URLs for multiple datasets

        Args:
            ckpt_form_id (UUID): Checkpoint ID
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
                path=f"{ckpt_form_id}/start_mpu/",
                json={
                    "path": path,
                    "num_parts": num_parts,
                },
            )
            start_mpu_resps.append(response.json())
        return start_mpu_resps

    def complete_mpu(
        self, ckpt_form_id: UUID, path: str, upload_id: str, parts: List[Dict[str, Any]]
    ) -> None:
        safe_request(
            self.post, err_prefix=f"Failed to complete multipart upload for {path}"
        )(
            path=f"{ckpt_form_id}/complete_mpu/",
            json={
                "path": path,
                "upload_id": upload_id,
                "parts": parts,
            },
        )

    def abort_mpu(self, ckpt_form_id: UUID, path: str, upload_id: str) -> None:
        safe_request(
            self.post, err_prefix=f"Failed to abort multipart upload for {path}"
        )(
            path=f"{ckpt_form_id}/abort_mpu/",
            json={
                "path": path,
                "upload_id": upload_id,
            },
        )

    def _multipart_upload_file(
        self, ckpt_form_id: UUID, file_path: str, url_dict: Dict[str, Any], ctx: tqdm
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
                assert not f.read(S3_MPU_PART_MAX_SIZE)
            self.complete_mpu(ckpt_form_id, object_path, upload_id, parts)
        except FileNotFoundError:
            secho_error_and_exit(f"{file_path} is not found.")
        except Exception as exc:
            self.abort_mpu(ckpt_form_id, object_path, upload_id)
            secho_error_and_exit(f"File upload is aborted: ({exc!r})")

    def upload_files(
        self,
        ckpt_form_id: UUID,
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
                            ckpt_form_id,
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
