# Copyright (C) 2022 FriendliAI

"""PeriFlow DataClient Service"""

import math
import os
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import Dict, List, Optional

import typer
from requests import Request, Session
from tqdm import tqdm
from tqdm.utils import CallbackIOWrapper

from pfcli.service import StorageType, storage_type_map_inv
from pfcli.service.client.base import ClientService, safe_request
from pfcli.utils.format import secho_error_and_exit
from pfcli.utils.fs import storage_path_to_local_path
from pfcli.utils.validate import validate_storage_region

# The actual hard limit of a part size is 5 GiB, and we use 200 MiB part size.
# See https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html.
S3_MPU_PART_MAX_SIZE = 200 * 1024 * 1024  # 200 MiB
S3_UPLOAD_SIZE_LIMIT = 5 * 1024 * 1024 * 1024  # 5 GiB


class DataClientService(ClientService):
    def get_dataset(self, dataset_id: int) -> dict:
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
        metadata: Optional[dict] = None,
        files: Optional[List[dict]] = None,
        active: Optional[bool] = None,
    ) -> dict:
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

    def get_spu_urls(self, dataset_id: int, paths: List[str]) -> List[dict]:
        """Get single part upload URLs for multiple files.

        Args:
            dataset_id (T): _description_
            paths (List[str]): _description_

        Returns:
            List[dict]: _description_
        """
        response = safe_request(self.post, err_prefix="Failed to get presigned URLs.")(
            path=f"{dataset_id}/upload/", json={"paths": paths}
        )
        return response.json()

    def get_mpu_urls(
        self, dataset_id: int, paths: List[str], src_path: Optional[str] = None
    ) -> List[dict]:
        """Get multipart upload URLs for multiple datasets

        Args:
            dataset_id (T): The ID of dataset
            paths (List[str]): A list of upload target paths

        Returns:
            List[dict]: A list of multipart upload responses for multiple target files.
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
        self, dataset_id: int, path: str, upload_id: str, parts: List[dict]
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
        self, dataset_id: int, file_path: str, url_dict: dict, ctx: tqdm
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
                    wrapped_object = _CustomCallbackIOWrapper(
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
        mpu_url_dicts: List[dict],
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
                    executor.submit(_upload_file, local_path, upload_url, t)
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


class FileSizeType(Enum):
    LARGE = "LARGE"
    SMALL = "SMALL"


def filter_files_by_size(paths: List[str], size_type: FileSizeType) -> List[str]:
    if size_type is FileSizeType.LARGE:
        return [path for path in paths if get_file_size(path) >= S3_UPLOAD_SIZE_LIMIT]
    if size_type is FileSizeType.SMALL:
        res = []
        for path in paths:
            size = get_file_size(path)
            if size == 0:
                # NOTE: S3 does not support file uploading for 0B size files.
                typer.secho(
                    f"Skip uploading file ({path}) with size 0B.", fg=typer.colors.RED
                )
                continue
            if size < S3_UPLOAD_SIZE_LIMIT:
                res.append(path)
        return res


def expand_paths(path: Path, expand: bool, size_type: FileSizeType) -> List[str]:
    if path.is_file():
        paths = [str(path.name)]
        paths = filter_files_by_size(paths, size_type)
    else:
        paths = [str(p) for p in path.rglob("*")]
        paths = filter_files_by_size(paths, size_type)
        rel_path = path if expand else path.parent
        paths = [str(Path(p).relative_to(rel_path)) for p in paths if Path(p).is_file()]

    return paths


def _upload_file(file_path: str, url: str, ctx: tqdm) -> None:
    try:
        with open(file_path, "rb") as f:
            fileno = f.fileno()
            total_file_size = os.fstat(fileno).st_size
            if total_file_size == 0:
                typer.secho(
                    f"The file with 0B size ({file_path}) is skipped.",
                    fg=typer.colors.RED,
                )
                return

            wrapped_object = CallbackIOWrapper(ctx.update, f, "read")
            s = Session()
            req = Request("PUT", url, data=wrapped_object)
            prep = req.prepare()
            prep.headers["Content-Length"] = str(
                total_file_size
            )  # necessary to use ``CallbackIOWrapper``
            response = s.send(prep)
            if response.status_code != 200:
                secho_error_and_exit(
                    f"Failed to upload file ({file_path}): {response.content}"
                )
    except FileNotFoundError:
        secho_error_and_exit(f"{file_path} is not found.")


def get_file_size(file_path: str, prefix: Optional[str] = None) -> int:
    """Calculate a file size in bytes.

    Args:
        file_path (str): Path to the target file.
        prefix (Optional[str], optional): If it is not None, attach the prefix to the path. Defaults to None.

    Returns:
        int: The size of a file.
    """
    if prefix is not None:
        file_path = os.path.join(prefix, file_path)

    return os.stat(file_path).st_size


def get_total_file_size(file_paths: List[str], prefix: Optional[str] = None) -> int:
    return sum([get_file_size(file_path, prefix) for file_path in file_paths])


class _CustomCallbackIOWrapper(CallbackIOWrapper):
    def __init__(self, callback, stream, method="read", chunk_size=None):
        """
        Wrap a given `file`-like object's `read()` or `write()` to report
        lengths to the given `callback`
        """
        super().__init__(callback, stream, method)
        self._chunk_size = chunk_size
        self._cursor = 0

        func = getattr(stream, method)
        if method == "write":

            @wraps(func)
            def write(data, *args, **kwargs):
                res = func(data, *args, **kwargs)
                callback(len(data))
                return res

            self.wrapper_setattr("write", write)
        elif method == "read":

            @wraps(func)
            def read(*args, **kwargs):
                assert chunk_size is not None
                if self._cursor >= chunk_size:
                    self._cursor = 0
                    return

                data = func(*args, **kwargs)
                data_size = len(data)  # default to 8 KiB
                callback(data_size)
                self._cursor += data_size
                return data

            self.wrapper_setattr("read", read)
        else:
            raise KeyError("Can only wrap read/write methods")
