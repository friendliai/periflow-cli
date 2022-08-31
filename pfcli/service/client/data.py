# Copyright (C) 2022 FriendliAI

"""PeriFlow DataClient Service"""

import math
import os
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION
from functools import wraps
from pathlib import Path
from typing import Dict, List, Optional

import requests
from requests import Request, Session
from tqdm import tqdm
from tqdm.utils import CallbackIOWrapper

from pfcli.service import StorageType, storage_type_map_inv
from pfcli.service.client.base import ClientService, T, safe_request
from pfcli.utils import secho_error_and_exit, storage_path_to_local_path, validate_storage_region

# The actual hard limit of a part size is 5 GiB, and we use 200 MiB part size.
# See https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html.
S3_MPU_PART_MAX_SIZE = 200 * 1024 * 1024        # 200 MiB
S3_UPLOAD_SIZE_LIMIT = 5 * 1024 * 1024 * 1024   # 5 GiB


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

    def get_spu_urls(self, datastore_id: T, paths: List[str]) -> List[dict]:
        """Get single part upload URLs for multiple files.

        Args:
            datastore_id (T): _description_
            paths (List[str]): _description_

        Returns:
            List[dict]: _description_
        """
        response = safe_request(self.post, err_prefix="Failed to get presigned URLs.")(
            path=f"{datastore_id}/upload/",
            json={"paths": paths}
        )
        return response.json()

    def get_mpu_urls(self, datastore_id: T, paths: List[str]) -> List[dict]:
        """Get multipart upload URLs for multiple datasets

        Args:
            datastore_id (T): The ID of dataset
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
            num_parts = math.ceil(get_file_size(path) / S3_MPU_PART_MAX_SIZE)
            response = safe_request(self.post, err_prefix="Failed to get presigned URLs for multipart upload.")(
                path=f"{datastore_id}/start_mpu/",
                json={
                    "path": path,
                    "num_parts": num_parts,
                }
            )
            start_mpu_resps.append(response.json())
        return start_mpu_resps

    def complete_mpu(self, datastore_id: T, path: str, upload_id: str, parts: List[dict]) -> None:
        safe_request(self.post, err_prefix=f"Failed to complete multipart upload for {path}")(
            path=f"{datastore_id}/complete_mpu/",
            json={
                "path": path,
                "upload_id": upload_id,
                "parts": parts,
            }
        )

    def abort_mpu(self, datastore_id: T, path: str, upload_id: str) -> None:
        safe_request(self.post, err_prefix=f"Failed to abort multipart upload for {path}")(
            path=f"{datastore_id}/abort_mpu/",
            json={
                "path": path,
                "upload_id": upload_id,
            }
        )

    def _multipart_upload_file(
        self,
        datastore_id: T,
        file_path: str,
        urls: List[str],
        upload_id: str,
        ctx: tqdm
    ) -> None:
        parts = []
        try:
            with open(file_path, 'rb') as f:
                fileno = f.fileno()
                total_file_size = os.fstat(fileno).st_size
                for idx, url_info in enumerate(urls):
                    cursor = idx * S3_MPU_PART_MAX_SIZE
                    f.seek(cursor)
                    chunk_size = S3_MPU_PART_MAX_SIZE if idx < len(urls) - 1 else total_file_size - cursor
                    wrapped_object = _CustomCallbackIOWrapper(ctx.update, f, 'read', chunk_size)
                    s = Session()
                    req = Request('PUT', url_info['upload_url'], data=wrapped_object)
                    prep = req.prepare()
                    prep.headers['Content-Length'] = chunk_size
                    response = s.send(prep)

                    etag = response.headers['ETag']
                    parts.append(
                        {
                            'etag': etag,
                            'part_number': url_info['part_number'],
                        }
                    )
                assert not f.read(S3_MPU_PART_MAX_SIZE)
            self.complete_mpu(datastore_id, file_path, upload_id, parts)
        except FileNotFoundError:
            secho_error_and_exit(f"{file_path} is not found.")
        except Exception:
            self.abort_mpu(datastore_id, file_path, upload_id)
            secho_error_and_exit("File upload is aborted.")

    def upload_files(
        self,
        datastore_id: T,
        spu_url_dicts: List[Dict[str, str]],
        mpu_url_dicts: List[dict],
        source_path: Path,
        expand: bool,
    ) -> None:
        spu_local_paths = [
            storage_path_to_local_path(url_info['path'], source_path, expand) for url_info in spu_url_dicts
        ]
        mpu_local_paths = [
            storage_path_to_local_path(url_info['path'], source_path, expand) for url_info in mpu_url_dicts
        ]
        total_size = get_total_file_size(spu_local_paths + mpu_local_paths)
        spu_urls = [ url_info['upload_url'] for url_info in spu_url_dicts ]
        mpu_urls = [ url_info['upload_urls'] for url_info in mpu_url_dicts ]
        mpu_upload_ids = [ url_info['upload_id'] for url_info in mpu_url_dicts ]

        with tqdm(total=total_size, unit='B', unit_scale=True, unit_divisor=1024) as t:
            with ThreadPoolExecutor() as executor:
                # Normal upload for files with size < 5 GiB
                futs = [
                    executor.submit(
                        _upload_file, local_path, upload_url, t
                    ) for (local_path, upload_url) in zip(spu_local_paths, spu_urls)
                ]
                # Multipart upload for large files with sizes >= 5 GiB
                futs.extend([
                    executor.submit(
                        self._multipart_upload_file, datastore_id, local_path, upload_urls, upload_id, t
                    ) for (local_path, upload_urls, upload_id) in zip(mpu_local_paths, mpu_urls, mpu_upload_ids)
                ])
                wait(futs, return_when=FIRST_EXCEPTION)
                for fut in futs:
                    exc = fut.exception()
                    if exc is not None:
                        raise exc


def expand_paths(path: Path, expand: bool) -> List[str]:
    if path.is_file():
        paths = [str(path.name)]
    else:
        paths = list(path.rglob('*'))
        rel_path = path if expand else path.parent
        paths = [str(f.relative_to(rel_path)) for f in paths if f.is_file()]

    return paths


def get_spu_targets(src_path: Path, expand: bool) -> List[str]:
    return [ path for path in expand_paths(src_path, expand) if get_file_size(path) < S3_UPLOAD_SIZE_LIMIT ]


def get_mpu_targets(src_path: Path, expand: bool) -> List[str]:
    return [ path for path in expand_paths(src_path, expand) if get_file_size(path) >= S3_UPLOAD_SIZE_LIMIT ]


def _upload_file(file_path: str, url: str, ctx: tqdm) -> None:
    try:
        with open(file_path, 'rb') as f:
            fileno = f.fileno()
            total_file_size = os.fstat(fileno).st_size
            wrapped_object = CallbackIOWrapper(ctx.update, f, 'read')
            s = Session()
            req = Request('PUT', url, data=wrapped_object)
            prep = req.prepare()
            prep.headers['Content-Length'] = total_file_size    # necessary to use ``CallbackIOWrapper``
            s.send(prep)
    except FileNotFoundError:
        secho_error_and_exit(f"{file_path} is not found.")


def get_file_size(file_path: str) -> int:
    return os.stat(file_path).st_size


def get_total_file_size(file_paths: List[str]) -> int:
    return sum([ get_file_size(file_path) for file_path in file_paths ])


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
            self.wrapper_setattr('write', write)
        elif method == "read":
            @wraps(func)
            def read(*args, **kwargs):
                if self._cursor >= chunk_size:
                    self._cursor = 0
                    return

                data = func(*args, **kwargs)
                data_size = len(data)
                callback(data_size)
                self._cursor += data_size
                return data
            self.wrapper_setattr('read', read)
        else:
            raise KeyError("Can only wrap read/write methods")
