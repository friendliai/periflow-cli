# Copyright (c) 2022-present, FriendliAI Inc. All rights reserved.

"""PeriFlow CLI File System Management Utilities"""

from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION
from contextlib import contextmanager
from datetime import datetime
from dateutil.tz import tzlocal
from enum import Enum
from functools import wraps
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import zipfile

import pathspec
import requests
from requests import Request, Session
from tqdm import tqdm
from tqdm.utils import CallbackIOWrapper
import typer

from pfcli.utils.format import secho_error_and_exit

# The actual hard limit of a part size is 5 GiB, and we use 200 MiB part size.
# See https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html.
S3_MPU_PART_MAX_SIZE = 200 * 1024 * 1024  # 200 MiB
S3_UPLOAD_SIZE_LIMIT = 5 * 1024 * 1024 * 1024  # 5 GiB

periflow_directory = Path.home() / ".periflow"


def get_periflow_directory() -> Path:
    periflow_directory.mkdir(exist_ok=True)
    return periflow_directory


@contextmanager
def zip_dir(base_path: Path, target_files: List[Path], zip_path: Path):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as zip_file:
        for file in target_files:
            zip_file.write(file, file.relative_to(base_path.parent))
    typer.secho("Uploading workspace directory...", fg=typer.colors.MAGENTA)
    try:
        yield zip_path.open("rb")
    finally:
        zip_path.unlink()


def get_workspace_files(dir_path: Path) -> List[Path]:
    ignore_file = dir_path / ".pfignore"
    all_files = set(x for x in dir_path.rglob("*") if x.is_file() and x != ignore_file)

    if not ignore_file.exists():
        return list(all_files)

    with open(ignore_file, "r", encoding="utf-8") as f:
        ignore_patterns = f.read()
    spec = pathspec.PathSpec.from_lines(
        pathspec.patterns.GitWildMatchPattern, ignore_patterns.splitlines()
    )
    matched_files = set(dir_path / x for x in spec.match_tree_files(dir_path))  # type: ignore
    return list(all_files.difference(matched_files))


def storage_path_to_local_path(
    storage_path: str, source_path: Path, expand: bool
) -> str:
    if source_path.is_file():
        return str(source_path)

    if expand:
        return str(source_path / Path(storage_path))

    return str(source_path / Path(storage_path.split("/", 1)[1]))


def get_file_info(storage_path: str, source_path: Path, expand: bool) -> Dict[str, Any]:
    loacl_path = storage_path_to_local_path(storage_path, source_path, expand)
    return {
        "name": os.path.basename(storage_path),
        "path": storage_path,
        "mtime": datetime.fromtimestamp(
            os.stat(loacl_path).st_mtime, tz=tzlocal()
        ).isoformat(),
        "size": os.stat(loacl_path).st_size,
    }


def get_content_size(url: str) -> int:
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        secho_error_and_exit("Failed to download (invalid url)")
    return int(response.headers["Content-Length"])


def download_range(url: str, start: int, end: int, output: str, ctx: tqdm) -> None:
    headers = {"Range": f"bytes={start}-{end}"}
    response = requests.get(url, headers=headers, stream=True)

    with open(output, "wb") as f:
        wrapped_object = CallbackIOWrapper(ctx.update, f, "write")
        for part in response.iter_content(1024):
            wrapped_object.write(part)


def download_file_simple(url: str, out: str, content_length: int) -> None:
    response = requests.get(url, stream=True)
    with tqdm.wrapattr(
        open(out, "wb"), "write", miniters=1, total=content_length
    ) as fout:
        for chunk in response.iter_content(chunk_size=4096):
            fout.write(chunk)


def download_file_parallel(
    url: str, out: str, content_length: int, chunk_size: int = 1024 * 1024 * 4
) -> None:
    chunks = range(0, content_length, chunk_size)

    temp_out_prefix = os.path.join(os.path.dirname(out), f".{os.path.basename(out)}")

    try:
        with tqdm(
            total=content_length, unit="B", unit_scale=True, unit_divisor=1024
        ) as t:
            with ThreadPoolExecutor() as executor:
                futs = [
                    executor.submit(
                        download_range,
                        url,
                        start,
                        start + chunk_size - 1,
                        f"{temp_out_prefix}.part{i}",
                        t,
                    )
                    for i, start in enumerate(chunks)
                ]
                wait(futs, return_when=FIRST_EXCEPTION)

        # Merge partitioned files
        with open(out, "wb") as f:
            for i in range(len(chunks)):
                chunk_path = f"{temp_out_prefix}.part{i}"
                with open(chunk_path, "rb") as chunk_f:
                    f.write(chunk_f.read())

                os.remove(chunk_path)
    finally:
        # Clean up zombie temporary partitioned files
        for i in range(len(chunks)):
            chunk_path = f"{temp_out_prefix}.part{i}"
            if os.path.isfile(chunk_path):
                os.remove(chunk_path)


def download_file(url: str, out: str) -> None:
    file_size = get_content_size(url)

    # Create directory if not exists
    dirpath = os.path.dirname(out)
    os.makedirs(dirpath, exist_ok=True)

    if file_size >= 16 * 1024 * 1024:
        download_file_parallel(url, out, file_size)
    else:
        download_file_simple(url, out, file_size)


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


def upload_file(file_path: str, url: str, ctx: tqdm) -> None:
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
            with Session() as s:
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


class CustomCallbackIOWrapper(CallbackIOWrapper):
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


def upload_part(
    file_path: str,
    chunk_index: int,
    part_number: int,
    upload_url: str,
    ctx: tqdm,
    is_last_part: bool,
) -> Dict[str, Any]:
    with open(file_path, "rb") as f:
        fileno = f.fileno()
        total_file_size = os.fstat(fileno).st_size
        cursor = chunk_index * S3_MPU_PART_MAX_SIZE
        f.seek(cursor)
        chunk_size = min(S3_MPU_PART_MAX_SIZE, total_file_size - cursor)
        wrapped_object = CustomCallbackIOWrapper(ctx.update, f, "read", chunk_size)
        with Session() as s:
            req = Request("PUT", upload_url, data=wrapped_object)
            prep = req.prepare()
            prep.headers["Content-Length"] = str(chunk_size)
            response = s.send(prep)
        response.raise_for_status()

        if is_last_part:
            assert not f.read(
                S3_MPU_PART_MAX_SIZE
            ), "Some parts of your data is not uploaded. Please try again."

    etag = response.headers["ETag"]
    return {
        "etag": etag,
        "part_number": part_number,
    }
