# Copyright (c) 2022-present, FriendliAI Inc. All rights reserved.

"""PeriFlow CLI File System Management Utilities"""

from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION
from contextlib import contextmanager
from datetime import datetime
from dateutil.tz import tzlocal
import os
from pathlib import Path
from typing import List
import zipfile

import pathspec
import requests
from tqdm import tqdm
from tqdm.utils import CallbackIOWrapper
import typer

from pfcli.utils.format import secho_error_and_exit

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


def get_file_info(storage_path: str, source_path: Path, expand: bool) -> dict:
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
