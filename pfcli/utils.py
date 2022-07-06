# Copyright (C) 2021 FriendliAI

"""PeriFlow CLI Shared Utilities"""

import os
import zipfile
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from dateutil.tz import tzlocal
from pathlib import Path
from subprocess import CalledProcessError, check_call
from typing import Callable, Optional, List, Dict
from urllib.parse import urljoin

import pathspec
import typer
import requests
from requests.exceptions import HTTPError
from requests.models import Response
from tqdm import tqdm
from tqdm.utils import CallbackIOWrapper

from pfcli.service import (
    CloudType,
    StorageType,
    storage_region_map,
    cloud_region_map
)

# Variables
periflow_directory = Path.home() / ".periflow"
periflow_api_server = "https://api-staging.friendli.ai/api/"
periflow_ws_server = "wss://api-ws-staging.friendli.ai/ws/"
periflow_discuss_url = "https://discuss-staging.friendli.ai/"
periflow_mr_server = "https://pfmodelregistry-staging.friendli.ai/"
periflow_serve_server = "http://0.0.0.0:8000/"
periflow_auth_server = "https://pfauth-staging.friendli.ai/"
periflow_meter_server = "https://pfmeter-staging.friendli.ai/"


def get_periflow_directory() -> Path:
    periflow_directory.mkdir(exist_ok=True)
    return periflow_directory


def datetime_to_pretty_str(past: Optional[datetime], long_list: bool = False):
    cur = datetime.now().astimezone()
    delta = cur - past
    if long_list:
        if delta < timedelta(minutes=1):
            return f'{delta.seconds % 60}s ago'
        if delta < timedelta(hours=1):
            return f'{round((delta.seconds % 3600) / 60)}m {delta.seconds % 60}s ago'
        elif delta < timedelta(days=1):
            return f'{delta.seconds // 3600}h {round((delta.seconds % 3600) / 60)}m {delta.seconds % 60}s ago'
        elif delta < timedelta(days=3):
            return f'{delta.days}d {delta.seconds // 3600}h ' \
                   f'{round((delta.seconds % 3600) / 60)}m ago'
        else:
            return past.astimezone(tz=cur.tzinfo).strftime("%Y-%m-%d %H:%M:%S")
    else:
        if delta < timedelta(hours=1):
            return f'{round((delta.seconds % 3600) / 60)} mins ago'
        elif delta < timedelta(days=1):
            return f'{round(delta.seconds / 3600)} hours ago'
        else:
            return f'{delta.days + round(delta.seconds / (3600 * 24))} days ago'


def timedelta_to_pretty_str(start: datetime, finish: datetime, long_list: bool = False):
    delta = finish - start
    if long_list:
        if delta < timedelta(minutes=1):
            return f'{(delta.seconds % 60)}s'
        if delta < timedelta(hours=1):
            return f'{(delta.seconds % 3600) // 60}m {(delta.seconds % 60)}s'
        elif delta < timedelta(days=1):
            return f'{delta.seconds // 3600}h {(delta.seconds % 3600) // 60}m {(delta.seconds % 60)}s'
        else:
            return f'{delta.days}d {delta.seconds // 3600}h ' \
                   f'{(delta.seconds % 3600) // 60}m {delta.seconds % 60}s'
    else:
        if delta < timedelta(hours=1):
            return f'{round((delta.seconds % 3600) / 60)} mins'
        elif delta < timedelta(days=1):
            return f'{round(delta.seconds / 3600)} hours'
        else:
            return f'{delta.days + round(delta.seconds / (3600 * 24))} days'


def get_auth_uri(path: str) -> str:
    return urljoin(periflow_auth_server, path)


def get_uri(path: str) -> str:
    return urljoin(periflow_api_server, path)


def get_wss_uri(path: str) -> str:
    return urljoin(periflow_ws_server, path)


def get_pfs_uri(path: str) -> str:
    return urljoin(periflow_serve_server, path)


def get_mr_uri(path: str) -> str:
    return urljoin(periflow_mr_server, path)


def get_meter_uri(path: str) -> str:
    return urljoin(periflow_meter_server, path)


def secho_error_and_exit(text: str, color: str = typer.colors.RED):
    typer.secho(text, err=True, fg=color)
    raise typer.Exit(1)


def get_remaining_terminal_columns(occupied: int) -> int:
    return os.get_terminal_size().columns - occupied


def utc_to_local(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc).astimezone(tz=None)


def datetime_to_simple_string(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


@contextmanager
def zip_dir(base_path: Path, target_files: List[Path], zip_path: Path):
    typer.secho("Preparing workspace directory...", fg=typer.colors.MAGENTA)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as zip_file:
        for file in target_files:
            zip_file.write(file, file.relative_to(base_path.parent))
    typer.secho("Uploading workspace directory...", fg=typer.colors.MAGENTA)
    try:
        yield zip_path.open("rb")
    finally:
        zip_path.unlink()


def open_editor(path: str, editor: Optional[str] = None):
    default_editor = editor or get_default_editor()
    try:
        check_call([default_editor, path])
    except CalledProcessError:
        typer.secho("", fg=typer.colors.RED)


def get_default_editor() -> str:
    return os.environ.get("PERIFLOW_CLI_EDITOR", "vim")


def validate_storage_region(vendor: StorageType, region: str):
    available_regions = storage_region_map[vendor]
    if region not in available_regions:
        secho_error_and_exit(
            f"'{region}' is not supported region for {vendor}. Please choose another one in {available_regions}."
        )


def validate_cloud_region(vendor: CloudType, region: str):
    available_regions = cloud_region_map[vendor]
    if region not in available_regions:
        secho_error_and_exit(
            f"'{region}' is not supported region for {vendor}. Please choose another one in {available_regions}."
        )


def get_workspace_files(dir_path: Path) -> List[Path]:
    ignore_file = dir_path / ".pfignore"
    all_files = set(x for x in dir_path.rglob("*") if x.is_file() and x != ignore_file)

    if not ignore_file.exists():
        return list(all_files)

    with open(ignore_file, "r", encoding="utf-8") as f:
        ignore_patterns = f.read()
    spec = pathspec.PathSpec.from_lines(pathspec.patterns.GitWildMatchPattern, ignore_patterns.splitlines())
    matched_files = set(dir_path / x for x in spec.match_tree_files(dir_path))
    return list(all_files.difference(matched_files))


def _upload_file(file_path: str, url: str, ctx: tqdm):
    try:
        with open(file_path, 'rb') as f:
            wrapped_object = CallbackIOWrapper(ctx.update, f, 'read')
            requests.put(url, data=wrapped_object)
    except FileNotFoundError:
        secho_error_and_exit(f"{file_path} is not found.")


def _get_total_file_size(file_paths: List[str]) -> int:
    return sum([os.stat(file_path).st_size for file_path in file_paths])


def storage_path_to_local_path(storage_path: str, source_path: Path, expand: bool) -> str:
    if source_path.is_file():
        return str(source_path)

    if expand:
        return str(source_path / Path(storage_path))

    return str(source_path / Path(storage_path.split('/', 1)[1]))


def upload_files(url_dicts: List[Dict[str, str]], source_path: Path, expand: bool) -> None:
    local_paths = [ storage_path_to_local_path(url_info['path'], source_path, expand) for url_info in url_dicts ]
    total_size = _get_total_file_size(local_paths)
    upload_urls = [ url_info['upload_url'] for url_info in url_dicts ]

    with tqdm(total=total_size, unit='B', unit_scale=True, unit_divisor=1024) as t:
        with ThreadPoolExecutor() as executor:
            futs = [
                executor.submit(
                    _upload_file, local_path, upload_url, t
                ) for (local_path, upload_url) in zip(local_paths, upload_urls)
            ]
            wait(futs, return_when=FIRST_EXCEPTION)


def get_file_info(storage_path: str, source_path: Path, expand: bool) -> dict:
    loacl_path = storage_path_to_local_path(storage_path, source_path, expand)
    return {
        'name': os.path.basename(storage_path),
        'path': storage_path,
        'mtime': datetime.fromtimestamp(os.stat(loacl_path).st_mtime, tz=tzlocal()).isoformat(),
        'size': os.stat(loacl_path).st_size,
    }


def get_content_size(url: str) -> int:
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        secho_error_and_exit("Failed to download (invalid url)")
    return int(response.headers['Content-Length'])


def download_range(url: str, start: int, end: int, output: str, ctx: tqdm) -> None:
    headers = {'Range': f'bytes={start}-{end}'}
    response = requests.get(url, headers=headers, stream=True)

    with open(output, 'wb') as f:
        wrapped_object = CallbackIOWrapper(ctx.update, f, 'write')
        for part in response.iter_content(1024):
            wrapped_object.write(part)


def download_file_simple(url: str, out: str, content_length: int) -> None:
    response = requests.get(url, stream=True)
    with tqdm.wrapattr(open(out, "wb"), "write", miniters=1, total=content_length) as fout:
        for chunk in response.iter_content(chunk_size=4096):
            fout.write(chunk)


def download_file_parallel(url: str, out: str, content_length: int, chunk_size: int = 1024 * 1024 * 4) -> None:
    chunks = range(0, content_length, chunk_size)

    temp_out_prefix = os.path.join(os.path.dirname(out), f'.{os.path.basename(out)}')

    try:
        with tqdm(total=content_length, unit='B', unit_scale=True, unit_divisor=1024) as t:
            with ThreadPoolExecutor() as executor:
                futs = [
                    executor.submit(
                        download_range, url, start, start + chunk_size - 1, f'{temp_out_prefix}.part{i}', t
                    ) for i, start in enumerate(chunks)
                ]
                wait(futs, return_when=FIRST_EXCEPTION)

        # Merge partitioned files
        with open(out, 'wb') as f:
            for i in range(len(chunks)):
                chunk_path = f'{temp_out_prefix}.part{i}'
                with open(chunk_path, 'rb') as chunk_f:
                    f.write(chunk_f.read())

                os.remove(chunk_path)
    finally:
        # Clean up zombie temporary partitioned files
        for i in range(len(chunks)):
            chunk_path = f'{temp_out_prefix}.part{i}'
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


def decode_http_err(exc: HTTPError) -> str:
    try:
        if exc.response.status_code == 500:
            error_str = f"Internal Server Error: Please contact to system admin via {periflow_discuss_url}"
        elif exc.response.status_code == 404:
            error_str = "Not Found: The requested resource is not found. Please check it again. " \
                        f"If you cannot find out why this error occurs, please visit {periflow_discuss_url}."
        else:
            response = exc.response
            detail_json = response.json()
            if 'detail' in detail_json:
                error_str = f"Error Code: {response.status_code}\nDetail: {detail_json['detail']}"
            elif 'error_description' in detail_json:
                error_str = f"Error Code: {response.status_code}\nDetail: {detail_json['error_description']}"
            else:
                error_str = f"Error Code: {response.status_code}"
    except ValueError:
        error_str = exc.response.content.decode()

    return error_str


def paginated_get(response_getter: Callable[..., Response], path: Optional[str] = None, **params) -> List[dict]:
    """Pagination listing
    """
    response_dict = response_getter(path=path, params={**params}).json()
    items = response_dict["results"]
    next_cursor = response_dict["next_cursor"]

    while next_cursor is not None:
        response_dict = response_getter(
            path=path,
            params={
                **params,
                "cursor": next_cursor
            }
        ).json()
        items.extend(response_dict["results"])
        next_cursor = response_dict["next_cursor"]

    return items
