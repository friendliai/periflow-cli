# Copyright (C) 2022 FriendliAI

"""PeriFlow Client Service"""

import copy
import typer
import requests
from dataclasses import dataclass
from functools import wraps
from requests.models import Response
from string import Template
from typing import Callable, Generic, Optional, TypeVar, Union
from urllib.parse import urljoin, urlparse

import uuid

from pfcli.context import get_current_group_id, get_current_project_id
from pfcli.service.auth import auto_token_refresh, get_auth_header
from pfcli.utils.format import secho_error_and_exit
from pfcli.utils.request import decode_http_err
from pfcli.utils.url import get_auth_uri


T = TypeVar("T", bound=Union[int, str, uuid.UUID])


def safe_request(
    func: Callable[..., Response], *, err_prefix: str = ""
) -> Callable[..., Response]:
    if err_prefix:
        err_prefix = err_prefix.rstrip() + "\n"

    @wraps(func)
    def wrapper(*args, **kwargs) -> Response:
        try:
            return func(*args, **kwargs)
        except requests.HTTPError as exc:
            # typer.secho(exc.response.content)
            secho_error_and_exit(err_prefix + decode_http_err(exc))

    return wrapper


@dataclass
class URLTemplate:
    pattern: Template

    def render(
        self, pk: Optional[T] = None, path: Optional[str] = None, **kwargs
    ) -> str:
        """render URLTemplate

        Args:
            pk: primary key
            path: additional path to attach
        """
        if pk is None and path is None:
            return self.pattern.substitute(**kwargs)

        pattern = copy.deepcopy(self.pattern)
        need_trailing_slash = pattern.template.endswith("/")

        if pk is not None:
            pattern.template = urljoin(pattern.template + "/", str(pk))
            if need_trailing_slash:
                pattern.template += "/"

        if path is not None:
            pattern.template = urljoin(pattern.template + "/", path.rstrip("/"))
            if need_trailing_slash:
                pattern.template += "/"

        return pattern.substitute(**kwargs)

    def get_base_url(self) -> str:
        result = urlparse(self.pattern.template)
        return f"{result.scheme}://{result.hostname}"

    def attach_pattern(self, pattern: str) -> None:
        self.pattern.template = urljoin(self.pattern.template + "/", pattern)

    def replace_path(self, path: str):
        result = urlparse(self.pattern.template)
        result = result._replace(path=path)
        self.pattern.template = result.geturl()

    def copy(self) -> "URLTemplate":
        return URLTemplate(pattern=Template(self.pattern.template))


class ClientService(Generic[T]):
    def __init__(self, template: Template, **kwargs):
        self.url_template = URLTemplate(template)
        self.url_kwargs = kwargs

    @auto_token_refresh
    def list(self, path: Optional[str] = None, **kwargs) -> Response:
        return requests.get(
            self.url_template.render(path=path, **self.url_kwargs),
            **{"headers": get_auth_header(), **kwargs},
        )

    @auto_token_refresh
    def retrieve(self, pk: T, path: Optional[str] = None, **kwargs) -> Response:
        return requests.get(
            self.url_template.render(pk=pk, path=path, **self.url_kwargs),
            **{"headers": get_auth_header(), **kwargs},
        )

    @auto_token_refresh
    def post(self, path: Optional[str] = None, **kwargs) -> Response:
        return requests.post(
            self.url_template.render(path=path, **self.url_kwargs),
            **{"headers": get_auth_header(), **kwargs},
        )

    @auto_token_refresh
    def partial_update(self, pk: T, path: Optional[str] = None, **kwargs) -> Response:
        return requests.patch(
            self.url_template.render(pk=pk, path=path, **self.url_kwargs),
            **{"headers": get_auth_header(), **kwargs},
        )

    @auto_token_refresh
    def delete(self, pk: T, path: Optional[str] = None, **kwargs) -> Response:
        return requests.delete(
            self.url_template.render(pk=pk, path=path, **self.url_kwargs),
            **{"headers": get_auth_header(), **kwargs},
        )

    @auto_token_refresh
    def update(self, pk: T, path: Optional[str] = None, **kwargs) -> Response:
        return requests.put(
            self.url_template.render(pk=pk, path=path, **self.url_kwargs),
            **{"headers": get_auth_header(), **kwargs},
        )

    def bare_post(self, path: Optional[str] = None, **kwargs) -> Response:
        r = requests.post(
            self.url_template.render(path=path, **self.url_kwargs), **kwargs
        )
        r.raise_for_status()
        return r


class UserRequestMixin:
    user_id: uuid.UUID

    @auto_token_refresh
    def _userinfo(self) -> Response:
        return requests.get(get_auth_uri("oauth2/userinfo"), headers=get_auth_header())

    def get_current_userinfo(self) -> dict:
        response = safe_request(self._userinfo, err_prefix="Failed to get userinfo.")()
        return response.json()

    def get_current_user_id(self) -> uuid.UUID:
        userinfo = self.get_current_userinfo()
        return uuid.UUID(userinfo["sub"].split("|")[1])

    def initialize_user(self):
        self.user_id = self.get_current_user_id()


class GroupRequestMixin:
    group_id: uuid.UUID

    def initialize_group(self):
        group_id = get_current_group_id()
        if group_id is None:
            secho_error_and_exit("Organization is not set.")
        self.group_id = group_id


class ProjectRequestMixin:
    project_id: uuid.UUID

    def initialize_project(self):
        project_id = get_current_project_id()
        if project_id is None:
            secho_error_and_exit("Project is not set.")
        self.project_id = project_id
