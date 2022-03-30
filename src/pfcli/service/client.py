# Copyright (C) 2021 FriendliAI

"""PeriFlow Client Service"""

from __future__ import annotations

import copy
import json
import functools
from string import Template
from typing import Callable, TypeVar, Type, Dict, Tuple, Optional, Union
from dataclasses import dataclass

import requests
from requests import Response, HTTPError

from pfcli.autoauth import (
    TokenType,
    get_auth_header,
    get_token,
    update_token,
)
from pfcli.utils import get_uri, secho_error_and_exit
from pfcli.service import ServiceType


A = TypeVar('A', bound='ClientService')
T = TypeVar('T', bound=Union[int, str])


@dataclass
class URLTemplate:
    template: Template

    def render(self, pk: Optional[T] = None, **kwargs) -> str:
        if pk is not None:
            t = copy.deepcopy(self.template)
            t.template += '$id/'
            return t.substitute(**kwargs, id=pk)

        return self.template.substitute(**kwargs)


class ClientService:
    def __init__(self, template: Template, **kwargs):
        self.headers = get_auth_header()
        self.url_template = URLTemplate(template)
        self.url_kwargs = kwargs

    def _auto_token_refresh(func: Callable[..., Response]) -> Callable:
        @functools.wraps(func)
        def inner(self, *args, **kwargs) -> Response:
            r = func(self, *args, **kwargs)
            if r.status_code == 401 or r.status_code == 403:
                refresh_token = get_token(TokenType.REFRESH)
                if refresh_token is not None:
                    refresh_r = requests.post(get_uri("token/refresh/"), data={"refresh": refresh_token})
                    try:
                        refresh_r.raise_for_status()
                        update_token(token_type="access", token=refresh_r.json()["access"])
                        # We need to restore file offset if we want to transfer file objects
                        if "files" in kwargs:
                            files = kwargs["files"]
                            for file_name, file_tuple in files.items():
                                for element in file_tuple:
                                    if hasattr(element, "seek"):
                                        # Restore file offset
                                        element.seek(0)
                        r = func(self, *args, **kwargs)
                    except HTTPError:
                        secho_error_and_exit("Failed to refresh access token... Please login again")
                else:
                    secho_error_and_exit("Failed to refresh access token... Please login again")
            return r
        return inner

    @_auto_token_refresh
    def list(self, **kwargs) -> Response:
        return requests.get(
            self.url_template.render(**self.url_kwargs),
            headers=self.headers,
            **kwargs 
        )

    @_auto_token_refresh
    def retrieve(self, pk: T, **kwargs) -> Response:
        return requests.get(
            self.url_template.render(pk, **self.url_kwargs),
            headers=self.headers,
            **kwargs
        )

    @_auto_token_refresh
    def create(self, pk: T, **kwargs) -> Response:
        return requests.post(
            self.url_template.render(pk, **self.url_kwargs),
            headers=self.headers,
            **kwargs
        )

    @_auto_token_refresh
    def partial_update(self, pk: T, **kwargs) -> Response:
        return requests.patch(
            self.url_template.render(pk, **self.url_kwargs),
            headers=self.headers,
            **kwargs
        )

    @_auto_token_refresh
    def delete(self, pk: T, **kwargs) -> Response:
        return requests.delete(
            self.url_template.render(pk, **self.url_kwargs),
            headers=self.headers,
            **kwargs
        )


class GroupRequestMixin:
    user_group_service: UserGroupClientService
    group_id: int

    def initialize_group(self):
        self.user_group_service = build_client(ServiceType.USER_GROUP)
        self.group_id = self.user_group_service.get_group_id()


class UserGroupClientService(ClientService):
    def get_group_id(self) -> int:
        response = self.list()
        if response.status_code != 200:
            secho_error_and_exit(f"Cannot acquire group info.")
        groups = response.json()["results"]
        if len(groups) == 0:
            secho_error_and_exit("You are not assigned to any group... Please contact admin")
        if len(groups) > 1:
            secho_error_and_exit(
                "Currently we do not support users with more than two groups... Please contact admin"
            )
        return groups[0]['id']


class JobClientService(ClientService):
    def list_jobs(self) -> dict:
        response = self.list()
        return response.json()["results"]

    def get_job(self, pk: int) -> dict:
        response = self.retrieve(pk)
        return response.json()


class JobCheckpointService(ClientService):
    def list_checkpoints(self) -> dict:
        response = self.list()
        return json.loads(response.content)


class JobArtifactService(ClientService):
    def list_artifacts(self) -> dict:
        response = self.list()
        return json.loads(response.content)


class GroupJobClientService(ClientService, GroupRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_group()
        super().__init__(template, group_id=self.group_id, **kwargs)

    def list_jobs(self) -> dict:
        response = self.list(params={"group_id": self.group_id})
        return response.json()['results']


client_template_map: Dict[ServiceType, Tuple[Type[A], Template]] = {
    ServiceType.USER_GROUP: (UserGroupClientService, Template(get_uri('user/group/'))),
    ServiceType.JOB: (JobClientService, Template(get_uri('job/'))),
    ServiceType.JOB_CHECKPOINT: (JobCheckpointService, Template(get_uri('job/$job_id/checkpoint/'))),
    ServiceType.JOB_ARTIFACT: (JobArtifactService, Template(get_uri('job/$job_id/artifact/'))),
    ServiceType.GROUP_JOB: (GroupJobClientService, Template(get_uri('group/$group_id/job/'))),
}


def build_client(request_type: ServiceType, **kwargs) -> A:
    """Factory function to create client service.

    Args:
        request_type (RequestAPI): 

    Returns:
        A: _description_
    """
    cls, template = client_template_map[request_type]
    return cls(template, **kwargs)
